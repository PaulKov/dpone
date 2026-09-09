from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.config import LoadConfig
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.physical_design_models import PhysicalReconciliationOptions
from dpone.readiness.physical_reconciliation import PhysicalDesignReconciler
from dpone.readiness.physical_state import PhysicalColumnState, PhysicalTableState
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.lifecycle import RuntimeLifecycleContext, RuntimeLifecycleService
from dpone.runtime.sinks.clickhouse_physical_reconciliation import (
    ClickHousePhysicalMigrationDialect,
    parse_clickhouse_table_settings,
)
from dpone.runtime.sinks.load_payload import LoadPayload


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


class _ClickHouseSink:
    def __init__(self, actual: PhysicalTableState) -> None:
        self.actual = actual
        self.ddl: list[str] = []

    def target_table_exists(self, load_config: LoadConfig) -> bool:
        del load_config
        return True

    def inspect_physical_design(self, load_config: LoadConfig) -> PhysicalTableState:
        del load_config
        return self.actual

    def apply_physical_ddl(self, request: object) -> None:
        self.ddl.append(str(getattr(request, "sql")))


def test_physical_reconciliation_no_drift_is_noop() -> None:
    desired = _desired_state()

    plan = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=desired,
        options=PhysicalReconciliationOptions(),
        dialect=ClickHousePhysicalMigrationDialect(),
    )

    assert plan.has_drift is False
    assert plan.blockers == ()
    assert plan.ddl == ()


def test_clickhouse_table_setting_drift_is_auto_safe_only_when_enabled() -> None:
    desired = _desired_state()
    actual = replace(desired, table_settings={"min_rows_for_wide_part": 0})

    blocked = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual,
        options=PhysicalReconciliationOptions(mode="block"),
        dialect=ClickHousePhysicalMigrationDialect(),
    )
    auto_safe = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual,
        options=PhysicalReconciliationOptions(mode="auto_safe"),
        dialect=ClickHousePhysicalMigrationDialect(),
    )

    assert "physical_design.online_safe:table_settings.min_rows_for_wide_part" in blocked.blockers
    assert blocked.ddl == ()
    assert auto_safe.blockers == ()
    assert auto_safe.ddl == ("ALTER TABLE `landing`.`orders` MODIFY SETTING min_rows_for_wide_part = 8192",)


def test_clickhouse_create_time_table_setting_is_not_auto_safe() -> None:
    desired = replace(_desired_state(), table_settings={"index_granularity": 8192})
    actual = replace(desired, table_settings={"index_granularity": 4096})

    plan = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual,
        options=PhysicalReconciliationOptions(mode="auto_safe"),
        dialect=ClickHousePhysicalMigrationDialect(),
    )

    assert "physical_design.blocking:table_settings.index_granularity" in plan.blockers
    assert plan.ddl == ()


def test_extra_actual_table_settings_are_warning_only() -> None:
    desired = _desired_state()
    actual = replace(desired, table_settings={"min_rows_for_wide_part": 8192, "index_granularity": 8192})

    plan = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual,
        options=PhysicalReconciliationOptions(mode="auto_safe"),
        dialect=ClickHousePhysicalMigrationDialect(),
    )

    assert plan.has_drift is False
    assert plan.blockers == ()
    assert plan.ddl == ()
    assert "physical_design.warning:table_settings.index_granularity" in plan.warnings


@pytest.mark.parametrize(
    ("actual", "blocker"),
    [
        ({"engine": "ReplacingMergeTree"}, "physical_design.shadow_required:engine"),
        ({"partition_by": "toYYYYMM(other_date)"}, "physical_design.shadow_required:partition_by"),
        ({"order_by": ("other_id",)}, "physical_design.shadow_required:order_by"),
        ({"ttl": "created_at + toIntervalDay(14)"}, "physical_design.shadow_required:ttl"),
    ],
)
def test_clickhouse_layout_and_column_drift_are_not_auto_applied(actual: dict, blocker: str) -> None:
    desired = _desired_state()
    actual_state = replace(desired, **actual)

    plan = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual_state,
        options=PhysicalReconciliationOptions(mode="auto_safe"),
        dialect=ClickHousePhysicalMigrationDialect(),
    )

    assert blocker in plan.blockers
    assert plan.ddl == ()


def test_clickhouse_column_type_drift_is_schema_evolution_owned_without_blocker() -> None:
    desired = _desired_state()
    actual_state = replace(
        desired,
        columns={"id": PhysicalColumnState(name="id", target_type="String", nullable=True)},
    )

    plan = PhysicalDesignReconciler().reconcile(
        desired=desired,
        actual=actual_state,
        options=PhysicalReconciliationOptions(mode="auto_safe"),
        dialect=ClickHousePhysicalMigrationDialect(),
    )

    assert plan.blockers == ()
    assert plan.ddl == ()
    assert any(action.decision == "schema_evolution_owned" for action in plan.actions)


def test_clickhouse_settings_parser_reports_unparseable_settings() -> None:
    settings, error = parse_clickhouse_table_settings(
        "CREATE TABLE landing.orders (id Int64) ENGINE = MergeTree ORDER BY id SETTINGS bad fragment"
    )

    assert settings == {}
    assert error and "Cannot parse" in error


def test_runtime_physical_reconciliation_blocks_by_default() -> None:
    cfg = _load_config(reconciliation_mode="block")
    desired = _plan_state(cfg)
    sink = _ClickHouseSink(replace(desired, table_settings={"min_rows_for_wide_part": 0}))

    with pytest.raises(RuntimeError, match="physical DDL apply blocked"):
        RuntimeLifecycleService().prepare_after_schema_evolution(
            load_config=cfg,
            sink=sink,
            context=_context(),
        )

    assert sink.ddl == []


def test_runtime_physical_reconciliation_auto_safe_executes_setting_ddl() -> None:
    cfg = _load_config(reconciliation_mode="auto_safe")
    desired = _plan_state(cfg)
    sink = _ClickHouseSink(replace(desired, table_settings={"min_rows_for_wide_part": 0}))

    result = RuntimeLifecycleService().prepare_after_schema_evolution(
        load_config=cfg,
        sink=sink,
        context=_context(),
    )

    assert result.ddl_apply is not None
    assert result.ddl_apply["applied"] is True
    assert sink.ddl == ["ALTER TABLE `landing`.`orders` MODIFY SETTING min_rows_for_wide_part = 8192"]


def test_schema_physical_diff_cli_outputs_explainable_plan(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))
    manifest = tmp_path / "manifest.yaml"
    actual = tmp_path / "actual.json"
    manifest.write_text(
        yaml.safe_dump(
            {
                "source": {"options": {"columns": [{"name": "id", "type": "bigint"}]}},
                "sink": {
                    "type": "clickhouse",
                    "table": {"schema": "landing", "name": "orders"},
                    "options": {
                        "physical_design": {
                            "reconciliation": {"mode": "plan_only"},
                            "storage": {
                                "clickhouse": {
                                    "order_by": ["id"],
                                    "table_settings": {"min_rows_for_wide_part": 8192},
                                }
                            },
                        }
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    actual.write_text(
        json.dumps(
            {
                "sink_type": "clickhouse",
                "table": "landing.orders",
                "engine": "MergeTree",
                "order_by": ["id"],
                "columns": {"id": {"type": "Nullable(Int64)", "nullable": True}},
                "table_settings": {"min_rows_for_wide_part": 0},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exit_info:
        cli_main.main(
            [
                "schema",
                "physical-diff",
                "--manifest",
                str(manifest),
                "--actual",
                str(actual),
                "--format",
                "json",
            ]
        )

    assert exit_info.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "plan_only"
    assert payload["ddl"] == ["ALTER TABLE `landing`.`orders` MODIFY SETTING min_rows_for_wide_part = 8192"]
    assert "physical_design.online_safe:table_settings.min_rows_for_wide_part" in payload["blockers"]


def _desired_state() -> PhysicalTableState:
    return _plan_state(_load_config(reconciliation_mode="block"))


def _plan_state(cfg: LoadConfig) -> PhysicalTableState:
    plan = PhysicalDesignPlanner().plan(
        sink_type="clickhouse",
        table="landing.orders",
        source_schema=[("id", "bigint")],
        options=PhysicalDesignOptions.from_config(cfg.options["physical_design"]),
    )
    return PhysicalTableState.from_physical_plan(plan)


def _load_config(*, reconciliation_mode: str) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={
            "sink_type": "clickhouse",
            "physical_design": {
                "apply_runtime": True,
                "reconciliation": {"mode": reconciliation_mode},
                "storage": {
                    "clickhouse": {
                        "order_by": ["id"],
                        "ttl": "created_at + toIntervalDay(7)",
                        "table_settings": {"min_rows_for_wide_part": 8192},
                    }
                },
            },
        },
    )


def _context() -> RuntimeLifecycleContext:
    return RuntimeLifecycleContext(
        payload=LoadPayload(
            artifact=InMemoryRowsArtifact([{"id": 1}]),
            schema=[("id", "bigint")],
        )
    )
