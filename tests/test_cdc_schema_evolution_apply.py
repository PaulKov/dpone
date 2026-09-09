from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from dpone.ops.cdc.schema_apply import CdcSchemaEvolutionApplyService
from dpone.ops.cdc.schema_apply_clickhouse import ClickHouseCdcSchemaDdlPlanner
from dpone.ops.cdc.schema_apply_models import CdcSchemaApplyPolicy
from dpone.ops.cdc.schema_evolution_models import CdcSchemaChangeEvent


class _FakeClickHouseConnector:
    database = "default"

    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute_query(self, query: str) -> int:
        self.queries.append(query)
        return 0


class _FakeTypedMaterializer:
    def __init__(self, *, passed: bool = True) -> None:
        self.passed = passed
        self.calls: list[dict[str, object]] = []

    def materialize(self, **kwargs: object) -> SimpleNamespace:
        self.calls.append(dict(kwargs))
        output_dir = Path(str(kwargs["output_dir"]))
        output_dir.mkdir(parents=True, exist_ok=True)
        json_path = output_dir / "cdc_typed_materialization.json"
        json_path.write_text(
            json.dumps(
                {
                    "schema_version": "dpone.cdc_clickhouse_typed_materialization.v1",
                    "passed": self.passed,
                    "blockers": [] if self.passed else ["typed.failed"],
                    "target_dataset": kwargs["target_dataset"],
                }
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(
            passed=self.passed,
            blockers=tuple() if self.passed else ("typed.failed",),
            json_path=str(json_path),
            markdown_path=str(output_dir / "cdc_typed_materialization.md"),
            to_dict=lambda: json.loads(json_path.read_text(encoding="utf-8")),
            to_markdown=lambda: "# typed\n",
        )


def _schema_change_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "change": {
            "change_id": "orders-add-status-reason",
            "kind": "add_column",
            "captured_at": "2026-06-12T12:20:00Z",
            "source_table": "dbo.orders",
            "source_column": "status_reason",
            "target_table": "serving.orders_current_typed",
            "target_column": "status_reason",
            "old_type": "",
            "new_type": "nvarchar(100)",
            "old_nullable": True,
            "new_nullable": True,
            "source_offset": "42",
            "breaking": False,
        },
        "plan": {
            "sink_impact": "add nullable String column",
            "compatibility_level": "compatible",
            "target_ddl_preview": "",
            "ddl_dry_run_passed": True,
            "backfill_required": False,
            "backfill_plan": "",
            "type_widening_safe": True,
            "offset_schema_ordering_safe": True,
            "approved_by": ["data-architect"],
        },
    }
    for key, value in overrides.items():
        section, _, field = key.partition("__")
        nested = payload[section]
        assert isinstance(nested, dict)
        nested[field] = value
    return payload


def _write_schema_change(path: Path, **overrides: object) -> Path:
    path.write_text(json.dumps(_schema_change_payload(**overrides)), encoding="utf-8")
    return path


def test_clickhouse_schema_apply_planner_renders_safe_add_column_ddl() -> None:
    change = CdcSchemaChangeEvent.from_dict(_schema_change_payload()["change"])  # type: ignore[arg-type]

    plan = ClickHouseCdcSchemaDdlPlanner().plan(
        change=change,
        target_dataset="serving.orders_current_typed",
        policy=CdcSchemaApplyPolicy(mode="dry_run", require_approval=True),
    )

    assert plan.passed is True
    assert plan.operation == "add_column"
    assert plan.target_dataset == "serving.orders_current_typed"
    assert (
        plan.ddl
        == "ALTER TABLE `serving`.`orders_current_typed` ADD COLUMN IF NOT EXISTS `status_reason` Nullable(String)"
    )
    assert plan.backfill_sql == (
        "ALTER TABLE `serving`.`orders_current_typed` "
        "UPDATE `status_reason` = JSONExtractString(`dpone_cdc_payload_json`, 'status_reason') "
        "WHERE JSONExtractRaw(`dpone_cdc_payload_json`, 'status_reason') != ''"
    )
    assert plan.blockers == tuple()


def test_clickhouse_schema_apply_planner_blocks_breaking_changes() -> None:
    change = CdcSchemaChangeEvent.from_dict(
        _schema_change_payload(change__kind="drop_column", change__breaking=True)["change"]  # type: ignore[arg-type]
    )

    plan = ClickHouseCdcSchemaDdlPlanner().plan(
        change=change,
        target_dataset="serving.orders_current_typed",
        policy=CdcSchemaApplyPolicy(mode="dry_run", require_approval=True),
    )

    assert plan.passed is False
    assert plan.ddl == ""
    assert "cdc_schema_apply.unsupported_change" in plan.blockers
    assert "cdc_schema_apply.breaking_change" in plan.blockers


def test_schema_apply_service_dry_run_writes_plan_without_executing_ddl(tmp_path: Path) -> None:
    connector = _FakeClickHouseConnector()
    typed = _FakeTypedMaterializer()
    schema_change = _write_schema_change(tmp_path / "schema_change.json")

    report = CdcSchemaEvolutionApplyService(
        connector=connector,
        ddl_planner=ClickHouseCdcSchemaDdlPlanner(),
        typed_materializer=typed,
    ).apply(
        output_dir=tmp_path / "apply",
        schema_change_json=schema_change,
        sink="clickhouse",
        target_dataset="serving.orders_current_typed",
        mode="dry_run",
        cdc_dataset="analytics.orders_cdc",
        unique_key=("order_id",),
        columns=("order_id=Int32", "status_reason=Nullable(String)"),
        typed_refresh=True,
        require_approval=True,
    )

    plan_payload = json.loads((tmp_path / "apply" / "cdc_schema_apply_plan.json").read_text(encoding="utf-8"))
    result_payload = json.loads((tmp_path / "apply" / "cdc_schema_apply_result.json").read_text(encoding="utf-8"))
    assert report.passed is True
    assert report.mode == "dry_run"
    assert connector.queries == []
    assert typed.calls == []
    assert plan_payload["ddl"].startswith("ALTER TABLE")
    assert result_payload["applied"] is False
    assert result_payload["typed_refresh"]["ran"] is False


def test_schema_apply_service_apply_executes_ddl_and_runs_typed_refresh(tmp_path: Path) -> None:
    connector = _FakeClickHouseConnector()
    typed = _FakeTypedMaterializer()
    schema_change = _write_schema_change(tmp_path / "schema_change.json")

    report = CdcSchemaEvolutionApplyService(
        connector=connector,
        ddl_planner=ClickHouseCdcSchemaDdlPlanner(),
        typed_materializer=typed,
    ).apply(
        output_dir=tmp_path / "apply",
        schema_change_json=schema_change,
        sink="clickhouse",
        target_dataset="serving.orders_current_typed",
        mode="apply",
        cdc_dataset="analytics.orders_cdc",
        unique_key=("order_id",),
        columns=("order_id=Int32", "status_reason=Nullable(String)"),
        typed_refresh=True,
        require_approval=True,
    )

    result_payload = json.loads((tmp_path / "apply" / "cdc_schema_apply_result.json").read_text(encoding="utf-8"))
    assert report.passed is True
    assert connector.queries == [report.plan.ddl, report.plan.backfill_sql]
    assert typed.calls
    assert typed.calls[0]["target_dataset"] == "serving.orders_current_typed"
    assert result_payload["applied"] is True
    assert result_payload["typed_refresh"]["passed"] is True
    assert Path(result_payload["typed_refresh"]["json_path"]).exists()


def test_schema_apply_service_blocks_when_typed_refresh_fails(tmp_path: Path) -> None:
    connector = _FakeClickHouseConnector()
    typed = _FakeTypedMaterializer(passed=False)
    schema_change = _write_schema_change(tmp_path / "schema_change.json")

    report = CdcSchemaEvolutionApplyService(
        connector=connector,
        ddl_planner=ClickHouseCdcSchemaDdlPlanner(),
        typed_materializer=typed,
    ).apply(
        output_dir=tmp_path / "apply",
        schema_change_json=schema_change,
        sink="clickhouse",
        target_dataset="serving.orders_current_typed",
        mode="apply",
        cdc_dataset="analytics.orders_cdc",
        unique_key=("order_id",),
        columns=("order_id=Int32", "status_reason=Nullable(String)"),
        typed_refresh=True,
        require_approval=True,
    )

    assert report.passed is False
    assert "cdc_schema_apply.typed_refresh_failed" in report.blockers
