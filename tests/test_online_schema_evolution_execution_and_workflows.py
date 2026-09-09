from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.config import LoadConfig, LoadStrategy
from dpone.readiness.ddl_execution import GovernedDdlExecutor, PostgresOnlineDdlAdapter
from dpone.readiness.ddl_governance import DdlGovernancePolicy, OnlineSchemaPlanner, SchemaChangeLedger
from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.sinks.base import LoadResult


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


class _Logger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def log_etl_start(self, payload):
        del payload

    def log_etl_progress(self, event, payload):
        self.events.append((event, payload))

    def log_etl_error(self, message, payload):
        del message, payload

    def log_etl_end(self, payload):
        del payload

    def info(self, message):
        del message

    def warning(self, message):
        del message


class _Connector:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute_query(self, statement: str) -> None:
        self.executed.append(statement)


class _Source:
    connector = object()

    def __init__(self) -> None:
        self._result = SimpleNamespace(
            artifact=InMemoryRowsArtifact([{"id": 1, "name": "Ada"}]),
            schema=[("id", "bigint"), ("name", "text")],
            state=None,
            force_full_refresh=False,
        )

    def get_incremental_state(self, load_config):
        del load_config
        return None

    def extract(self, load_config, state):
        del load_config, state
        return self._result


class _PostgresSink:
    def __init__(self) -> None:
        self.connector = _Connector()
        self.received_payload = None

    def get_target_schema(self, load_config):
        del load_config
        return [("id", "bigint")]

    def load(self, load_config, payload):
        del load_config
        self.received_payload = payload
        return LoadResult(inserted_rows=1, updated_rows=0, total_rows=1, staging_rows=1)


def _load_config(**options) -> LoadConfig:
    options.setdefault("lineage", False)
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options=options,
    )


def _add_column_governed_plan():
    plan = SchemaComparator(SchemaEvolutionPolicy()).compare(
        source=[ColumnDef("id", "bigint", False), ColumnDef("name", "text")],
        target=[ColumnDef("id", "bigint", False)],
    )
    return OnlineSchemaPlanner().plan(
        schema_plan=plan,
        dialect="postgres",
        table="landing.orders",
        policy=DdlGovernancePolicy(ddl_mode="online", lock_timeout_seconds=5, statement_timeout_seconds=60),
    )


def test_governed_ddl_executor_applies_decorated_online_actions() -> None:
    connector = _Connector()
    GovernedDdlExecutor(adapter=PostgresOnlineDdlAdapter()).apply(
        connector=connector, governed_plan=_add_column_governed_plan()
    )

    assert connector.executed == [
        "SET lock_timeout = '5s'",
        "SET statement_timeout = '60s'",
        'ALTER TABLE "landing"."orders" ADD COLUMN "name" text',
    ]


def test_runtime_applies_governed_ddl_before_staging_load() -> None:
    sink = _PostgresSink()
    logger = _Logger()
    cfg = _load_config(schema_evolution={"ddl_mode": "online", "lock_timeout_seconds": 5})

    ETLProcessor(_Source(), sink, etl_logger=logger).run(cfg)

    assert sink.connector.executed == [
        "SET lock_timeout = '5s'",
        'ALTER TABLE "landing"."orders" ADD COLUMN "name" text',
    ]
    assert sink.received_payload is not None
    assert any(event == "ONLINE_SCHEMA_EVOLUTION_PLAN" for event, _ in logger.events)


def test_table_size_budget_defers_inline_ddl() -> None:
    plan = SchemaComparator(SchemaEvolutionPolicy()).compare(
        source=[ColumnDef("id", "bigint", False), ColumnDef("name", "text")],
        target=[ColumnDef("id", "bigint", False)],
    )
    governed = OnlineSchemaPlanner().plan(
        schema_plan=plan,
        dialect="postgres",
        table="landing.orders",
        policy=DdlGovernancePolicy(ddl_mode="online", max_table_size_for_inline_ddl=10),
        table_row_count=11,
    )

    assert governed.blockers == ("schema_evolution.table_size_budget:add_column:name",)
    assert governed.actions[0].decision == "defer"


def test_schema_approval_cli_writes_approval_artifact(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    ledger = SchemaChangeLedger(tmp_path).record(
        run_id="run01", table="landing.orders", dialect="postgres", plan=_add_column_governed_plan()
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "schema",
                "approve",
                "--ledger",
                str(ledger),
                "--approver",
                "data-architect",
                "--output-dir",
                str(tmp_path / "approvals"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["approval_status"] == "approved"
    assert payload["approver"] == "data-architect"
    assert Path(payload["artifact_path"]).exists()


def test_expand_contract_cli_writes_three_phase_plan(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    source = tmp_path / "source.json"
    target = tmp_path / "target.json"
    source.write_text(json.dumps([{"name": "amount", "dtype": "text"}]), encoding="utf-8")
    target.write_text(json.dumps([{"name": "amount", "dtype": "int"}]), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "schema",
                "expand-contract",
                "--source",
                str(source),
                "--target",
                str(target),
                "--table",
                "landing.orders",
                "--dialect",
                "postgres",
                "--output-dir",
                str(tmp_path / "expand"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["phases"] == ["expand", "backfill", "contract"]
    assert "__dpone__nc__amount" in json.dumps(payload)
    assert Path(payload["artifact_path"]).exists()
