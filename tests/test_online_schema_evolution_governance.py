from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.readiness.ddl_governance import (
    DdlCapabilityRegistry,
    DdlGovernancePolicy,
    OnlineSchemaPlanner,
    SchemaChangeLedger,
)
from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.sinks.base import LoadResult


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


class _Source:
    def __init__(self, rows, schema):
        self.connector = object()
        self._result = SimpleNamespace(
            artifact=InMemoryRowsArtifact(rows),
            schema=schema,
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
    def __init__(self, target_schema):
        self.connector = SimpleNamespace()
        self.target_schema = target_schema
        self.applied: list[str] = []
        self.received_payload = None

    def get_target_schema(self, load_config):
        del load_config
        return self.target_schema

    def apply_schema_plan(self, load_config, plan):
        self.applied.extend(plan.ddl_sql("postgres", f"{load_config.target_schema}.{load_config.target_table}"))

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


def test_governance_policy_parses_contract_defaults_and_modes() -> None:
    policy = DdlGovernancePolicy.from_schema_evolution_options(
        {
            "tables": "freeze",
            "columns": "quarantine",
            "data_type": "variant_column",
            "ddl_mode": "online",
            "lock_timeout_seconds": 7,
            "statement_timeout_seconds": 30,
            "max_table_size_for_inline_ddl": 100_000,
            "on_schema_change": "notify",
        }
    )

    assert policy.tables == "freeze"
    assert policy.columns == "quarantine"
    assert policy.data_type == "variant_column"
    assert policy.ddl_mode == "online"
    assert policy.lock_timeout_seconds == 7
    assert policy.statement_timeout_seconds == 30
    assert policy.max_table_size_for_inline_ddl == 100_000
    assert policy.on_schema_change == "notify"


def test_online_planner_classifies_safe_add_and_blocking_type_widen() -> None:
    plan = SchemaComparator(SchemaEvolutionPolicy(mode="widening")).compare(
        source=[ColumnDef("id", "bigint", False), ColumnDef("name", "varchar(128)")],
        target=[ColumnDef("id", "int", False)],
    )

    governed = OnlineSchemaPlanner(DdlCapabilityRegistry()).plan(
        schema_plan=plan,
        dialect="postgres",
        table="public.orders",
        policy=DdlGovernancePolicy(ddl_mode="online", lock_timeout_seconds=5),
    )

    assert governed.online_eligible is False
    assert [item.risk_level for item in governed.actions] == ["blocking", "metadata_only"]
    assert governed.actions[0].decision == "defer"
    assert governed.actions[1].decision == "apply"
    assert governed.actions[1].ddl[0] == "SET lock_timeout = '5s'"
    assert governed.blockers == ("schema_evolution.blocking:type_widen:id",)
    assert "expand-contract" in governed.expand_contract_plan[0]


def test_schema_change_ledger_writes_detected_and_applied_events(tmp_path: Path) -> None:
    plan = SchemaComparator(SchemaEvolutionPolicy()).compare(
        source=[ColumnDef("id", "bigint", False), ColumnDef("name", "text")],
        target=[ColumnDef("id", "bigint", False)],
    )
    governed = OnlineSchemaPlanner(DdlCapabilityRegistry()).plan(
        schema_plan=plan,
        dialect="mssql",
        table="dbo.orders",
        policy=DdlGovernancePolicy(ddl_mode="online"),
    )

    artifact = SchemaChangeLedger(tmp_path).record(run_id="run01", table="dbo.orders", dialect="mssql", plan=governed)

    payload = json.loads(artifact.read_text(encoding="utf-8"))
    assert payload["run_id"] == "run01"
    assert payload["table"] == "dbo.orders"
    assert payload["status"] == "applied"
    assert payload["actions"][0]["schema_change_id"].startswith("sch_")
    assert payload["actions"][0]["risk_level"] == "metadata_only"


def test_runtime_online_mode_rejects_blocking_widen_before_load_and_records_ledger(tmp_path: Path) -> None:
    sink = _PostgresSink(target_schema=[("id", "int")])
    source = _Source(rows=[{"id": 1}], schema=[("id", "bigint")])
    cfg = _load_config(
        schema_evolution={
            "ddl_mode": "online",
            "ledger_path": str(tmp_path),
            "lock_timeout_seconds": 5,
        }
    )

    with pytest.raises(RuntimeError, match="online schema evolution blockers"):
        ETLProcessor(source, sink, etl_logger=_Logger()).run(cfg)

    assert sink.applied == []
    assert sink.received_payload is None
    ledger_files = list(tmp_path.glob("*.json"))
    assert len(ledger_files) == 1
    ledger = json.loads(ledger_files[0].read_text(encoding="utf-8"))
    assert ledger["status"] == "deferred"
    assert ledger["blockers"] == ["schema_evolution.blocking:type_widen:id"]
