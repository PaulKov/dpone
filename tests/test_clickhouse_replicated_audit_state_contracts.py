from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.etl.decision_lifecycle import RuntimeDecisionLifecycle
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.runtime.route_runtime import LoadStepAuditRecord
from dpone.runtime.route_runtime_factory import LoadStepAuditStorageFactory

ROOT = Path(__file__).resolve().parents[1]


def test_runtime_decision_lifecycle_uses_replicated_clickhouse_audit_tables_when_clustered() -> None:
    connector = ClickHouseConnector()
    governance = LoadGovernanceService()
    identity = LoadIdentityService()
    lifecycle = RuntimeDecisionLifecycle(
        load_governance_service=governance,
        load_identity_service=identity,
        logger=None,
    )
    load_config = _clustered_config()

    lifecycle.configure_audit_storage(sink=SimpleNamespace(connector=connector), load_config=load_config)
    load_record = identity.start(load_config, process_name="orders")
    governance.record_load_step(
        load_record=load_record,
        step_id="route_decision",
        phase="pre_extract",
        kind="runtime_route_decision",
        status="succeeded",
        details={"selected_route_id": "typed_binary_streaming"},
    )

    rendered = "\n".join(query for query, _ in connector.queries)
    assert "CREATE DATABASE IF NOT EXISTS `state` ON CLUSTER `dwh`" in rendered
    assert "CREATE TABLE IF NOT EXISTS `state`.`__dpone__loads` ON CLUSTER `dwh`" in rendered
    assert "CREATE TABLE IF NOT EXISTS `state`.`__dpone__load_steps` ON CLUSTER `dwh`" in rendered
    assert "ENGINE = ReplicatedReplacingMergeTree" in rendered


def test_route_audit_storage_factory_passes_clickhouse_cluster_design() -> None:
    connector = ClickHouseConnector()
    sink = SimpleNamespace(connector=connector)

    storage = LoadStepAuditStorageFactory().from_sink(sink, _clustered_config())
    storage.record_load_step(
        LoadStepAuditRecord(
            run_id="run-1",
            load_id="load-1",
            step_id="route_decision",
            phase="pre_extract",
            kind="runtime_route_decision",
            status="succeeded",
            started_at=datetime(2026, 1, 1, tzinfo=UTC),
            finished_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=UTC),
            details_json={"selected_route_id": "typed_binary_streaming"},
        )
    )

    rendered = "\n".join(query for query, _ in connector.queries)
    assert "CREATE DATABASE IF NOT EXISTS `state` ON CLUSTER `dwh`" in rendered
    assert "CREATE TABLE IF NOT EXISTS `state`.`__dpone__load_steps` ON CLUSTER `dwh`" in rendered
    assert "ENGINE = ReplicatedReplacingMergeTree" in rendered


def test_manifest_schemas_expose_clickhouse_audit_engine_contract() -> None:
    for relative in ("src/dpone/schema/etl-config.schema.json", "src/dpone/schema/etl-batch-manifest.schema.json"):
        schema = json.loads((ROOT / relative).read_text(encoding="utf-8"))
        audit = _find_load_governance_audit_properties(schema)

        engine = audit["clickhouse"]["properties"]["engine"]
        assert engine["default"] == "auto"
        assert engine["enum"] == ["auto", "MergeTree", "ReplacingMergeTree", "ReplicatedReplacingMergeTree"]


def _clustered_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="raw",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "physical_design": {"storage": {"clickhouse": {"cluster": "dwh"}}},
            "load_governance": {
                "audit": {
                    "enabled": True,
                    "state_schema": "state",
                    "loads_table": "__dpone__loads",
                    "steps_table": "__dpone__load_steps",
                }
            },
        },
    )


def _find_load_governance_audit_properties(schema: dict[str, object]) -> dict[str, object]:
    stack = [schema]
    while stack:
        current = stack.pop()
        properties = current.get("properties") if isinstance(current, dict) else None
        if isinstance(properties, dict) and "load_governance" in properties:
            governance = properties["load_governance"]
            if isinstance(governance, dict):
                audit = governance.get("properties", {}).get("audit", {})
                if isinstance(audit, dict):
                    return audit.get("properties", {})
        if isinstance(current, dict):
            stack.extend(value for value in current.values() if isinstance(value, dict))
            stack.extend(value for value in current.values() if isinstance(value, list))
        elif isinstance(current, list):
            stack.extend(value for value in current if isinstance(value, dict | list))
    raise AssertionError("load_governance.audit schema not found")


class ClickHouseConnector:
    def __init__(self) -> None:
        self.queries: list[tuple[str, object | None]] = []

    def execute_query(self, query, params=None):  # noqa: ANN001
        self.queries.append((str(query), params))
        return 0
