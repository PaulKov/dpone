from __future__ import annotations

from types import SimpleNamespace

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.etl.decision_lifecycle import RuntimeDecisionLifecycle
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.lineage.audit import LoadIdentityService


def test_runtime_lifecycle_materializes_mssql_load_and_step_audit_in_dbo() -> None:
    connector = MSSQLConnector()
    governance = LoadGovernanceService()
    identity = LoadIdentityService()
    lifecycle = RuntimeDecisionLifecycle(
        load_governance_service=governance,
        load_identity_service=identity,
        logger=None,
    )
    load_config = _mssql_config()

    lifecycle.configure_audit_storage(
        sink=SimpleNamespace(connector=connector),
        load_config=load_config,
    )
    started = identity.start(load_config, process_name="marketing_sync")
    staged = identity.mark_staged(started, extracted_rows=2)
    identity.mark_committed(
        staged,
        SimpleNamespace(
            staging_rows=2,
            total_rows=2,
            inserted_rows=2,
            updated_rows=0,
            soft_deleted_rows=0,
        ),
    )
    governance.record_load_step(
        load_record=started,
        step_id="target_commit",
        phase="load",
        kind="target_finalization",
        status="succeeded",
        details={"loaded_rows": 2},
    )

    rendered = "\n".join(query for query, _ in connector.queries)
    assert "OBJECT_ID(N'dbo.__dpone__loads'" in rendered
    assert "MERGE [dbo].[__dpone__loads]" in rendered
    assert "OBJECT_ID(N'dbo.__dpone__load_steps'" in rendered
    assert "INSERT INTO [dbo].[__dpone__load_steps]" in rendered
    assert any(params and len(params) > 1 and params[1] == "committed" for _, params in connector.queries)


def test_runtime_lifecycle_does_not_wire_mssql_audit_when_explicitly_disabled() -> None:
    connector = MSSQLConnector()
    governance = LoadGovernanceService()
    identity = LoadIdentityService()
    lifecycle = RuntimeDecisionLifecycle(
        load_governance_service=governance,
        load_identity_service=identity,
        logger=None,
    )
    load_config = _mssql_config()
    load_config.options["load_governance"]["audit"]["enabled"] = False

    lifecycle.configure_audit_storage(
        sink=SimpleNamespace(connector=connector),
        load_config=load_config,
    )

    assert identity.audit_storage is None
    assert not governance.audit_storage_configured
    assert connector.queries == []


def test_runtime_lifecycle_preserves_injected_mssql_audit_storages() -> None:
    connector = MSSQLConnector()
    load_audit = object()
    step_audit = RecordingStepAuditStorage()
    governance = LoadGovernanceService(audit_storage=step_audit)
    identity = LoadIdentityService(audit_storage=load_audit)
    lifecycle = RuntimeDecisionLifecycle(
        load_governance_service=governance,
        load_identity_service=identity,
        logger=None,
    )

    lifecycle.configure_audit_storage(
        sink=SimpleNamespace(connector=connector),
        load_config=_mssql_config(),
    )
    governance.record_load_step(
        load_record=SimpleNamespace(run_id="run", load_id="load"),
        step_id="injected",
        phase="load",
        kind="custom",
        status="succeeded",
    )

    assert identity.audit_storage is load_audit
    assert [record.step_id for record in step_audit.records] == ["injected"]
    assert connector.queries == []


def _mssql_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="clickhouse",
        target_conn_id="mssql",
        source_schema="marketing",
        source_table="sample_web_sync",
        target_schema="ch",
        target_table="marketing__sample_web_sync",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "load_governance": {
                "enabled": True,
                "audit": {
                    "enabled": True,
                    "state_schema": "dbo",
                    "loads_table": "__dpone__loads",
                    "steps_table": "__dpone__load_steps",
                },
            }
        },
    )


class MSSQLConnector:
    """Minimal SQL Server connector double with Example_System as default DB."""

    def __init__(self) -> None:
        self.database = "Example_System"
        self.queries: list[tuple[str, tuple[object, ...] | None]] = []

    def execute_query(
        self,
        query: str,
        params: tuple[object, ...] | None = None,
    ) -> int:
        self.queries.append((str(query), params))
        return 0

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    @staticmethod
    def qualified_name(schema: str, table: str) -> str:
        return f"[{schema}].[{table}]"


class RecordingStepAuditStorage:
    def __init__(self) -> None:
        self.records: list[object] = []

    def record_step(self, record: object) -> None:
        self.records.append(record)
