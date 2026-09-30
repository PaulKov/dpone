"""Existing explicit MSSQL state resolves one load/step audit location."""

import pytest

from dpone.config import LoadConfig
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.bootstrap_state import RuntimeStateBootstrap
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.errors import RuntimeConfigurationError
from dpone.runtime.state.factory import StateFactory
from tests.test_selected_runtime_audit import MSSQLConnector


def _bootstrap(monkeypatch, *, audit=None, legacy=False, fail_step=False, state_overrides=None):
    connector = MSSQLConnector()
    connector.closed = 0

    def close():
        connector.closed += 1

    connector.close = close
    if fail_step:
        execute = connector.execute_query

        def fail(query, params=None):
            if "CREATE TABLE [Example_Metadata].[ops].[step_events]" in query:
                raise RuntimeError("step catalog unavailable")
            return execute(query, params)

        connector.execute_query = fail
    monkeypatch.setattr(ResolvedConnectorFactory, "create", lambda *a, **k: connector)
    monkeypatch.setattr(StateFactory, "create_mssql_connector", lambda **k: connector)
    monkeypatch.setattr(StateFactory, "create_mssql_xmin_state_storage", lambda **k: object())
    monkeypatch.setattr(StateFactory, "create_mssql_kafka_offset_state_storage", lambda **k: object())
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="sink",
        source_schema="raw",
        source_table="events",
        target_schema="analytics",
        target_table="events",
        options={"load_governance": {"audit": audit or {"steps_table": "step_events"}}},
    )
    state = {"type": "mssql", "table": {"database": "Example_Metadata", "schema": "ops"}}
    state.update(state_overrides or {})

    def run():
        if legacy:
            return RuntimeStateBootstrap().build(
                config={},
                sink_cfg={"type": "clickhouse"},
                state_cfg={
                    **state,
                    "connection_id": "metadata",
                    "credentials_source": "env",
                    "vault_mount_point": "test",
                },
                load_config=config,
            )
        return RuntimeStateBootstrap().build_resolved(
            state_cfg=state,
            load_config=config,
            state_connection=ResolvedBindingConnection(
                credentials=CredentialsConfig(database="Example_Metadata", schema="ops"),
                safe_metadata={},
                descriptor=ResolvedConnectionDescriptor(connection_type="mssql", properties={}),
            ),
        )

    return connector, run


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("explicit", [False, True])
def test_mssql_bootstrap_admits_same_scoped_load_and_step_stores(monkeypatch, legacy, explicit):
    audit = {"steps_table": "step_events"}
    if explicit:
        audit.update(state_schema="ops", loads_table="dpone_load_audit")
    connector, build = _bootstrap(monkeypatch, audit=audit, legacy=legacy)
    bindings = build()
    pair = getattr(bindings, "audit_bindings", None)
    assert pair is not None, "bootstrap dropped the selected step audit"
    assert pair.loads is bindings.load_audit_storage
    assert pair.connector is connector
    assert pair.steps.fq_table == "[Example_Metadata].[ops].[step_events]"
    queries = [sql for sql, _ in connector.queries]
    assert any("CREATE TABLE [Example_Metadata].[ops].[dpone_load_audit]" in sql for sql in queries)
    assert any("CREATE TABLE [Example_Metadata].[ops].[step_events]" in sql for sql in queries)


@pytest.mark.parametrize(
    "override",
    [
        {"state_schema": "other"},
        {"loads_table": "other"},
        {"steps_table": "dpone_load_audit"},
        {"steps_table": " dpone_load_audit "},
        {"steps_table": "etl_xmin_state"},
        {"steps_table": "etl_run_state"},
        {"steps_table": "other.steps"},
    ],
)
def test_conflicting_audit_scope_fails_before_audit_ddl(monkeypatch, override):
    connector, build = _bootstrap(monkeypatch, audit=override)
    with pytest.raises(RuntimeConfigurationError):
        build()
    assert connector.queries == []
    assert connector.closed == 1


def test_partial_pair_failure_closes_owned_connector_and_returns_no_bindings(monkeypatch):
    connector, build = _bootstrap(monkeypatch, fail_step=True)
    with pytest.raises(RuntimeError, match="step catalog unavailable"):
        build()
    assert connector.closed == 1


def test_disabled_audit_preserves_load_ledger_without_creating_step_table(monkeypatch):
    connector, build = _bootstrap(monkeypatch, audit={"mode": "off"})
    bindings = build()
    pair = getattr(bindings, "audit_bindings", None)
    assert pair is not None
    assert pair.loads is bindings.load_audit_storage
    assert pair.steps is None
    assert not any("load_steps" in sql for sql, _ in connector.queries)


@pytest.mark.parametrize(
    ("steps_table", "state_overrides"),
    [
        ("dpone_partition_checkpoints", {}),
        ("etl_kafka_offsets", {}),
        ("custom_checkpoints", {"partition_checkpoint_table": {"name": "custom_checkpoints"}}),
        ("custom_offsets", {"kafka_table": {"kafka_name": "custom_offsets"}}),
    ],
)
def test_step_audit_cannot_repurpose_checkpoint_or_offset_relation(monkeypatch, steps_table, state_overrides):
    connector, build = _bootstrap(
        monkeypatch,
        audit={"steps_table": steps_table},
        state_overrides=state_overrides,
    )
    with pytest.raises(RuntimeConfigurationError):
        build()
    assert connector.queries == []
    assert connector.closed == 1
