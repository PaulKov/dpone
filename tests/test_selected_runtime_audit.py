"""Selected metadata audit must not leak into a business sink."""

from types import SimpleNamespace

import pytest

from dpone.contracts import RuntimeConfigurationError
from dpone.ports import runtime_hydrator
from dpone.runtime.etl.decision_lifecycle import RuntimeDecisionLifecycle
from dpone.runtime.governance.audit_tap import RuntimeLoadStepAuditCollector
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.runtime.state.mssql import MSSQLLoadAuditStorage
from dpone.runtime.state.mssql_load_step_audit import MSSQLLoadStepAuditStorage
from tests.test_mssql_load_audit_lifecycle import MSSQLConnector as _LegacyConnector
from tests.test_mssql_load_audit_lifecycle import RecordingStepAuditStorage, _mssql_config


class MSSQLConnector(_LegacyConnector):
    @staticmethod
    def qualified_name(schema, table, *, database=None):
        prefix = f"[{database}]." if database else ""
        return f"{prefix}[{schema}].[{table}]"


def _pair(*, steps=True):
    pair_type = getattr(runtime_hydrator, "RuntimeAuditBindings", None)
    assert pair_type is not None, "runtime must retain the selected load/step audit pair"
    connector = MSSQLConnector()
    return pair_type(
        loads=MSSQLLoadAuditStorage(connector, database="Example_Metadata", schema="dbo", table="loads"),
        steps=(
            MSSQLLoadStepAuditStorage(connector, database="Example_Metadata", schema="dbo", table="steps")
            if steps
            else None
        ),
        connector=connector,
    )


class _ForbiddenSink:
    @property
    def connector(self):
        raise AssertionError("selected audit must not inspect the business connector")


@pytest.mark.parametrize("consumer", ["lifecycle", "route"])
@pytest.mark.parametrize("enabled", [True, False])
def test_explicit_audit_selector_without_hydrated_pair_never_falls_back(consumer, enabled):
    from dpone.runtime.route_runtime_factory import RouteCapabilityRuntimeFactory
    from tests.test_route_capability_runtime_factory import _AssemblySpy

    config = _mssql_config()
    config.options["load_governance"]["audit"] = {
        "enabled": enabled,
        "storage": {"type": "mssql", "connection_ref": "metadata", "provisioning": "external"},
    }
    with pytest.raises(RuntimeConfigurationError, match="audit"):
        if consumer == "lifecycle":
            RuntimeDecisionLifecycle(
                load_governance_service=LoadGovernanceService(),
                load_identity_service=LoadIdentityService(),
                logger=None,
            ).configure_audit_storage(sink=_ForbiddenSink(), load_config=config)
        else:
            RouteCapabilityRuntimeFactory(columnar_assembly=_AssemblySpy()).build(
                load_config=config,
                source=object(),
                sink=_ForbiddenSink(),
            )


@pytest.mark.parametrize("missing", ["loads", "connector"])
def test_selected_audit_rejects_missing_endpoint_ownership(missing):
    fields = {"loads": object(), "steps": None, "connector": object()}
    fields[missing] = None
    with pytest.raises(RuntimeConfigurationError):
        runtime_hydrator.RuntimeAuditBindings(**fields)


def test_enabled_route_rejects_missing_selected_steps_before_assembly():
    from dpone.runtime.route_runtime_factory import RouteCapabilityRuntimeFactory
    from tests.test_route_capability_runtime_factory import _AssemblySpy, _cfg

    assembly = _AssemblySpy()
    with pytest.raises(RuntimeConfigurationError):
        RouteCapabilityRuntimeFactory(columnar_assembly=assembly).build(
            load_config=_cfg(), source=object(), sink=_ForbiddenSink(), audit_bindings=_pair(steps=False)
        )
    assert assembly.calls == 0


def test_selected_pair_records_load_and_step_in_metadata_and_preserves_collector():
    pair = _pair()
    identity = LoadIdentityService(audit_storage=pair.loads)
    collector = RuntimeLoadStepAuditCollector()
    service = LoadGovernanceService(audit_storage=collector)
    lifecycle = RuntimeDecisionLifecycle(
        load_governance_service=service, load_identity_service=identity, logger=None, audit_bindings=pair
    )
    lifecycle.configure_audit_storage(sink=_ForbiddenSink(), load_config=_mssql_config())
    # Repeated configuration must not nest delegates or duplicate a durable event.
    lifecycle.configure_audit_storage(sink=_ForbiddenSink(), load_config=_mssql_config())
    started = identity.start(_mssql_config(), process_name="example_sync")
    service.record_load_step(load_record=started, step_id="publish", phase="load", kind="commit", status="succeeded")
    queries = [sql for sql, _ in pair.connector.queries]
    assert any("MERGE [Example_Metadata].[dbo].[loads]" in sql for sql in queries)
    assert sum("INSERT INTO [Example_Metadata].[dbo].[steps]" in sql for sql in queries) == 1
    assert [r.step_id for r in collector.records] == ["publish"]
    assert collector.to_jsonable()[0]["step_id"] == "publish"


@pytest.mark.parametrize("conflict", ["loads", "steps", "delegate", "missing_steps"])
def test_selected_pair_rejects_partial_or_conflicting_audit_before_any_write(conflict):
    pair = _pair(steps=conflict != "missing_steps")
    identity = LoadIdentityService(audit_storage=object() if conflict == "loads" else pair.loads)
    storage = RecordingStepAuditStorage()
    if conflict != "steps":
        storage = RuntimeLoadStepAuditCollector(storage if conflict == "delegate" else None)
    service = LoadGovernanceService(audit_storage=storage)
    lifecycle = RuntimeDecisionLifecycle(
        load_governance_service=service, load_identity_service=identity, logger=None, audit_bindings=pair
    )
    with pytest.raises(RuntimeConfigurationError):
        lifecycle.configure_audit_storage(sink=_ForbiddenSink(), load_config=_mssql_config())
    assert pair.connector.queries == []


def test_disabled_audit_neither_requires_steps_nor_falls_back():
    pair = _pair(steps=False)
    config = _mssql_config()
    config.options["load_governance"]["audit"]["mode"] = "off"
    service = LoadGovernanceService()
    RuntimeDecisionLifecycle(
        load_governance_service=service,
        load_identity_service=LoadIdentityService(audit_storage=pair.loads),
        logger=None,
        audit_bindings=pair,
    ).configure_audit_storage(sink=_ForbiddenSink(), load_config=config)
    assert not service.audit_storage_configured
    assert pair.connector.queries == []


@pytest.mark.parametrize("shared", [False, True])
def test_worker_disposal_closes_owned_audit_connector_once(shared):
    from dpone.runtime.etl.backfill_process_runtime import close_runtime_bindings

    pair = _pair()
    closed = []
    pair.connector.close = lambda: closed.append("audit")
    endpoint = pair.connector if shared else None
    bindings = runtime_hydrator.RuntimeBindings(
        source_obj=SimpleNamespace(connector=endpoint),
        sink_obj=SimpleNamespace(connector=endpoint, state_storage=SimpleNamespace(connector=endpoint)),
        audit_bindings=pair,
    )
    close_runtime_bindings(bindings)
    assert closed == ["audit"]


@pytest.mark.parametrize("enabled", [True, False])
def test_route_decision_uses_selected_metadata_and_never_constructs_sink_factory(tmp_path, monkeypatch, enabled):
    from dpone.runtime import route_runtime_factory
    from tests.test_route_capability_runtime_factory import _cfg, _MssqlSource, _real_columnar_assembly, _Sink

    pair = _pair(steps=enabled)
    source, sink = _MssqlSource(), _Sink()
    config = _cfg()
    if not enabled:
        config.options["load_governance"]["audit"]["mode"] = "off"

    def forbidden():
        raise AssertionError("selected route constructed a sink audit factory")

    monkeypatch.setattr(route_runtime_factory, "LoadStepAuditStorageFactory", forbidden)
    factory = route_runtime_factory.RouteCapabilityRuntimeFactory(
        columnar_assembly=_real_columnar_assembly(tmp_path, sink_connector=sink.connector)
    )
    route = factory.build(load_config=config, source=source, sink=sink, audit_bindings=pair)
    route.prepare(
        load_config=config,
        source=source,
        sink=sink,
        load_record=SimpleNamespace(run_id="run-1", load_id="load-1"),
    )
    inserts = [sql for sql, _ in pair.connector.queries if "INSERT INTO" in sql]
    assert bool(inserts) is enabled
    assert all("[Example_Metadata].[dbo].[steps]" in sql for sql in inserts)
