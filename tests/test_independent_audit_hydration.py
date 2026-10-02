"""Real audit-store preflight precedes business endpoints with state disabled."""

import pytest

from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.state.factory import StateFactory
from tests.test_independent_audit_selection import _setup
from tests.test_mssql_step_audit_location import _columns
from tests.test_runtime_connection_composition_root import _ContextLoader, _EndpointFactory


class _AuditCatalog:
    def __init__(self, events):
        self.events = events
        self.columns = {"dpone_load_audit": _load_columns(), "__dpone__load_steps": _columns()}
        self.closed = 0
        self.writes = []

    def close(self):
        self.closed += 1

    def get_records(self, sql, params, *, as_dict):
        assert as_dict
        table = params[-1]
        assert table in self.columns  # no checkpoint/run/xmin relation
        if "FROM [Example_Metadata].sys.columns AS c" in sql:
            assert "[Example_Metadata].sys.columns" in sql
            assert params == ("Example_Metadata", "ops", table)
            self.events.append("columns:" + table)
            return self.columns[table]
        assert "[Example_Metadata].sys.indexes" in sql
        assert params == ("ops", table)
        self.events.append("indexes:" + table)
        return [{"index_id": 1, "column_name": "load_id", "key_ordinal": 1}] if table == "dpone_load_audit" else []

    def execute_query(self, sql, params=None):
        self.writes.append((sql, params))
        raise AssertionError("Hydration must not provision or write audit/business data")

    @staticmethod
    def quote_identifier(value):
        return "[" + value.replace("]", "]]") + "]"

    @staticmethod
    def qualified_name(schema, table, *, database=None):
        return ".".join(f"[{part}]" for part in (database, schema, table) if part)


def _load_columns():
    # Independent catalog fixture, not generated from implementation contracts.
    shapes = [
        ("run_id", "char", 26, False),
        ("load_id", "char", 26, False),
        ("status", "nvarchar", 64, False),
        ("process_name", "nvarchar", 1024, True),
        ("strategy", "nvarchar", 128, False),
        *(
            (name, "nvarchar", 512, False)
            for name in ("source_schema", "source_table", "target_schema", "target_table")
        ),
        *(
            (name, "datetime2", 8, nullable)
            for name, nullable in (
                ("started_at", False),
                ("staged_at", True),
                ("committed_at", True),
                ("failed_at", True),
                ("__dpone__loaded_at", False),
            )
        ),
        *(
            (name, "bigint", 8, True)
            for name in (
                "extracted_rows",
                "staged_rows",
                "inserted_rows",
                "updated_rows",
                "loaded_rows",
                "deleted_rows",
                "reactivated_rows",
                "unchanged_rows",
                "soft_deleted_rows",
                "hard_deleted_rows",
                "active_rows",
                "total_rows",
            )
        ),
        ("commit_receipt_id", "nvarchar", 256, True),
        ("commit_outcome", "nvarchar", 128, True),
        ("error_message", "nvarchar", -1, True),
        ("artifact_uri", "nvarchar", -1, True),
    ]
    flags = _columns()[0]
    return [
        dict(
            flags,
            column_name=name,
            type_name=kind,
            max_length=length,
            is_nullable=nullable,
            scale=7 if kind == "datetime2" else 0,
            precision=19 if kind == "bigint" else 0,
            is_ansi_padded=kind in {"char", "nvarchar"},
        )
        for name, kind, length, nullable in shapes
    ]


def _case(monkeypatch, *, endpoint_failure=None):
    events, connections, context, config = _setup()
    catalog = _AuditCatalog(events)

    def create(connection, **kwargs):
        assert connection is connections["audit-main"]
        events.append("audit-connector")
        return catalog

    def forbidden(**kwargs):
        raise AssertionError("Independent audit must not enable source state")

    monkeypatch.setattr(ResolvedConnectorFactory, "create", create)
    for method in (
        "create_mssql_xmin_state_storage",
        "create_mssql_run_state_storage",
        "create_mssql_kafka_offset_state_storage",
    ):
        monkeypatch.setattr(StateFactory, method, forbidden)

    class Endpoints(_EndpointFactory):
        def build_sink_resolved(self, config, connection, state, **kwargs):
            assert state is None
            if endpoint_failure == "sink":
                raise RuntimeError("sink construction failed")
            return super().build_sink_resolved(config, connection, state, **kwargs)

        def build_source_resolved(self, config, connection, state, **kwargs):
            assert state is None
            if endpoint_failure == "source":
                raise RuntimeError("source construction failed")
            return super().build_source_resolved(config, connection, state, **kwargs)

    hydrator = DefaultRuntimeHydrator(
        endpoint_factory=Endpoints(events), connection_context_loader=_ContextLoader(context)
    )
    return events, config, catalog, hydrator


def test_real_external_pair_is_admitted_before_business_io_without_state(monkeypatch):
    events, config, catalog, hydrator = _case(monkeypatch)
    bindings = hydrator.build(config=config, load_config=LoadConfigBuilder().build(config))
    assert bindings.audit_bindings is not None
    pair = bindings.audit_bindings
    assert pair.connector is catalog
    assert bindings.load_identity_service.audit_storage is pair.loads
    assert pair.loads.fq_table == "[Example_Metadata].[ops].[dpone_load_audit]"
    assert pair.steps.fq_table == "[Example_Metadata].[ops].[__dpone__load_steps]"
    assert (
        bindings.run_state_storage is bindings.partition_checkpoint_store is bindings.xmin_handoff_state_storage is None
    )
    assert events[-6:] == [
        "columns:dpone_load_audit",
        "indexes:dpone_load_audit",
        "columns:__dpone__load_steps",
        "indexes:__dpone__load_steps",
        "sink",
        "source",
    ]
    assert catalog.closed == 0  # ownership transferred to normal runtime disposal
    assert catalog.writes == []


@pytest.mark.parametrize("table", ["dpone_load_audit", "__dpone__load_steps"])
@pytest.mark.parametrize("defect", ["missing", "drift"])
def test_external_pair_failure_closes_connector_before_any_business_endpoint(monkeypatch, table, defect):
    events, config, catalog, hydrator = _case(monkeypatch)
    if defect == "missing":
        catalog.columns[table] = []
    else:
        catalog.columns[table][0]["max_length"] = 1
    with pytest.raises(RuntimeError, match="mssql_external_state_contract"):
        hydrator.build(config=config, load_config=LoadConfigBuilder().build(config))
    assert catalog.closed == 1
    assert catalog.writes == []
    assert "source" not in events and "sink" not in events


@pytest.mark.parametrize("endpoint_failure", ["source", "sink"])
def test_later_hydration_failure_does_not_leak_admitted_audit_connection(monkeypatch, endpoint_failure):
    _, config, catalog, hydrator = _case(monkeypatch, endpoint_failure=endpoint_failure)
    with pytest.raises(RuntimeError, match="construction failed"):
        hydrator.build(config=config, load_config=LoadConfigBuilder().build(config))
    assert catalog.closed == 1


def test_disabled_step_audit_retains_explicit_load_identity_ledger(monkeypatch):
    events, config, catalog, hydrator = _case(monkeypatch)
    config["sink"]["options"]["load_governance"]["audit"]["enabled"] = False
    bindings = hydrator.build(config=config, load_config=LoadConfigBuilder().build(config))
    assert bindings.audit_bindings is not None
    assert bindings.audit_bindings.steps is None
    assert bindings.load_identity_service.audit_storage is bindings.audit_bindings.loads
    assert not any("__dpone__load_steps" in event for event in events)
    assert catalog.closed == 0 and catalog.writes == []
