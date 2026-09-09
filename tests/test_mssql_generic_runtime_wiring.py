from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
)
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.bootstrap_state_models import RuntimeStateBindings
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.state.factory import StateFactory
from dpone.runtime.state.mssql import MSSQLXMinStateStorage
from dpone.runtime.state.mssql_generic_transaction_names import GENERIC_TRANSACTION_TABLES
from dpone.runtime.state.mssql_generic_transaction_storage import (
    MssqlGenericTransactionStateStorage,
)
from dpone.runtime.state.mssql_target_identity_contract import TARGET_IDENTITY_REGISTRY_TABLE


class _ContextLoader:
    @staticmethod
    def load() -> object:
        return object()


class _EnvironmentContextLoader:
    @staticmethod
    def load() -> object:
        return SimpleNamespace(environment="ci")


class _EndpointFactory:
    target_connector = object()

    @classmethod
    def build_sink_resolved(
        cls,
        _config,
        _connection,
        state_storage,
        **_kwargs,
    ) -> SimpleNamespace:
        return SimpleNamespace(connector=cls.target_connector, state_storage=state_storage)

    @staticmethod
    def build_source_resolved(
        _config,
        _connection,
        state_storage,
        **_kwargs,
    ) -> SimpleNamespace:
        source = SimpleNamespace(state_storage=state_storage)
        source.bind_postgres_source_authority = lambda verifier: setattr(
            source,
            "source_authority_verifier",
            verifier,
        )
        return source


def _database_authority_verifier_factory(**_kwargs: object) -> SimpleNamespace:
    return SimpleNamespace(
        authority_sha256="sha256:" + "a" * 64,
        verify_pins=lambda: None,
        verify=lambda **_values: None,
    )


def _source_authority_verifier_factory(_connection: object) -> SimpleNamespace:
    identity = SourcePhysicalIdentity(
        dialect="postgres",
        cluster_identifier="761991928213",
        database="source",
        effective_principal="reader",
        session_principal="reader",
        topology_role="primary",
        version=2,
        authority_sha256="sha256:" + "b" * 64,
        timeline_id=1,
        database_oid=10,
        effective_principal_oid=11,
        session_principal_oid=11,
        schema="public",
        schema_oid=2200,
        relation="orders",
        relation_oid=12,
    )
    return SimpleNamespace(
        preflight=lambda _config: identity,
        verify_snapshot=lambda **_kwargs: identity,
    )


def _connection(connection_type: str, *, database: str, schema: str) -> ResolvedBindingConnection:
    properties = {"database": database, "schema": schema}
    if connection_type == "mssql":
        properties["database_authorities"] = {
            database: {
                "database_id": 7 if database == "DWH" else 8,
                "create_token": "2026-08-16T00:00:00.0000000",
                "database_guid": (
                    "11111111-1111-1111-1111-111111111111"
                    if database == "DWH"
                    else "22222222-2222-2222-2222-222222222222"
                ),
            }
        }
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(database=database, schema=schema),
        safe_metadata={},
        descriptor=ResolvedConnectionDescriptor(
            connection_type=connection_type,
            properties=properties,
        ),
    )


def test_production_hydration_uses_only_target_identity_and_generic_four_object_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connector = object()
    connections = SimpleNamespace(
        strict=True,
        source=_connection("postgres", database="source", schema="public"),
        sink=_connection("mssql", database="DWH", schema="dbo"),
        state=_connection("mssql", database="Example_System", schema="governance"),
        proxy=None,
        receipts=(),
    )
    monkeypatch.setattr(
        "dpone.runtime.bootstrap_hydrator.resolve_runtime_connections",
        lambda **_kwargs: connections,
    )
    monkeypatch.setattr(
        ResolvedConnectorFactory,
        "create",
        lambda *_args, **_kwargs: state_connector,
    )

    def forbidden_factory(*_args, **_kwargs):
        raise AssertionError("generic hydration must not construct XMin or secondary state objects")

    monkeypatch.setattr(StateFactory, "create_mssql_xmin_state_storage", forbidden_factory)
    monkeypatch.setattr(StateFactory, "create_mssql_run_state_storage", forbidden_factory)
    monkeypatch.setattr(StateFactory, "create_mssql_load_audit_storage", forbidden_factory)
    monkeypatch.setattr(StateFactory, "create_mssql_kafka_offset_state_storage", forbidden_factory)

    bindings = DefaultRuntimeHydrator(
        endpoint_factory=_EndpointFactory,
        connection_context_loader=_ContextLoader(),
        mssql_database_authority_verifier_factory=_database_authority_verifier_factory,
        postgres_source_authority_verifier_factory=_source_authority_verifier_factory,
    ).build(
        config={
            "source": {"type": "postgres", "connection_ref": "source"},
            "sink": {"type": "mssql", "connection_ref": "sink"},
            "state": {
                "type": "mssql",
                "connection_ref": "state",
                "atomicity": "target_atomic",
                "provisioning": "external",
                "table": {"name": "unused_xmin_table"},
            },
        },
        load_config=LoadConfig(
            source_conn_id="source",
            target_conn_id="sink",
            source_schema="public",
            source_table="orders",
            target_schema="dbo",
            target_table="orders",
            load_strategy=LoadStrategy.FULL_REFRESH,
        ),
    )

    storage = bindings.sink_obj.state_storage
    assert isinstance(storage, MssqlGenericTransactionStateStorage)
    assert bindings.source_obj.state_storage is storage
    assert storage.connector is state_connector
    assert storage.database == "Example_System"
    assert storage.schema == "governance"
    assert storage._transaction_connector is _EndpointFactory.target_connector
    assert storage.database_authority_bound is True
    assert bindings.source_obj.state_storage is storage
    assert bindings.run_state_storage is None
    assert bindings.partition_checkpoint_store is None
    assert bindings.load_identity_service.audit_storage is None
    required_objects = (
        TARGET_IDENTITY_REGISTRY_TABLE,
        *storage.required_catalog_tables,
    )
    assert required_objects == (
        TARGET_IDENTITY_REGISTRY_TABLE,
        *GENERIC_TRANSACTION_TABLES,
    )
    assert len(required_objects) == 5


def test_key_snapshot_route_keeps_xmin_storage_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connector = object()
    xmin_storage = SimpleNamespace(atomicity="target_atomic", provisioning="external")
    calls: list[str] = []

    monkeypatch.setattr(
        ResolvedConnectorFactory,
        "create",
        lambda *_args, **_kwargs: state_connector,
    )
    monkeypatch.setattr(
        StateFactory,
        "create_mssql_xmin_state_storage",
        lambda **_kwargs: calls.append("xmin") or xmin_storage,
    )
    monkeypatch.setattr(
        StateFactory,
        "create_mssql_generic_transaction_state_storage",
        lambda **_kwargs: pytest.fail("key_snapshot must retain XMin state storage"),
    )
    monkeypatch.setattr(
        StateFactory,
        "create_mssql_load_audit_storage",
        lambda **_kwargs: SimpleNamespace(create_load_table=lambda: None),
    )

    from dpone.runtime.bootstrap_state import RuntimeStateBootstrap

    bindings = RuntimeStateBootstrap().build_resolved(
        state_cfg={
            "type": "mssql",
            "atomicity": "target_atomic",
            "provisioning": "external",
            "table": {"name": "dpone_source_state"},
        },
        state_connection=_connection("mssql", database="Example_System", schema="governance"),
        load_config=LoadConfig(
            source_conn_id="source",
            target_conn_id="sink",
            source_schema="public",
            source_table="orders",
            target_schema="dbo",
            target_table="orders",
            load_strategy=LoadStrategy.INCREMENTAL_MERGE,
            options={"reconciliation": {"enabled": True, "mode": "key_snapshot"}},
        ),
        sink_type="mssql",
    )

    assert calls == ["xmin"]
    assert bindings.xmin_state_storage is xmin_storage


def test_initial_backfill_composes_generic_chunks_and_dedicated_xmin_handoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connector = object()
    generic_storage = SimpleNamespace(kind="generic")
    handoff_storage = SimpleNamespace(kind="xmin")
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(
        ResolvedConnectorFactory,
        "create",
        lambda *_args, **_kwargs: state_connector,
    )
    monkeypatch.setattr(
        StateFactory,
        "create_mssql_generic_transaction_state_storage",
        lambda **kwargs: calls.append(("generic", kwargs["mssql_connector"])) or generic_storage,
    )
    monkeypatch.setattr(
        StateFactory,
        "create_mssql_xmin_state_storage",
        lambda **kwargs: calls.append(("xmin", kwargs["mssql_connector"])) or handoff_storage,
    )
    monkeypatch.setattr(
        "dpone.runtime.bootstrap_state.is_mssql_generic_transaction_storage",
        lambda storage: storage is generic_storage,
    )

    from dpone.runtime.bootstrap_state import RuntimeStateBootstrap

    bindings = RuntimeStateBootstrap().build_resolved(
        state_cfg={
            "type": "mssql",
            "atomicity": "target_atomic",
            "provisioning": "external",
            "table": {"name": "dpone_source_state"},
        },
        state_connection=_connection("mssql", database="Example_System", schema="governance"),
        load_config=LoadConfig(
            source_conn_id="source",
            target_conn_id="sink",
            source_schema="public",
            source_table="orders",
            target_schema="dbo",
            target_table="orders",
            load_strategy=LoadStrategy.BACKFILL,
            options={
                "source_type": "postgres",
                "sink_type": "mssql",
                "incremental_strategy": "xmin",
                "xmin_execution": {"mode": "initial", "handoff_id": "orders_v1"},
                "backfill": {
                    "inner_mode": "incremental_merge",
                    "chunk": {"column": "id"},
                    "state": {"backend": "audit_schema"},
                },
            },
        ),
        sink_type="mssql",
    )

    assert bindings.xmin_state_storage is generic_storage
    assert bindings.xmin_handoff_state_storage is handoff_storage
    assert calls == [("generic", state_connector), ("xmin", state_connector)]


def test_strict_key_snapshot_hydration_binds_database_authority_to_xmin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connector = object()
    xmin_storage = MSSQLXMinStateStorage(
        state_connector,
        database="Example_System",
        schema="governance",
        atomicity="target_atomic",
        provisioning="external",
    )
    state_bindings = RuntimeStateBindings(
        state_type="mssql",
        proxy_config={},
        xmin_state_storage=xmin_storage,
        shared_mssql_state_connector=state_connector,
        mssql_state_location=SimpleNamespace(atomicity="target_atomic"),
    )

    class StateBootstrap:
        @staticmethod
        def build_resolved(**_kwargs) -> RuntimeStateBindings:
            return state_bindings

        @staticmethod
        def build_run_state_storage(**_kwargs) -> None:
            return None

    connections = SimpleNamespace(
        strict=True,
        source=_connection("postgres", database="source", schema="public"),
        sink=_connection("mssql", database="DWH", schema="dbo"),
        state=_connection("mssql", database="Example_System", schema="governance"),
        proxy=None,
        receipts=(),
    )
    monkeypatch.setattr(
        "dpone.runtime.bootstrap_hydrator.resolve_runtime_connections",
        lambda **_kwargs: connections,
    )
    load_config = LoadConfig(
        source_conn_id="source",
        target_conn_id="sink",
        source_schema="public",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        options={"reconciliation": {"enabled": True, "mode": "key_snapshot"}},
    )

    bindings = DefaultRuntimeHydrator(
        state_bootstrap=StateBootstrap(),
        endpoint_factory=_EndpointFactory,
        connection_context_loader=_EnvironmentContextLoader(),
        mssql_database_authority_verifier_factory=_database_authority_verifier_factory,
        postgres_source_authority_verifier_factory=_source_authority_verifier_factory,
    ).build(
        config={
            "name": "orders",
            "source": {"type": "postgres", "connection_ref": "source"},
            "sink": {"type": "mssql", "connection_ref": "sink"},
            "state": {
                "type": "mssql",
                "connection_ref": "state",
                "atomicity": "target_atomic",
                "provisioning": "external",
            },
        },
        load_config=load_config,
    )

    assert bindings.source_obj.state_storage is xmin_storage
    assert bindings.sink_obj.state_storage is xmin_storage
    assert xmin_storage.database_authority_bound is True
    assert load_config.options["mssql_database_authority_sha256"].startswith("sha256:")
