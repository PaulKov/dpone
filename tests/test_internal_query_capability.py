"""Runtime-issued authority regressions for same-connection query artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
    RuntimeConnectionAuthorityError,
)
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContext
from dpone.runtime.internal_query_capability import (
    INTERNAL_QUERY_CROSS_DIALECT,
    INTERNAL_QUERY_GRANTED,
    InternalQueryCapability,
    InternalQueryCapabilityDecision,
)
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class _Connector:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def get_records(self, query: str) -> list[object]:
        self.queries.append(str(query))
        return []


class _Source:
    def __init__(self, connector: _Connector) -> None:
        self.connector = connector
        self.internal_query_capability: InternalQueryCapabilityDecision | None = None

    def bind_internal_query_capability(self, decision: InternalQueryCapabilityDecision) -> None:
        self.internal_query_capability = decision


@dataclass
class _Sink:
    connector: _Connector


class _EndpointFactory:
    def __init__(self, source: _Source, sink: _Sink) -> None:
        self.source = source
        self.sink = sink
        self.events: list[str] = []

    def build_sink_resolved(self, *_args: Any, **_kwargs: Any) -> _Sink:
        self.events.append("sink")
        return self.sink

    def build_source_resolved(self, *_args: Any, **_kwargs: Any) -> _Source:
        self.events.append("source")
        return self.source

    def build_sink(self, *_args: Any, **_kwargs: Any) -> _Sink:
        self.events.append("legacy-sink")
        return self.sink

    def build_source(self, *_args: Any, **_kwargs: Any) -> _Source:
        self.events.append("legacy-source")
        return self.source


class _StateBootstrap:
    def build_resolved(self, **_kwargs: Any) -> SimpleNamespace:
        return _state_bindings()

    def build(self, **_kwargs: Any) -> SimpleNamespace:
        return _state_bindings()

    def build_run_state_storage(self, **_kwargs: Any) -> None:
        return None


@dataclass
class _ContextLoader:
    context: RuntimeConnectionContext | None

    def load(self) -> RuntimeConnectionContext | None:
        return self.context


@dataclass
class _Resolver:
    connection: ResolvedBindingConnection

    def resolve(self, connection_ref: str) -> ResolvedBindingConnection:
        del connection_ref
        return self.connection


def test_capability_cannot_be_constructed_from_authoring_metadata() -> None:
    with pytest.raises(TypeError, match="only be issued by runtime hydration"):
        InternalQueryCapability(
            dialect="postgres",
            source_database="warehouse",
            source_schema="raw",
            source_table="orders",
            source_connector_identity=1,
            target_connector_identity=2,
            _issuer_token=object(),
        )


def test_mssql_equal_alias_and_shared_connector_do_not_bypass_runtime_authority() -> None:
    connector = _Connector()
    connector.get_records_iterator = lambda _query: iter(())  # type: ignore[attr-defined]
    config = SimpleNamespace(
        source_conn_id="shared",
        target_conn_id="shared",
        source_database="warehouse",
        source_schema="dbo",
        source_table="orders",
        batch_size=100,
        options={"source_type": "mssql", "sink_type": "mssql", "mssql_export_mode": "streaming"},
    )

    artifact = MSSQLQueryoutArtifactFactory(
        connector=connector,
        logger=SimpleNamespace(log_etl_progress=lambda *_args: None),
        sink_connector=connector,
    ).artifact_for_query(config, "SELECT [id] FROM [dbo].[orders]", [("id", "int")])

    assert isinstance(artifact, StreamingRowsArtifact)


@pytest.mark.parametrize(
    ("source_type", "sink_type"),
    (("postgres", "postgres"), ("PostgreSQL", "postgresql")),
)
def test_hydrator_issues_capability_only_after_target_relation_probe(
    source_type: str,
    sink_type: str,
) -> None:
    binding = _binding("postgres")
    source_connector = _Connector()
    target_connector = _Connector()
    source = _Source(source_connector)
    endpoints = _EndpointFactory(source, _Sink(target_connector))

    DefaultRuntimeHydrator(
        state_bootstrap=_StateBootstrap(),
        endpoint_factory=endpoints,
        connection_context_loader=_ContextLoader(_context(binding)),
    ).build(
        config={
            "source": {"type": source_type, "connection_ref": "shared"},
            "sink": {"type": sink_type, "connection_ref": "shared"},
            "state": {"type": "disabled"},
        },
        load_config=_load_config(),
    )

    assert endpoints.events == ["sink", "source"]
    assert target_connector.queries == ['SELECT 1 FROM "raw"."orders" WHERE FALSE']
    assert source.internal_query_capability is not None
    assert source.internal_query_capability.diagnostic.code == INTERNAL_QUERY_GRANTED
    assert source.internal_query_capability.authorizes(
        source_connector=source_connector,
        dialect="postgres",
        source_database="warehouse",
        source_schema="raw",
        source_table="orders",
    )


def test_strict_equal_alias_cross_dialect_fails_before_endpoint_hydration() -> None:
    binding = _binding("postgres")
    endpoints = _EndpointFactory(_Source(_Connector()), _Sink(_Connector()))

    with pytest.raises(RuntimeConnectionAuthorityError) as raised:
        DefaultRuntimeHydrator(
            state_bootstrap=_StateBootstrap(),
            endpoint_factory=endpoints,
            connection_context_loader=_ContextLoader(_context(binding)),
        ).build(
            config={
                "source": {"type": "postgres", "connection_ref": "shared"},
                "sink": {"type": "mssql", "connection_ref": "shared"},
                "state": {"type": "disabled"},
            },
            load_config=_load_config(),
        )

    assert raised.value.code == "DPONE_RUNTIME_CONNECTION_TYPE_MISMATCH"
    assert endpoints.events == []


def test_legacy_equal_alias_cross_dialect_binds_explicit_file_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    from dpone.runtime.credentials import authority_resolution

    monkeypatch.setattr(authority_resolution, "_legacy_warning_emitted", False)
    source = _Source(_Connector())
    target_connector = _Connector()
    endpoints = _EndpointFactory(source, _Sink(target_connector))
    config = {
        "runtime": {"compatibility": {"legacy_runtime_connections": "explicit_only"}},
        "source": {
            "type": "postgres",
            "connection_id": "shared",
            "connection_type": "params",
        },
        "sink": {
            "type": "mssql",
            "connection_id": "shared",
            "connection_type": "params",
        },
        "state": {"type": "disabled"},
    }

    with pytest.warns(DeprecationWarning):
        DefaultRuntimeHydrator(
            state_bootstrap=_StateBootstrap(),
            endpoint_factory=endpoints,
            connection_context_loader=_ContextLoader(None),
        ).build(config=config, load_config=_load_config())

    assert endpoints.events == ["legacy-sink", "legacy-source"]
    assert target_connector.queries == []
    assert source.internal_query_capability is not None
    assert not source.internal_query_capability.eligible
    assert source.internal_query_capability.diagnostic.code == INTERNAL_QUERY_CROSS_DIALECT


def _binding(connection_type: str) -> ResolvedBindingConnection:
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(
            host="db.example",
            port=5432,
            database="warehouse",
            username="runtime",
            password="secret",
        ),
        safe_metadata={"connection_ref": "shared"},
        descriptor=ResolvedConnectionDescriptor(connection_type=connection_type, properties={}),
    )


def _context(binding: ResolvedBindingConnection) -> RuntimeConnectionContext:
    return RuntimeConnectionContext(
        environment="test",
        binding_set={},
        connection_registry={},
        credential_runtime={},
        resolver=_Resolver(binding),
    )


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="shared",
        target_conn_id="shared",
        source_schema="raw",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        source_database="warehouse",
        target_database="warehouse",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )


def _state_bindings() -> SimpleNamespace:
    return SimpleNamespace(
        xmin_state_storage=None,
        kafka_offset_state_storage=None,
        partition_checkpoint_store=None,
        shared_bq_connector=None,
        shared_mssql_state_connector=None,
        mssql_state_location=None,
        load_audit_storage=None,
        proxy_config=None,
        proxy_connection=None,
    )
