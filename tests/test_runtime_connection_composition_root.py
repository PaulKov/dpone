from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityContractError
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
    RuntimeConnectionAuthorityError,
)
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.credentials import authority_resolution
from dpone.runtime.credentials.authority import resolve_runtime_connections
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.credentials.runtime_context import (
    RUNTIME_CONNECTION_CONTEXT_ENV,
    RUNTIME_INIT_FETCH_PLAN_B64_ENV,
    RUNTIME_INIT_FETCH_PLAN_SHA256_ENV,
    RuntimeConnectionContext,
    RuntimeConnectionContextLoader,
)
from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import bind_behavior_scenario
from tests.test_runtime_connection_context_loader import (
    _replace_plan_descriptor,
    _runtime_context,
)

_MSSQL_ALIASES = (
    "mssql",
    "MSSQL",
    "microsoft mssql",
    "microsoft_mssql",
    "odbc",
    "sqlserver",
    "sql_server",
    "sql-server",
)


def test_strict_endpoint_import_does_not_load_legacy_credential_factory() -> None:
    script = textwrap.dedent(
        """
        import sys

        from dpone.runtime.bootstrap_sources_sinks import RuntimeEndpointFactory
        from dpone.runtime.credentials.resolved_endpoint_factory import (
            ResolvedEndpointFactory,
        )

        assert RuntimeEndpointFactory is not None
        assert ResolvedEndpointFactory is not None
        assert "dpone.runtime.credentials.connector_factory" not in sys.modules
        """
    )

    subprocess.run([sys.executable, "-c", script], check=True)


def test_legacy_factory_import_does_not_construct_credentials_manager() -> None:
    script = textwrap.dedent(
        """
        import dpone.runtime.credentials.manager as manager_module

        class ForbiddenCredentialsManager:
            def __init__(self):
                raise AssertionError("credential manager constructed during import")

        manager_module.CredentialsManager = ForbiddenCredentialsManager

        from dpone.runtime.credentials.connector_factory import BaseFactory

        assert BaseFactory.manager is None
        """
    )

    subprocess.run([sys.executable, "-c", script], check=True)


def test_strict_hydrator_resolves_every_ref_before_building_runtime_objects() -> None:
    events: list[str] = []
    context = RuntimeConnectionContext(
        environment="prod",
        binding_set={},
        connection_registry={},
        credential_runtime={},
        resolver=_Resolver(
            events=events,
            connections={
                "source-main": _connection("postgres"),
                "sink-main": _connection("clickhouse"),
            },
        ),
    )
    state_bootstrap = _StateBootstrap(events)

    bindings = DefaultRuntimeHydrator(
        state_bootstrap=state_bootstrap,
        endpoint_factory=_EndpointFactory(events),
        connection_context_loader=_ContextLoader(context),
    ).build(
        config={
            "source": {"type": "postgres", "connection_ref": "source-main"},
            "sink": {"type": "clickhouse", "connection_ref": "sink-main"},
            "state": {"type": "disabled"},
        },
        load_config=_load_config(),
    )

    assert events == [
        "resolve:sink-main",
        "resolve:source-main",
        "state",
        "sink",
        "source",
        "run-state",
    ]
    assert bindings.source_obj == "source"
    assert bindings.sink_obj == "sink"
    assert [receipt["resolver"] for receipt in bindings.credential_resolution_receipts] == [
        "vault_kv",
        "vault_kv",
    ]


@pytest.mark.parametrize("source_type", ("postgres", "postgresql", "PostgreSQL"))
@pytest.mark.parametrize("sink_type", _MSSQL_ALIASES)
def test_strict_hydrator_canonicalizes_manifest_endpoint_aliases(
    source_type: str,
    sink_type: str,
) -> None:
    events: list[str] = []
    context = RuntimeConnectionContext(
        environment="prod",
        binding_set={},
        connection_registry={},
        credential_runtime={},
        resolver=_Resolver(
            events=events,
            connections={
                "source-main": _connection("postgres"),
                "sink-main": _connection("mssql"),
            },
        ),
    )
    endpoints = _CanonicalEndpointFactory(events)

    DefaultRuntimeHydrator(
        state_bootstrap=_StateBootstrap(events),
        endpoint_factory=endpoints,
        connection_context_loader=_ContextLoader(context),
    ).build(
        config={
            "source": {"type": source_type, "connection_ref": "source-main"},
            "sink": {"type": sink_type, "connection_ref": "sink-main"},
            "state": {"type": "disabled"},
        },
        load_config=_load_config(),
    )

    assert endpoints.source_type == "postgres"
    assert endpoints.sink_type == "mssql"


def test_strict_hydrator_keeps_real_alias_type_mismatch_fail_closed() -> None:
    events: list[str] = []
    context = RuntimeConnectionContext(
        environment="prod",
        binding_set={},
        connection_registry={},
        credential_runtime={},
        resolver=_Resolver(
            events=events,
            connections={
                "source-main": _connection("postgres"),
                "sink-main": _connection("clickhouse"),
            },
        ),
    )

    with pytest.raises(RuntimeConnectionAuthorityError) as raised:
        DefaultRuntimeHydrator(
            state_bootstrap=_StateBootstrap(events),
            endpoint_factory=_CanonicalEndpointFactory(events),
            connection_context_loader=_ContextLoader(context),
        ).build(
            config={
                "source": {"type": "postgresql", "connection_ref": "source-main"},
                "sink": {"type": "odbc", "connection_ref": "sink-main"},
                "state": {"type": "disabled"},
            },
            load_config=_load_config(),
        )

    assert raised.value.code == "DPONE_RUNTIME_CONNECTION_TYPE_MISMATCH"


@pytest.mark.parametrize("endpoint_type", _MSSQL_ALIASES)
def test_direct_strict_resolution_requires_mssql_database_authority_for_every_alias(
    endpoint_type: str,
) -> None:
    context = RuntimeConnectionContext(
        environment="prod",
        binding_set={},
        connection_registry={},
        credential_runtime={},
        resolver=_Resolver(events=[], connections={"sink-main": _connection("mssql")}),
    )
    load_config = _load_config()
    load_config.target_database = "DWH"
    load_config.staging_database = "DWH"

    with pytest.raises(
        MssqlDatabaseAuthorityContractError,
        match="mssql_transaction.target_database_authorities_required",
    ):
        resolve_runtime_connections(
            config={
                "source": {"type": "api", "api_type": "cbr"},
                "sink": {"type": endpoint_type, "connection_ref": "sink-main"},
                "state": {
                    "type": endpoint_type,
                    "reuse": "sink",
                    "atomicity": "target_atomic",
                },
            },
            load_config=load_config,
            context=context,
        )


@pytest.mark.parametrize("source_type", ("postgres", "postgresql", "PostgreSQL"))
def test_direct_strict_resolution_requires_postgres_source_authority_for_every_alias(
    source_type: str,
) -> None:
    context = RuntimeConnectionContext(
        environment="prod",
        binding_set={},
        connection_registry={},
        credential_runtime={},
        resolver=_Resolver(
            events=[],
            connections={
                "source-main": _connection("postgres"),
                "sink-main": _connection("mssql"),
            },
        ),
    )

    with pytest.raises(RuntimeConnectionAuthorityError) as raised:
        resolve_runtime_connections(
            config={
                "source": {"type": source_type, "connection_ref": "source-main"},
                "sink": {"type": "odbc", "connection_ref": "sink-main"},
                "state": {"type": "odbc", "reuse": "sink", "atomicity": "target_atomic"},
            },
            load_config=_load_config(),
            context=context,
        )

    assert raised.value.code == "DPONE_POSTGRES_SOURCE_AUTHORITY_INVALID"


def test_credential_free_api_source_does_not_require_connection_ref() -> None:
    context = RuntimeConnectionContext(
        environment="prod",
        binding_set={},
        connection_registry={},
        credential_runtime={},
        resolver=_Resolver(
            events=[],
            connections={"sink-main": _connection("clickhouse")},
        ),
    )

    resolved = resolve_runtime_connections(
        config={
            "source": {"type": "api", "api_type": "cbr"},
            "sink": {"type": "clickhouse", "connection_ref": "sink-main"},
            "state": {"type": "disabled"},
        },
        load_config=_load_config(),
        context=context,
    )

    assert resolved.source is None
    assert resolved.sink is not None


def test_source_materialization_work_ref_is_resolved_with_runtime_connections() -> None:
    events: list[str] = []
    source = _connection_with_location(
        "mssql",
        host="sql.example",
        database="analytics_staging",
        schema="dbo",
    )
    work = _connection_with_location(
        "mssql",
        host="sql.example",
        database="DWH_Dev",
        schema="system",
    )
    context = RuntimeConnectionContext(
        environment="dev",
        binding_set={},
        connection_registry={},
        credential_runtime={},
        resolver=_Resolver(
            events=events,
            connections={
                "source-main": source,
                "sink-main": _connection("clickhouse"),
                "mssql-dpone-snapshot-work": work,
            },
        ),
    )
    load_config = _load_config()
    load_config.options = {
        "native_transfer": {
            "snapshot": {
                "materialization": {
                    "work_connection_ref": "mssql-dpone-snapshot-work",
                }
            }
        }
    }

    resolved = resolve_runtime_connections(
        config={
            "source": {"type": "mssql", "connection_ref": "source-main"},
            "sink": {"type": "clickhouse", "connection_ref": "sink-main"},
            "state": {"type": "disabled"},
        },
        load_config=load_config,
        context=context,
    )

    assert events == [
        "resolve:mssql-dpone-snapshot-work",
        "resolve:sink-main",
        "resolve:source-main",
    ]
    assert resolved.source_materialization is work


def test_source_materialization_work_ref_requires_verified_context() -> None:
    load_config = _load_config()
    load_config.options = {
        "native_transfer": {
            "snapshot": {
                "materialization": {
                    "work_connection_ref": "mssql-dpone-snapshot-work",
                }
            }
        }
    }

    with pytest.raises(RuntimeConnectionAuthorityError) as raised:
        resolve_runtime_connections(
            config={
                "runtime": {"compatibility": {"legacy_runtime_connections": "explicit_only"}},
                "source": {
                    "type": "mssql",
                    "connection_id": "source-main",
                    "connection_type": "params",
                },
                "sink": {
                    "type": "clickhouse",
                    "connection_id": "sink-main",
                    "connection_type": "params",
                },
                "state": {"type": "disabled"},
            },
            load_config=load_config,
            context=None,
        )

    assert raised.value.code == "DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED"


def test_pinned_init_fetch_context_reaches_strict_hydrator_and_resolved_factories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vault_credentials = {
        "resolver": "vault_kv",
        "mount": "kv",
        "kv_version": 2,
        "path": "dpone/prod/credentials/orders",
        "fields": {"username": "db_user", "password": "db_password"},
        "version_policy": "latest",
        "resolution_scope": "workload_start",
    }
    environ, context_root = _runtime_context(tmp_path, source_credentials=vault_credentials)
    registry_path = context_root / "connection-registry.json"
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    registry["connections"]["sink-registry"]["credentials"] = dict(vault_credentials)
    registry["connections"]["source-registry"]["connection"] = {"host": "source.example"}
    registry["connections"]["sink-registry"]["connection"] = {"host": "sink.example"}
    registry_bytes = json.dumps(
        registry,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    registry_path.write_bytes(registry_bytes)
    environ = _replace_plan_descriptor(environ, name="connection_registry", content=registry_bytes)
    credential_runtime = {
        "schema": "dpone.credential-runtime.v1",
        "environment": "prod",
        "vault": {
            "address": "https://vault.example",
            "auth": {"method": "kubernetes", "role": "dpone-runtime"},
        },
    }
    credential_bytes = json.dumps(
        credential_runtime,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    (context_root / "credential-runtime.json").write_bytes(credential_bytes)
    environ = _replace_plan_descriptor(environ, name="credential_runtime", content=credential_bytes)
    for key, value in environ.items():
        monkeypatch.setenv(key, value)

    class _VaultReader:
        def get_secret(self, *, mount_point: str, path: str) -> dict[str, object]:
            del mount_point, path
            return {"db_user": "runtime", "db_password": "secret", "_metadata": {"version": 9}}

    events: list[str] = []
    endpoint_factory = _RecordingResolvedEndpointFactory(events)
    state_bootstrap = _StateBootstrap(events)

    bindings = DefaultRuntimeHydrator(
        state_bootstrap=state_bootstrap,
        endpoint_factory=endpoint_factory,
        connection_context_loader=RuntimeConnectionContextLoader(
            vault_reader_factory=lambda _: _VaultReader(),
        ),
    ).build(
        config={
            "source": {"type": "postgres", "connection_ref": "source-main"},
            "sink": {"type": "clickhouse", "connection_ref": "sink-main"},
            "state": {"type": "disabled"},
        },
        load_config=_load_config(),
    )

    assert events[:2] == ["state", "sink"]
    assert events[2] == "source"
    assert endpoint_factory.sink_connection is not None
    assert endpoint_factory.source_connection is not None
    assert endpoint_factory.sink_connection.descriptor is not None
    assert endpoint_factory.source_connection.descriptor is not None
    assert endpoint_factory.sink_connection.descriptor.connection_type == "clickhouse"
    assert endpoint_factory.source_connection.descriptor.connection_type == "postgres"
    assert endpoint_factory.source_connection.descriptor.properties["host"] == "source.example"
    assert endpoint_factory.sink_connection.descriptor.properties["host"] == "sink.example"
    assert endpoint_factory.source_connection.credentials.username == "runtime"
    assert "secret" not in repr(bindings.credential_resolution_receipts)
    assert all("vault_path" not in receipt for receipt in bindings.credential_resolution_receipts)
    assert all(receipt.get("resolver") == "vault_kv" for receipt in bindings.credential_resolution_receipts)
    assert RUNTIME_CONNECTION_CONTEXT_ENV in environ
    assert RUNTIME_INIT_FETCH_PLAN_B64_ENV in environ
    assert RUNTIME_INIT_FETCH_PLAN_SHA256_ENV in environ


def test_legacy_hydrator_keeps_explicit_compatibility_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    state_bootstrap = _StateBootstrap(events)
    monkeypatch.setattr(authority_resolution, "_legacy_warning_emitted", False)
    legacy_config = {
        "runtime": {
            "compatibility": {
                "legacy_runtime_connections": "explicit_only",
            }
        },
        "source": {
            "type": "postgres",
            "connection_id": "source-main",
            "connection_type": "params",
        },
        "sink": {
            "type": "clickhouse",
            "connection_id": "sink-main",
            "connection_type": "params",
        },
        "state": {"type": "disabled"},
    }

    with pytest.warns(DeprecationWarning) as caught:
        DefaultRuntimeHydrator(
            state_bootstrap=state_bootstrap,
            endpoint_factory=_EndpointFactory(events),
            connection_context_loader=_ContextLoader(None),
        ).build(
            config=legacy_config,
            load_config=_load_config(),
        )

    with warnings.catch_warnings(record=True) as repeated:
        warnings.simplefilter("always")
        resolve_runtime_connections(
            config=legacy_config,
            load_config=_load_config(),
            context=None,
        )

    assert len(caught) == 1
    assert repeated == []
    assert events == ["legacy-state", "legacy-sink", "legacy-source", "run-state"]


def test_legacy_hydrator_canonicalizes_aliases_before_endpoint_factories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    endpoints = _CanonicalEndpointFactory(events)
    monkeypatch.setattr(authority_resolution, "_legacy_warning_emitted", False)

    with pytest.warns(DeprecationWarning):
        DefaultRuntimeHydrator(
            state_bootstrap=_StateBootstrap(events),
            endpoint_factory=endpoints,
            connection_context_loader=_ContextLoader(None),
        ).build(
            config={
                "runtime": {"compatibility": {"legacy_runtime_connections": "explicit_only"}},
                "source": {
                    "type": "PostgreSQL",
                    "connection_id": "source-main",
                    "connection_type": "params",
                },
                "sink": {
                    "type": "odbc",
                    "connection_id": "sink-main",
                    "connection_type": "params",
                },
                "state": {"type": "disabled"},
            },
            load_config=_load_config(),
        )

    assert endpoints.source_type == "postgres"
    assert endpoints.sink_type == "mssql"


@pytest.mark.parametrize(
    "source,state",
    [
        (
            {"type": "api", "api_type": "cbr", "connection_id": "legacy"},
            {"type": "disabled"},
        ),
        (
            {"type": "postgres", "connection_ref": "source-main"},
            {"reuse": "sink", "connection_id": "legacy"},
        ),
    ],
)
def test_strict_context_rejects_every_legacy_connection_authority(
    source: dict[str, Any],
    state: dict[str, Any],
) -> None:
    context = RuntimeConnectionContext(
        environment="prod",
        binding_set={},
        connection_registry={},
        credential_runtime={},
        resolver=_Resolver(
            events=[],
            connections={
                "source-main": _connection("postgres"),
                "sink-main": _connection("clickhouse"),
            },
        ),
    )

    with pytest.raises(RuntimeConnectionAuthorityError) as exc:
        resolve_runtime_connections(
            config={
                "source": source,
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": "sink-main",
                },
                "state": state,
            },
            load_config=_load_config(),
            context=context,
        )

    assert exc.value.code == "DPONE_RUNTIME_CONNECTION_AUTHORITY_CONFLICT"


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source-main",
        target_conn_id="sink-main",
        source_schema="raw",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )


def _connection(connection_type: str) -> ResolvedBindingConnection:
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(),
        safe_metadata={
            "connection_ref": f"{connection_type}-registry",
            "resolver": "vault_kv",
            "resolved_version": 17,
        },
        descriptor=ResolvedConnectionDescriptor(
            connection_type=connection_type,
            properties={},
        ),
    )


def _connection_with_location(
    connection_type: str,
    *,
    host: str,
    database: str,
    schema: str,
) -> ResolvedBindingConnection:
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(
            host=host,
            port=1433,
            database=database,
            schema=schema,
        ),
        safe_metadata={"connection_ref": f"{connection_type}-registry"},
        descriptor=ResolvedConnectionDescriptor(
            connection_type=connection_type,
            properties={},
        ),
    )


@dataclass
class _ContextLoader:
    context: RuntimeConnectionContext | None

    def load(self) -> RuntimeConnectionContext | None:
        return self.context


@dataclass
class _Resolver:
    events: list[str]
    connections: dict[str, ResolvedBindingConnection]

    def resolve(self, connection_ref: str) -> ResolvedBindingConnection:
        self.events.append(f"resolve:{connection_ref}")
        return self.connections[connection_ref]


@dataclass
class _StateBindings:
    xmin_state_storage: Any = None
    kafka_offset_state_storage: Any = None
    partition_checkpoint_store: Any = None
    shared_bq_connector: Any = None
    proxy_config: dict[str, Any] | None = None


class _StateBootstrap:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    def build_resolved(self, **_: Any) -> _StateBindings:
        self._events.append("state")
        return _StateBindings()

    def build(self, **_: Any) -> _StateBindings:
        self._events.append("legacy-state")
        return _StateBindings()

    def build_run_state_storage(self, **_: Any) -> None:
        self._events.append("run-state")


class _EndpointFactory:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    def build_sink_resolved(self, *_: Any, **__: Any) -> str:
        self._events.append("sink")
        return "sink"

    def build_source_resolved(self, *_: Any, **__: Any) -> str:
        self._events.append("source")
        return "source"

    def build_sink(self, *_: Any, **__: Any) -> str:
        self._events.append("legacy-sink")
        return "sink"

    def build_source(self, *_: Any, **__: Any) -> str:
        self._events.append("legacy-source")
        return "source"


class _CanonicalEndpointFactory(_EndpointFactory):
    def __init__(self, events: list[str]) -> None:
        super().__init__(events)
        self.source_type: str | None = None
        self.sink_type: str | None = None

    def build_sink_resolved(self, sink_cfg: dict[str, Any], *_: Any, **__: Any) -> str:
        self.sink_type = str(sink_cfg.get("type"))
        return super().build_sink_resolved(sink_cfg)

    def build_source_resolved(self, source_cfg: dict[str, Any], *_: Any, **__: Any) -> str:
        self.source_type = str(source_cfg.get("type"))
        return super().build_source_resolved(source_cfg)

    def build_sink(self, sink_cfg: dict[str, Any], *_: Any, **__: Any) -> str:
        self.sink_type = str(sink_cfg.get("type"))
        return super().build_sink(sink_cfg)

    def build_source(self, source_cfg: dict[str, Any], *_: Any, **__: Any) -> str:
        self.source_type = str(source_cfg.get("type"))
        return super().build_source(source_cfg)


class _RecordingResolvedEndpointFactory:
    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.source_connection: ResolvedBindingConnection | None = None
        self.sink_connection: ResolvedBindingConnection | None = None

    def build_sink_resolved(
        self,
        _sink_cfg: Any,
        connection: ResolvedBindingConnection,
        *_: Any,
        **__: Any,
    ) -> str:
        self.sink_connection = connection
        self._events.append("sink")
        return "sink"

    def build_source_resolved(
        self,
        _source_cfg: Any,
        connection: ResolvedBindingConnection,
        *_: Any,
        **__: Any,
    ) -> str:
        self.source_connection = connection
        self._events.append("source")
        return "source"


def _source_schema_default_behavior(_modules: dict[str, Any]) -> dict[str, bool]:
    import dpone
    from dpone.app.runtime_bootstrap import build_default_runtime_hydrator
    from dpone.runtime.errors import RuntimeConfigurationError

    forbidden_public_symbols = {
        "PostgresMssqlSelectedRelationSchemaAuthorityV1",
        "PostgresMssqlSourceSchemaRuntime",
        "PostgresMssqlSourceSchemaRuntimeV1",
        "PreparedPostgresSourceBoundary",
    }
    declared_public = set(getattr(dpone, "__all__", ()))
    lazy_public = set(getattr(dpone, "_EXPORTS", ()))
    loaded_public = set(vars(dpone))
    app_hydrator = build_default_runtime_hydrator()

    events: list[str] = []

    class Resolver:
        def require_runtime_activatable(self, **_kwargs: Any) -> object:
            return object()

    class CorrectnessRuntimeFactory:
        def build(self, **_kwargs: Any) -> object:
            return object()

    config = {
        "runtime": {"compatibility": {"legacy_runtime_connections": "explicit_only"}},
        "source": {"type": "postgres", "connection_id": "source-main", "connection_type": "params"},
        "sink": {"type": "mssql", "connection_id": "sink-main", "connection_type": "params"},
        "state": {"type": "disabled"},
    }
    error = None
    try:
        DefaultRuntimeHydrator(
            state_bootstrap=_StateBootstrap(events),
            endpoint_factory=_EndpointFactory(events),
            connection_context_loader=_ContextLoader(None),
            postgres_mssql_correctness_route_resolver=Resolver(),
            postgres_mssql_correctness_runtime_factory=CorrectnessRuntimeFactory(),
            postgres_mssql_source_schema_runtime_factory=None,
        ).build(config=config, load_config=_load_config())
    except RuntimeConfigurationError as exc:
        error = exc
    except TypeError:
        pytest.fail("source-schema runtime factory injection is absent", pytrace=False)
    return {
        "factory_absent": getattr(error, "code", str(error))
        == "DPONE_POSTGRES_MSSQL_SOURCE_SCHEMA_AUTHORITY_RUNTIME_REQUIRED",
        "app_composition_unregistered": getattr(
            app_hydrator,
            "_postgres_mssql_source_schema_runtime_factory",
            None,
        )
        is None,
        "failure_before_source_object": "source" not in events,
        "no_public_export": forbidden_public_symbols.isdisjoint(declared_public | lazy_public | loaded_public),
    }


bind_behavior_scenario("runtime.default-activation-blocked", _source_schema_default_behavior)
