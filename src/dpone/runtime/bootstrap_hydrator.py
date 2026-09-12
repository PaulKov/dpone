"""Default runtime hydrator implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from dpone.config.postgres_xmin_execution import require_postgres_xmin_execution_route
from dpone.contracts.runtime_connection import ResolvedBindingConnection
from dpone.ports.runtime_hydrator import RuntimeBindings
from dpone.runtime.bootstrap_config import mapping_or_empty
from dpone.runtime.bootstrap_load_identity import build_load_identity_service
from dpone.runtime.bootstrap_mssql_authority import (
    apply_connection_database_defaults,
    bind_target_atomic_state,
    preflight_target_atomic_database_authority,
)
from dpone.runtime.bootstrap_postgres_source_authority import (
    bind_postgres_source_authority,
    preflight_postgres_source_authority,
)
from dpone.runtime.bootstrap_sources_sinks import RuntimeEndpointFactory
from dpone.runtime.bootstrap_state import RuntimeStateBootstrap
from dpone.runtime.credentials.authority import canonical_runtime_endpoint_type, resolve_runtime_connections
from dpone.runtime.credentials.runtime_context import RuntimeConnectionContextLoader
from dpone.runtime.errors import RuntimeConfigurationError
from dpone.runtime.internal_query_capability import InternalQueryCapabilityIssuer
from dpone.runtime.postgres_xmin_execution import (
    PostgresXminExecutionMode,
    postgres_xmin_execution_policy,
)
from dpone.runtime.source_materialization_location import bind_source_materialization_location
from dpone.runtime.storage_policy import RuntimeStoragePolicy


class DefaultRuntimeHydrator:
    """Build runtime source/sink/logger/state objects for execution."""

    def __init__(
        self,
        *,
        state_bootstrap: RuntimeStateBootstrap | None = None,
        endpoint_factory: Any = RuntimeEndpointFactory,
        connection_context_loader: RuntimeConnectionContextLoader | None = None,
        mssql_database_authority_verifier_factory: Callable[..., Any] | None = None,
        postgres_source_authority_verifier_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._state_bootstrap = state_bootstrap or RuntimeStateBootstrap()
        self._endpoint_factory = endpoint_factory
        self._connection_context_loader = connection_context_loader or RuntimeConnectionContextLoader()
        self._mssql_database_authority_verifier_factory = mssql_database_authority_verifier_factory
        self._postgres_source_authority_verifier_factory = postgres_source_authority_verifier_factory

    def build(
        self,
        *,
        config: Mapping[str, Any],
        load_config: LoadConfig,
        sink_connection: ResolvedBindingConnection | None = None,
        state_connection: ResolvedBindingConnection | None = None,
        mssql_transaction_admission_service: Any | None = None,
        composition_transaction_fence: Any | None = None,
    ) -> RuntimeBindings:
        require_postgres_xmin_execution_route(load_config)
        sink_cfg = _canonical_endpoint_config(mapping_or_empty(config.get("sink")))
        source_cfg = _canonical_endpoint_config(mapping_or_empty(config.get("source")))
        state_cfg = _canonical_endpoint_config(mapping_or_empty(config.get("state", {})))
        runtime_config = dict(config)
        runtime_config.update(source=source_cfg, sink=sink_cfg, state=state_cfg)
        state_configured = "state" in config and config.get("state") is not None
        context = self._connection_context_loader.load()
        connections = resolve_runtime_connections(
            config=runtime_config,
            load_config=load_config,
            context=context,
        )
        _ = (mssql_transaction_admission_service, composition_transaction_fence)
        apply_connection_database_defaults(load_config=load_config, connections=connections)
        bind_source_materialization_location(load_config=load_config, connections=connections)
        runtime_storage_policy = RuntimeStoragePolicy.from_sources(
            runtime=mapping_or_empty(runtime_config.get("runtime")),
            source_options=load_config.options,
        )
        source_authority_verifier = preflight_postgres_source_authority(
            connections=connections,
            state_config=state_cfg,
            load_config=load_config,
            verifier_factory=self._postgres_source_authority_verifier_factory,
        )
        database_authority_verifier = preflight_target_atomic_database_authority(
            connections=connections,
            state_config=state_cfg,
            load_config=load_config,
            verifier_factory=self._mssql_database_authority_verifier_factory,
        )
        _inject_state_identity(config=runtime_config, load_config=load_config, context=context)
        connections = _with_issued_overlays(
            connections,
            sink_connection=sink_connection,
            state_connection=state_connection,
        )
        if connections.strict:
            state_bindings = self._state_bootstrap.build_resolved(
                state_cfg=state_cfg,
                load_config=load_config,
                state_connection=connections.state,
                proxy_connection=connections.proxy,
                state_configured=state_configured,
                sink_type=_resolved_endpoint_type(connections.sink, sink_cfg),
            )
            sink_obj = self._endpoint_factory.build_sink_resolved(
                sink_cfg,
                connections.sink,
                state_bindings.xmin_state_storage,
                proxy_connection=connections.proxy,
                shared_bq_connector=state_bindings.shared_bq_connector,
                runtime_storage_policy=runtime_storage_policy,
            )
        else:
            state_bindings = self._state_bootstrap.build(
                config=runtime_config,
                sink_cfg=sink_cfg,
                state_cfg=state_cfg,
                load_config=load_config,
                state_configured=state_configured,
            )
            sink_obj = self._endpoint_factory.build_sink(
                sink_cfg,
                state_bindings.xmin_state_storage,
                shared_bq_connector=state_bindings.shared_bq_connector,
                proxy_config=state_bindings.proxy_config,
                runtime_storage_policy=runtime_storage_policy,
            )
        bind_target_atomic_state(
            state_bindings=state_bindings,
            sink_obj=sink_obj,
            connections=connections,
            database_authority_verifier=database_authority_verifier,
        )
        source_state_storage = (
            state_bindings.kafka_offset_state_storage
            if (source_cfg or {}).get("type") == "kafka"
            else state_bindings.xmin_state_storage
        )
        sink_connector = sink_obj.connector if hasattr(sink_obj, "connector") else None
        if connections.strict:
            source_obj = self._endpoint_factory.build_source_resolved(
                source_cfg,
                connections.source,
                source_state_storage,
                sink_connector=sink_connector,
            )
        else:
            source_obj = self._endpoint_factory.build_source(
                source_cfg,
                source_state_storage,
                sink_connector=sink_connector,
            )
        bind_postgres_source_authority(
            source_obj=source_obj,
            verifier=source_authority_verifier,
        )
        _bind_internal_query_capability(
            source_obj=source_obj,
            sink_obj=sink_obj,
            connections=connections,
            source_cfg=source_cfg,
            sink_cfg=sink_cfg,
            load_config=load_config,
        )
        run_state_storage = self._state_bootstrap.build_run_state_storage(
            state_bindings=state_bindings,
            state_cfg=state_cfg,
            sink_obj=sink_obj,
        )

        from dpone.runtime.process_logging import create_etl_logger

        etl_logger = create_etl_logger()

        return RuntimeBindings(
            source_obj=source_obj,
            sink_obj=sink_obj,
            etl_logger=etl_logger,
            run_state_storage=run_state_storage,
            xmin_handoff_state_storage=getattr(state_bindings, "xmin_handoff_state_storage", None),
            partition_checkpoint_store=state_bindings.partition_checkpoint_store,
            load_identity_service=build_load_identity_service(
                audit_storage=getattr(state_bindings, "load_audit_storage", None),
                etl_logger=etl_logger,
            ),
            credential_resolution_receipts=connections.receipts,
        )

    @staticmethod
    def _build_source(
        source_cfg: Mapping[str, Any],
        xmin_state_storage: Any,
        *,
        sink_connector: Any = None,
    ) -> Any:
        return RuntimeEndpointFactory.build_source(
            source_cfg,
            xmin_state_storage,
            sink_connector=sink_connector,
        )

    @staticmethod
    def _build_sink(
        sink_cfg: Mapping[str, Any],
        xmin_state_storage: Any,
        *,
        shared_bq_connector: Any = None,
        proxy_config: Mapping[str, Any] | None = None,
        runtime_storage_policy: RuntimeStoragePolicy | None = None,
    ) -> Any:
        return RuntimeEndpointFactory.build_sink(
            sink_cfg,
            xmin_state_storage,
            shared_bq_connector=shared_bq_connector,
            proxy_config=proxy_config,
            runtime_storage_policy=runtime_storage_policy,
        )

    @staticmethod
    def _build_api_source(
        *,
        source_cfg: Mapping[str, Any],
        vault_path: str | None,
        vault_mount_point: str | None = None,
        sink_connector: Any = None,
    ) -> Any:
        return RuntimeEndpointFactory.build_api_source(
            source_cfg=source_cfg,
            vault_path=vault_path,
            vault_mount_point=vault_mount_point,
            sink_connector=sink_connector,
        )


def _bind_internal_query_capability(
    *,
    source_obj: Any,
    sink_obj: Any,
    connections: Any,
    source_cfg: Mapping[str, Any],
    sink_cfg: Mapping[str, Any],
    load_config: Any,
) -> None:
    """Bind an optional fast-path proof after both endpoint connectors exist."""

    binder = getattr(source_obj, "bind_internal_query_capability", None)
    if not callable(binder):
        return
    decision = InternalQueryCapabilityIssuer().issue(
        resolved_connections=connections,
        source_config=source_cfg,
        sink_config=sink_cfg,
        load_config=load_config,
        source_connector=getattr(source_obj, "connector", None),
        target_connector=getattr(sink_obj, "connector", None),
    )
    binder(decision)


def _resolved_endpoint_type(connection: Any, config: Mapping[str, Any]) -> str:
    descriptor = getattr(connection, "descriptor", None)
    resolved = str(getattr(descriptor, "connection_type", "") or "").strip().lower()
    return resolved or str(config.get("type") or "").strip().lower()


def _canonical_endpoint_config(config: Mapping[str, Any]) -> Mapping[str, Any]:
    """Detach and canonicalize one endpoint type without mutating the manifest."""

    if not config:
        return config
    canonical = dict(config)
    if canonical.get("type") not in (None, ""):
        canonical["type"] = canonical_runtime_endpoint_type(canonical["type"])
    return canonical


def _with_issued_overlays(
    connections: Any,
    *,
    sink_connection: ResolvedBindingConnection | None,
    state_connection: ResolvedBindingConnection | None,
) -> Any:
    """Replace resolved sink/state after ambient authority pins are proven."""

    if sink_connection is None and state_connection is None:
        return connections
    if not connections.strict:
        raise RuntimeConfigurationError("composition_issued_login_overlay_required")
    updates: dict[str, ResolvedBindingConnection] = {}
    if sink_connection is not None:
        updates["sink"] = _require_issued_overlay(sink_connection)
    if state_connection is not None:
        updates["state"] = _require_issued_overlay(state_connection)
    return replace(connections, **updates)


def _require_issued_overlay(connection: ResolvedBindingConnection) -> ResolvedBindingConnection:
    if type(connection) is not ResolvedBindingConnection:
        raise RuntimeConfigurationError("composition_issued_login_overlay_required")
    if (connection.safe_metadata or {}).get("resolver") != "composition-issued-login":
        raise RuntimeConfigurationError("composition_issued_login_overlay_required")
    return connection


def _inject_state_identity(*, config: Mapping[str, Any], load_config: Any, context: Any) -> None:
    """Attach verified environment/process dimensions for collision-safe state."""

    options = getattr(load_config, "options", {}) or {}
    policy = options.get("reconciliation")
    xmin_execution = postgres_xmin_execution_policy(options)
    reconciliation_enabled = isinstance(policy, Mapping) and bool(policy.get("enabled", True))
    if not reconciliation_enabled and xmin_execution.mode is PostgresXminExecutionMode.AUTO:
        return
    process = str(config.get("name") or "").strip()
    environment = str(getattr(context, "environment", "") or "").strip()
    if not environment or not process:
        raise RuntimeConfigurationError(
            "PostgreSQL XMin key_snapshot/handoff requires verified runtime environment and process identity"
        )
    load_config.options["state_identity"] = {
        "environment": environment,
        "process": process,
    }


__all__ = ["DefaultRuntimeHydrator"]
