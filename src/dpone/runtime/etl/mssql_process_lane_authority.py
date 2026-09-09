"""Parent-owned identity authority for spawned MSSQL backfill lanes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any

from dpone.backfill.mssql_receipt_recovery import MssqlShadowInitialReceiptRecovery
from dpone.backfill.process_lane_contracts import ProcessLaneDispatch
from dpone.backfill.shadow_append_authority import require_shadow_append_authority
from dpone.runtime.etl.mssql_transaction_identity import (
    invocation_identity,
    invocation_route_fingerprint,
    is_mssql_transaction_operation,
    operation_request,
    require_source_physical_identity_binding,
    resolve_source_physical_identity,
)
from dpone.runtime.etl.portable_scope_preflight import (
    bind_cached_portable_scope_contract,
    portable_scope_column,
    portable_scope_column_contract,
    prepare_portable_scope_binding,
)
from dpone.runtime.governance.mssql_hook_replay_policy import bind_replay_safe_mssql_hook_graph
from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState
from dpone.runtime.state.mssql_route_preflight import resolve_atomic_mssql_target


class _ParentMssqlLaneSourceAuthority:
    """Canonicalize route aliases once before any exact catalog lookup."""

    def __init__(self, source: Any) -> None:
        self._source = source
        self._identities: dict[tuple[Any, ...], Any] = {}

    def bind(self, load_config: Any) -> tuple[Any, Any]:
        """Return canonical coordinates plus their signed physical identity."""

        key = _source_route_key(load_config)
        identity = self._identities.get(key)
        if identity is None:
            probe = replace(load_config)
            identity = resolve_source_physical_identity(self._source, probe)
        relation_authority = identity.version in {2, 3} and all(
            str(value or "").strip() for value in (identity.schema, identity.relation)
        )
        canonical = replace(
            load_config,
            source_schema=identity.schema if relation_authority else load_config.source_schema,
            source_table=identity.relation if relation_authority else load_config.source_table,
        )
        self._identities[key] = identity
        self._identities[_source_route_key(canonical)] = identity
        return canonical, identity


class _ParentMssqlLaneAuthority:
    """Serialize state calls and independently validate child route identity."""

    def __init__(
        self,
        state_storage: Any,
        *,
        source: Any,
        sink: Any,
        run_context: Any,
        dag_id: str | None,
        fresh_session_factory: Any | None = None,
    ) -> None:
        self._state = MssqlGenericTransactionState.from_state_storage(
            state_storage,
            fresh_session_factory=fresh_session_factory,
        )
        self.receipt_recovery = MssqlShadowInitialReceiptRecovery(self._state)
        self._scope_binder = _ParentPortableScopeBinder(source=source, sink=sink)
        self._attempt_projector = _ParentMssqlAttemptProjector(
            source=source,
            sink=sink,
            run_context=run_context,
            dag_id=dag_id,
        )

    def renew_operation_lease(self, operation: Any, expiry: Any) -> bool:
        return self._state.renew_operation_lease(
            operation,
            lease_expires_at_utc=expiry,
        )

    def prepare_dispatch(self, dispatch: ProcessLaneDispatch) -> ProcessLaneDispatch:
        """Warm parent catalog authority before the child IPC timeout starts."""

        canonical_config = self._attempt_projector.bind_source_authority(dispatch.load_config)
        bound_config = self._scope_binder.bind(canonical_config)
        self._attempt_projector.project(bound_config)
        return replace(dispatch, load_config=bound_config)

    def validate_operation_binding(self, dispatch: ProcessLaneDispatch, operation: Any) -> bool:
        """Require parent catalogs to reproduce the complete child identity."""

        return _validate_operation_binding(
            dispatch,
            operation,
            bind_scope=self._scope_binder.bind,
            project_attempt=self._attempt_projector.project,
        )


class _ParentPortableScopeBinder:
    """Bind every chunk AST to one parent-observed catalog contract per route."""

    def __init__(self, *, source: Any, sink: Any) -> None:
        self._source = source
        self._sink = sink
        self._contracts: dict[tuple[Any, ...], Any] = {}

    def bind(self, load_config: Any) -> Any:
        """Perform one catalog read, then bind later chunk ASTs without I/O."""

        column = portable_scope_column(load_config)
        if column is None:
            return load_config
        key = (*lane_route_coordinates(load_config), column)
        contract = self._contracts.get(key)
        if contract is None:
            prepared = prepare_portable_scope_binding(
                load_config,
                source=self._source,
                sink=self._sink,
            )
            self._contracts[key] = portable_scope_column_contract(prepared)
            return prepared
        return bind_cached_portable_scope_contract(load_config, contract)


@dataclass(frozen=True, slots=True)
class _ParentExpectedAttempt:
    """Attempt fields independently derived from parent-owned connectors."""

    invocation: Any
    target_identity: bytes
    route_fingerprint: bytes
    target_database: str
    target_schema: str
    target_table: str
    strategy: str


class _ParentMssqlAttemptProjector:
    """Resolve immutable route authority once and re-hash every dispatched config."""

    def __init__(
        self,
        *,
        source: Any,
        sink: Any,
        run_context: Any,
        dag_id: str | None,
        target_resolver: Callable[..., Any] = resolve_atomic_mssql_target,
    ) -> None:
        self._source = source
        self._sink = sink
        self._run_context = run_context
        self._dag_id = dag_id
        self._target_resolver = target_resolver
        self._source_authority = _ParentMssqlLaneSourceAuthority(source)
        self._authorities: dict[tuple[Any, ...], tuple[Any, Any]] = {}

    def bind_source_authority(self, load_config: Any) -> Any:
        """Canonicalize signed source aliases before exact catalog access."""

        return self._source_authority.bind(load_config)[0]

    def project(self, load_config: Any) -> _ParentExpectedAttempt:
        """Project the exact child attempt without trusting child-supplied identity."""

        load_config, source_identity = self._source_authority.bind(load_config)
        load_config = bind_replay_safe_mssql_hook_graph(load_config)
        shadow = require_shadow_append_authority(load_config)
        identity_coordinates = _identity_coordinates(load_config, shadow)
        key = (*lane_route_coordinates(load_config), *identity_coordinates)
        authority = self._authorities.get(key)
        if authority is None:
            physical = self._target_resolver(
                self._sink.connector,
                self._sink.state_storage,
                database=identity_coordinates[0],
                schema=identity_coordinates[1],
                table=identity_coordinates[2],
            )
            authority = (physical, source_identity)
            self._authorities[key] = authority
        physical, source_identity = authority
        invocation = invocation_identity(self._run_context, load_config, dag_id=self._dag_id)
        request_coordinates = (
            _configured_target_coordinates(load_config)
            if shadow is not None
            else (
                str(physical.database_name),
                str(physical.schema_name),
                str(physical.table_name),
            )
        )
        return _ParentExpectedAttempt(
            invocation=invocation,
            target_identity=physical.digest,
            route_fingerprint=invocation_route_fingerprint(
                load_config,
                target_identity=physical.digest,
                source_identity=source_identity,
            ),
            target_database=request_coordinates[0],
            target_schema=request_coordinates[1],
            target_table=request_coordinates[2],
            strategy=load_config.load_strategy.value,
        )


def _validate_operation_binding(
    dispatch: ProcessLaneDispatch,
    operation: Any,
    *,
    bind_scope: Callable[[Any], Any],
    project_attempt: Callable[[Any], _ParentExpectedAttempt],
) -> bool:
    """Fail closed unless parent catalogs reproduce the full child identity."""

    if not is_mssql_transaction_operation(operation):
        return False
    try:
        attempt = operation.attempt
        request = attempt.request
        load_config = dispatch.load_config
        options = getattr(load_config, "options", {}) or {}
        backfill = options.get("backfill") if isinstance(options, Mapping) else None
        chunk_context = backfill.get("chunk_context") if isinstance(backfill, Mapping) else None
        if not isinstance(chunk_context, Mapping):
            return False
        bound_config = bind_scope(load_config)
        expected_attempt = project_attempt(bound_config)
        expected_operation = operation_request(bound_config, expected_attempt.invocation)
        binding = dispatch.binding
        exact_chunk = (
            chunk_context.get("run_key") == binding.run_key and chunk_context.get("index") == binding.chunk_index
        )
        exact_attempt = (
            request.invocation == expected_attempt.invocation
            and request.target_identity == expected_attempt.target_identity
            and request.route_fingerprint == expected_attempt.route_fingerprint
            and request.target_database == expected_attempt.target_database
            and request.target_schema == expected_attempt.target_schema
            and request.target_table == expected_attempt.target_table
            and request.strategy == expected_attempt.strategy
        )
        exact_operation = (
            exact_attempt
            and expected_operation.operation_key(request) == operation.operation_key
            and expected_operation.scope_hash == operation.scope_hash
            and expected_operation.owner_digest == operation.owner_digest
        )
        return exact_chunk and exact_operation and exact_attempt
    except Exception:
        return False


def lane_route_coordinates(load_config: Any) -> tuple[Any, ...]:
    """Freeze connections and relations while allowing chunk predicates to vary."""

    return (
        load_config.source_conn_id,
        getattr(load_config, "source_database", None),
        load_config.source_schema,
        load_config.source_table,
        load_config.target_conn_id,
        getattr(load_config, "target_database", None),
        load_config.target_schema,
        load_config.target_table,
    )


def _identity_coordinates(load_config: Any, shadow: Any) -> tuple[str, str, str]:
    if shadow is None:
        return _configured_target_coordinates(load_config)
    return shadow.live_database, shadow.live_schema, shadow.live_table


def _configured_target_coordinates(load_config: Any) -> tuple[str, str, str]:
    return (
        str(getattr(load_config, "target_database", "") or ""),
        str(load_config.target_schema),
        str(load_config.target_table),
    )


def _source_route_key(load_config: Any) -> tuple[Any, ...]:
    binding = require_source_physical_identity_binding(load_config)
    return (
        load_config.source_conn_id,
        getattr(load_config, "source_database", None),
        load_config.source_schema,
        load_config.source_table,
        *binding,
    )


__all__ = ["lane_route_coordinates"]
