"""Parent-owned receipt and replay authority for spawned MSSQL lanes."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from dpone.backfill.mssql_receipt_recovery import MssqlShadowInitialReceiptRecovery
from dpone.backfill.process_lane_contracts import ProcessLaneDispatch
from dpone.backfill.process_lane_operation import (
    dispatch_portable_binding,
    is_mssql_transaction_operation,
    portable_binding_is_exact,
)
from dpone.runtime.etl.backfill_portable_scope_history import resolve_read_only_atomic_target
from dpone.runtime.etl.mssql_process_lane_authority import lane_route_coordinates
from dpone.runtime.etl.mssql_transaction_identity import (
    build_mssql_attempt_request,
    invocation_identity,
    operation_request,
    resolve_source_physical_identity,
)
from dpone.runtime.etl.mssql_transaction_request import (
    attempt_target_coordinates,
    live_target_coordinates,
)
from dpone.runtime.governance.mssql_hook_replay_policy import (
    MSSQL_HOOK_GRAPH_SHA256_OPTION,
    require_replay_safe_mssql_hooks,
)
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from dpone.runtime.sinks.mssql_receipt_projection import load_result_from_mssql_receipt
from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState

_LOG = logging.getLogger(__name__)


class ParentMssqlLaneReceiptAuthority:
    """Recompute child identity and validate replay receipts in the parent."""

    def __init__(
        self,
        state_storage: Any,
        *,
        source: Any,
        sink: Any,
        run_context: Any,
        dag_id: str | None,
        target_resolver: Any = resolve_read_only_atomic_target,
        source_identity_resolver: Any = resolve_source_physical_identity,
        fresh_session_factory: Any | None = None,
    ) -> None:
        self._state = MssqlGenericTransactionState.from_state_storage(
            state_storage,
            fresh_session_factory=fresh_session_factory,
        )
        self._state_storage = state_storage
        self._source = source
        self._sink = sink
        self._run_context = run_context
        self._dag_id = dag_id
        self._target_resolver = target_resolver
        self._source_identity_resolver = source_identity_resolver
        self._route_authorities: dict[tuple[Any, ...], tuple[Any, Any]] = {}
        self.receipt_recovery = MssqlShadowInitialReceiptRecovery(self._state)

    def prepare_dispatch(self, dispatch: ProcessLaneDispatch) -> ProcessLaneDispatch:
        """Warm the exact receipt authority before the child IPC budget starts."""

        portable_binding = dispatch_portable_binding(dispatch)
        if not portable_binding_is_exact(dispatch, portable_binding) or not _chunk_context_is_exact(dispatch):
            raise RuntimeError("mssql_transaction.parent_receipt_dispatch_binding_invalid")
        self._route_authority(identity_config(dispatch.load_config))
        return dispatch

    def renew_operation_lease(self, operation: Any, expiry: Any) -> bool:
        """Renew only the exact operation descriptor accepted by SQL state."""

        return self._state.renew_operation_lease(
            operation,
            lease_expires_at_utc=expiry,
        )

    def operation_history(self, attempt_request: Any) -> Any:
        """Expose only the fresh read-only migration query to the orchestrator."""

        return self._state.operation_history(attempt_request)

    def issue_receipt_probe(
        self,
        dispatch: ProcessLaneDispatch,
        load_id: str,
        portable_scope_binding: Any | None,
    ) -> tuple[Any, Any] | None:
        """Issue parent-derived route identity before any child receipt read."""

        return self._expected_requests(
            dispatch,
            load_id=load_id,
            portable_scope_binding=portable_scope_binding,
        )

    def validate_operation_binding(
        self,
        dispatch: ProcessLaneDispatch,
        operation: Any,
        portable_scope_binding: Any | None,
    ) -> bool:
        """Derive the complete child operation identity from parent authority."""

        if not is_mssql_transaction_operation(operation):
            _LOG.warning("event=dpone.backfill_process_operation_binding_rejected reason=operation_type")
            return False
        attempt = operation.attempt
        try:
            expected = self._expected_requests(
                dispatch,
                load_id=attempt.request.load_id,
                portable_scope_binding=portable_scope_binding,
            )
        except (AttributeError, TypeError, ValueError, RuntimeError):
            _LOG.warning("event=dpone.backfill_process_operation_binding_rejected reason=recompute")
            return False
        if expected is None:
            return False
        expected_attempt, expected_operation = expected
        exact_operation = (
            attempt.request == expected_attempt
            and attempt.is_current_generation is True
            and expected_operation.operation_key(attempt) == operation.operation_key
            and expected_operation.scope_hash == operation.scope_hash
            and expected_operation.owner_digest == operation.owner_digest
            and expected_operation.lease_expires_at_utc == operation.lease_expires_at_utc
        )
        if not exact_operation:
            _LOG.warning(
                "event=dpone.backfill_process_operation_binding_rejected exact_operation=%s",
                exact_operation,
            )
        return exact_operation

    def validate_replay_result(
        self,
        dispatch: ProcessLaneDispatch,
        attempt_request: Any,
        requested_operation: Any,
        result: Mapping[str, Any],
    ) -> bool:
        """Fresh-probe parent state and bind ledger metrics to its exact receipt."""

        try:
            expected = self._expected_requests(
                dispatch,
                load_id=attempt_request.load_id,
                portable_scope_binding=dispatch_portable_binding(dispatch),
            )
            if expected != (attempt_request, requested_operation):
                return False
            admission = self._state.replay_if_committed(attempt_request, requested_operation)
        except (AttributeError, TypeError, ValueError, RuntimeError):
            return False
        receipt = getattr(admission, "replay_receipt", None)
        if receipt is None:
            return False
        return _result_matches_receipt(result, receipt, load_id=attempt_request.load_id)

    def _expected_requests(
        self,
        dispatch: ProcessLaneDispatch,
        *,
        load_id: str,
        portable_scope_binding: Any | None,
    ) -> tuple[Any, Any] | None:
        if not portable_binding_is_exact(dispatch, portable_scope_binding) or not _chunk_context_is_exact(dispatch):
            return None
        load_config = identity_config(dispatch.load_config)
        invocation = invocation_identity(self._run_context, load_config, dag_id=self._dag_id)
        physical_target, source_identity = self._route_authority(load_config)
        coordinates = attempt_target_coordinates(load_config, physical_target=physical_target)
        attempt = build_mssql_attempt_request(
            load_config,
            invocation=invocation,
            target_identity=physical_target.digest,
            source_identity=source_identity,
            load_id=load_id,
            request_coordinates=coordinates,
        )
        return attempt, operation_request(load_config, invocation)

    def _route_authority(self, load_config: Any) -> tuple[Any, Any]:
        """Resolve physical identities once and freeze them for this lane route."""

        key = lane_route_coordinates(load_config)
        cached = self._route_authorities.get(key)
        if cached is not None:
            return cached
        require_binding = getattr(self._state_storage, "require_database_authority_binding", None)
        verify_authority = getattr(self._state_storage, "verify_database_authority", None)
        if not callable(require_binding) or not callable(verify_authority):
            raise RuntimeError("mssql_transaction.parent_database_authority_required")
        require_binding()
        verify_authority(self._sink.connector)
        database, schema, table = live_target_coordinates(load_config)
        physical_target = self._target_resolver(
            self._sink.connector,
            self._state_storage,
            database=database,
            schema=schema,
            table=table,
        )
        source_identity = self._source_identity_resolver(self._source, load_config)
        resolved = (physical_target, source_identity)
        self._route_authorities[key] = resolved
        return resolved


def identity_config(load_config: Any) -> Any:
    """Mirror the pure child hook binding before route hashing."""

    contract = require_replay_safe_mssql_hooks(load_config)
    options = dict(getattr(load_config, "options", {}) or {})
    options[MSSQL_HOOK_GRAPH_SHA256_OPTION] = contract.sha256
    return replace(load_config, options=options)


def _chunk_context_is_exact(dispatch: ProcessLaneDispatch) -> bool:
    options = getattr(dispatch.load_config, "options", {}) or {}
    backfill = options.get("backfill") if isinstance(options, Mapping) else None
    context = backfill.get("chunk_context") if isinstance(backfill, Mapping) else None
    return bool(
        isinstance(context, Mapping)
        and context.get("run_key") == dispatch.binding.run_key
        and context.get("index") == dispatch.binding.chunk_index
    )


def _result_matches_receipt(result: Mapping[str, Any], receipt: Any, *, load_id: str) -> bool:
    replay = load_result_from_mssql_receipt(receipt, outcome=AtomicCommitOutcome.REPLAY_SUPPRESSED)
    loaded_rows = (replay.replaced_rows or 0) if replay.replaced_rows else replay.inserted_rows
    expected: dict[str, Any] = {
        "extracted_rows": replay.staging_rows or 0,
        "loaded_rows": loaded_rows,
        "inserted_rows": replay.inserted_rows,
        "updated_rows": replay.updated_rows,
        "total_rows": replay.total_rows,
        "final_rows": replay.total_rows,
        "staging_rows": replay.staging_rows or 0,
        "soft_deleted_rows": replay.soft_deleted_rows or 0,
        "reactivated_rows": replay.reactivated_rows or 0,
        "unchanged_rows": replay.unchanged_rows or 0,
        "hard_deleted_rows": replay.hard_deleted_rows or 0,
        "active_rows": replay.active_rows,
        "commit_receipt_id": replay.commit_receipt_id,
        "commit_outcome": AtomicCommitOutcome.REPLAY_SUPPRESSED.value,
        "replaced_rows": replay.replaced_rows or 0,
        "deleted_lookback_rows": replay.deleted_lookback_rows or 0,
        "status": "success",
        "errors": [],
    }
    for key, value in expected.items():
        actual = result.get(key)
        actual = getattr(actual, "value", actual) if key == "commit_outcome" else actual
        if actual != value:
            return False
    metrics = result.get("reconciliation_metrics")
    return (
        str(result.get("load_id") or "") == load_id
        and isinstance(metrics, Mapping)
        and metrics.get("mssql_transaction_replay_suppressed") is True
    )


__all__ = ["ParentMssqlLaneReceiptAuthority", "identity_config"]
