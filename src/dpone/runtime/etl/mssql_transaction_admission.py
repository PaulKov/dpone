"""Pre-source admission for receipt-backed generic SQL Server loads."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from dataclasses import replace
from typing import Any

from dpone.contracts.mssql_source_checkpoint import (
    MssqlTransactionCheckpointMode,
    normalize_mssql_transaction_checkpoint_mode,
    require_generic_mssql_checkpoint_safety,
    require_snapshot_envelope_mssql_checkpoint_safety,
)
from dpone.contracts.mssql_transaction_governance import (
    MssqlAttemptRequest,
    MssqlOperationRequest,
    MssqlTransactionAdmission,
)
from dpone.contracts.target_max_incremental_cursor import raise_unsafe_target_max_mssql_cursor
from dpone.runtime.etl.mssql_operation_lease import (
    LEASE_OPTION,
    MssqlOperationLeaseController,
    MssqlOperationLeaseControllerError,
    MssqlOperationLeaseHeartbeat,
)
from dpone.runtime.etl.mssql_schema_preplan import (
    MSSQL_SCHEMA_PREPLAN_OPTION,
    MssqlSchemaPreplanner,
)
from dpone.runtime.etl.mssql_transaction_identity import (
    derive_mssql_runtime_requests as _locally_derived_requests,
)
from dpone.runtime.etl.mssql_transaction_identity import (
    invocation_identity,
    require_source_physical_identity_binding,
)
from dpone.runtime.etl.portable_scope_preflight import prepare_portable_scope_binding
from dpone.runtime.governance.mssql_hook_replay_policy import (
    bind_replay_safe_mssql_hook_graph,
)
from dpone.runtime.internal_query_capability import (
    InternalQueryCapabilityDecision,
    InternalQueryCapabilityDiagnostic,
)
from dpone.runtime.mssql_spool_route import bind_mssql_character_spool_preflight
from dpone.runtime.sink_dialect import is_mssql_dialect
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from dpone.runtime.sinks.mssql_receipt_projection import load_result_from_mssql_receipt
from dpone.runtime.sinks.mssql_transaction_requirement import (
    MSSQL_GENERIC_TRANSACTION_CAPABILITY,
    MSSQL_TRANSACTION_ADMISSION_OPTION,
    is_snapshot_envelope_route,
    require_generic_transaction_state,
)
from dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary import (
    MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION,
)
from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState
from dpone.runtime.state.mssql_route_preflight import resolve_atomic_mssql_target

ADMISSION_OPTION = MSSQL_TRANSACTION_ADMISSION_OPTION


class MssqlTransactionAdmissionService:
    """Resolve physical authority, allocate fences, and freeze admission."""

    def __init__(
        self,
        *,
        target_resolver: Any = resolve_atomic_mssql_target,
        state_factory: Any = None,
        operation_lease_factory: Any = MssqlOperationLeaseHeartbeat,
        operation_scope_refresher: Callable[[Any], Any] | None = None,
        start_operation_lease_on_admission: bool = False,
        operation_registrar: Callable[..., None] | None = None,
        preplan_verifier: Callable[..., Any] | None = None,
        composition_fence: Any | None = None,
        fence_connector: Any | None = None,
    ) -> None:
        self._target_resolver = target_resolver
        self._state_factory = state_factory or MssqlGenericTransactionState.from_state_storage
        self._operation_lease_factory = operation_lease_factory
        self._operation_scope_refresher = operation_scope_refresher
        self._start_operation_lease_on_admission = start_operation_lease_on_admission
        self._operation_registrar = operation_registrar
        self._preplan_verifier = preplan_verifier
        self._composition_fence = composition_fence
        self._fence_connector = fence_connector

    def prepare(
        self,
        load_config: Any,
        *,
        source: Any,
        sink: Any,
        run_context: Any,
        load_record: Any,
        dag_id: str | None,
    ) -> Any:
        if not self._is_mssql_sink(sink):
            return load_config
        self._require_capability(sink)
        load_config = bind_mssql_character_spool_preflight(load_config, source=source)
        load_config = bind_replay_safe_mssql_hook_graph(load_config)
        snapshot_envelope_route = is_snapshot_envelope_route(load_config)
        _require_source_checkpoint_mode(
            source,
            load_config,
            snapshot_envelope_route=snapshot_envelope_route,
        )
        if snapshot_envelope_route:
            return load_config
        _force_receiptable_source_artifact(source)
        state_storage = require_generic_transaction_state(load_config, getattr(sink, "state_storage", None))
        _require_portable_database_authority_binding(load_config, state_storage)
        require_source_physical_identity_binding(load_config)
        if _nested_normalization_enabled(load_config):
            raise RuntimeError("mssql_transaction.nested_package_atomicity_unsupported")
        target_database = str(getattr(load_config, "target_database", "") or "").strip()
        if not target_database:
            raise RuntimeError("mssql_transaction.target_database_required")
        # Parent-issued campaign bindings validate without catalog I/O. Direct
        # first attempts certify their catalogs here, before invocation/state
        # admission, because a missing binding cannot address an exact receipt.
        load_config = prepare_portable_scope_binding(load_config, source=source, sink=sink)
        state = self._state_factory(state_storage)
        invocation = invocation_identity(run_context, load_config, dag_id=dag_id)
        load_id = str(getattr(load_record, "load_id", "") or "")
        parent_authority = _parent_receipt_authority(
            self._operation_lease_factory,
            load_config,
            load_id=load_id,
        )
        if parent_authority is None:
            if self._operation_scope_refresher is not None:
                load_config = self._operation_scope_refresher(load_config)
                invocation = invocation_identity(run_context, load_config, dag_id=dag_id)
            attempt, requested_operation = _locally_derived_requests(
                load_config,
                source=source,
                sink=sink,
                state_storage=state_storage,
                target_resolver=self._target_resolver,
                invocation=invocation,
                load_id=load_id,
            )
        else:
            attempt, requested_operation = parent_authority
        state.preflight(sink.connector)
        replay = _probe_committed_before_source_catalog(state, attempt, requested_operation)
        if replay is not None:
            options = dict(getattr(load_config, "options", {}) or {})
            options[ADMISSION_OPTION] = replay
            return replace(load_config, options=options)

        if parent_authority is not None:
            local_attempt, local_operation = _locally_derived_requests(
                load_config,
                source=source,
                sink=sink,
                state_storage=state_storage,
                target_resolver=self._target_resolver,
                invocation=invocation,
                load_id=load_id,
            )
            if (local_attempt, local_operation) != parent_authority:
                raise RuntimeError("mssql_transaction.parent_route_authority_mismatch")
        admission = state.admit(attempt, requested_operation)
        options = dict(getattr(load_config, "options", {}) or {})
        options[ADMISSION_OPTION] = admission
        started_operation_lease: MssqlOperationLeaseController | None = None
        if admission.operation is not None:
            if admission.operation.lease_expires_at_utc is not None:
                operation_lease = self._operation_lease_factory(state, admission)
                bind_load_config = getattr(operation_lease, "bind_load_config", None)
                if callable(bind_load_config):
                    bind_load_config(load_config)
                options[LEASE_OPTION] = operation_lease
                if self._start_operation_lease_on_admission:
                    if not isinstance(operation_lease, MssqlOperationLeaseController):
                        raise MssqlOperationLeaseControllerError()
                    operation_lease.start()
                    started_operation_lease = operation_lease
            prepared_boundary = None
            try:
                prepared_boundary = _prepare_post_admission_source_boundary(
                    source,
                    load_config,
                )
                if prepared_boundary is not None:
                    options[MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION] = prepared_boundary
                    load_config = replace(load_config, options=options)
                options[MSSQL_SCHEMA_PREPLAN_OPTION] = MssqlSchemaPreplanner().plan(
                    load_config,
                    source=source,
                    sink=sink,
                    admission=admission,
                )
                if self._operation_registrar is not None:
                    digest = options[MSSQL_SCHEMA_PREPLAN_OPTION].target_mutation_plan.digest
                    if self._preplan_verifier is None:
                        self._operation_registrar(admission, digest)
                    else:
                        reference = self._preplan_verifier(
                            admission,
                            source=source,
                            prepared_boundary=prepared_boundary,
                            submitted_mutation_sha256=digest,
                        )
                        self._operation_registrar(admission, digest, preplan_reference=reference)
            except BaseException as primary:
                if prepared_boundary is not None:
                    prepared_boundary.abort_preserving(primary)
                if started_operation_lease is not None:
                    with suppress(Exception):
                        started_operation_lease.stop()
                raise
        return replace(load_config, options=options)

    def replay_result(self, load_config: Any) -> Any | None:
        """Project exact receipt metrics before any source or payload work."""

        admission = (getattr(load_config, "options", {}) or {}).get(ADMISSION_OPTION)
        if not isinstance(admission, MssqlTransactionAdmission) or admission.replay_receipt is None:
            return None
        if self._composition_fence is not None:
            connector = self._fence_connector
            if connector is None:
                raise RuntimeError("mssql_transaction.composition_fence_connector_required")
            try:
                connector.begin()
                self._composition_fence.require_current(connector, receipt=admission.replay_receipt)
            finally:
                connector.rollback()
        return load_result_from_mssql_receipt(
            admission.replay_receipt,
            outcome=AtomicCommitOutcome.REPLAY_SUPPRESSED,
        )

    @staticmethod
    def _is_mssql_sink(sink: Any) -> bool:
        dialect = getattr(sink, "target_dialect", None)
        return bool(callable(dialect) and is_mssql_dialect(dialect()))

    @staticmethod
    def _require_capability(sink: Any) -> None:
        capability = getattr(sink, "mssql_transaction_governance_capability", None)
        if not callable(capability) or capability() != MSSQL_GENERIC_TRANSACTION_CAPABILITY:
            raise RuntimeError("mssql_transaction.sink_capability_required")


def _nested_normalization_enabled(load_config: Any) -> bool:
    options = getattr(load_config, "options", {}) or {}
    normalization = options.get("normalization")
    return bool(isinstance(normalization, dict) and normalization.get("enabled"))


def _require_portable_database_authority_binding(load_config: Any, state_storage: Any) -> None:
    """Fail before catalog I/O when a direct portable route lacks signed pins."""

    if getattr(load_config, "portable_scope", None) is None:
        return
    require_binding = getattr(state_storage, "require_database_authority_binding", None)
    if not callable(require_binding):
        raise RuntimeError("mssql_transaction.database_authority_binding_required")
    require_binding()


def _require_source_checkpoint_mode(
    source: Any,
    load_config: Any,
    *,
    snapshot_envelope_route: bool,
) -> None:
    resolver = getattr(source, "mssql_transaction_checkpoint_mode", None)
    mode = normalize_mssql_transaction_checkpoint_mode(
        resolver(load_config) if callable(resolver) else MssqlTransactionCheckpointMode.UNKNOWN
    )
    if mode is MssqlTransactionCheckpointMode.TARGET_DERIVED_SINGLE_COLUMN_UNSAFE:
        source_type = getattr(source, "target_max_cursor_source_type", None)
        if source_type is not None:
            raise_unsafe_target_max_mssql_cursor(source_type)
    if snapshot_envelope_route:
        require_snapshot_envelope_mssql_checkpoint_safety(mode)
        return
    require_generic_mssql_checkpoint_safety(mode)


def _force_receiptable_source_artifact(source: Any) -> None:
    """Disable same-session query artifacts until they emit crypto evidence."""

    decision = getattr(source, "internal_query_capability", None)
    if not bool(getattr(decision, "eligible", False)):
        return
    binder = getattr(source, "bind_internal_query_capability", None)
    if not callable(binder):
        raise RuntimeError("mssql_transaction.internal_query_consumed_payload_evidence_unsupported")
    fallback = InternalQueryCapabilityDecision(
        capability=None,
        diagnostic=InternalQueryCapabilityDiagnostic(
            code="DPONE_INTERNAL_QUERY_TRANSACTION_EVIDENCE_FILE_FALLBACK",
            eligible=False,
            source_dialect="mssql",
            target_dialect="mssql",
            message="Receipt-backed MSSQL loads require immutable consumed-payload evidence.",
            action="Use the canonical file or streaming transfer boundary.",
        ),
    )
    binder(fallback)
    if bool(getattr(getattr(source, "internal_query_capability", None), "eligible", False)):
        raise RuntimeError("mssql_transaction.internal_query_fallback_not_applied")


def _prepare_post_admission_source_boundary(
    source: Any,
    load_config: Any,
) -> Any | None:
    """Open a source boundary only after replay suppression admitted work."""

    prepare = getattr(source, "prepare_mssql_source_boundary", None)
    return prepare(load_config) if callable(prepare) else None


def _probe_committed_before_source_catalog(
    state: Any,
    attempt: MssqlAttemptRequest,
    requested_operation: MssqlOperationRequest,
) -> MssqlTransactionAdmission | None:
    """Replay an exact receipt before catalog binding when identity is already frozen."""

    probe = getattr(state, "replay_if_committed", None)
    if not callable(probe):
        return None
    return probe(attempt, requested_operation)


def _parent_receipt_authority(
    operation_lease_factory: Any,
    load_config: Any,
    *,
    load_id: str,
) -> tuple[MssqlAttemptRequest, MssqlOperationRequest] | None:
    authorize = getattr(operation_lease_factory, "authorize_receipt_probe", None)
    if not callable(authorize):
        return None
    authority = authorize(load_config, load_id=load_id)
    if (
        not isinstance(authority, tuple)
        or len(authority) != 2
        or not isinstance(authority[0], MssqlAttemptRequest)
        or not isinstance(authority[1], MssqlOperationRequest)
    ):
        raise RuntimeError("mssql_transaction.parent_receipt_authority_invalid")
    return authority


__all__ = ["ADMISSION_OPTION", "MssqlTransactionAdmissionService"]
