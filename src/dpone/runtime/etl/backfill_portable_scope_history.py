"""Fail-closed migration proof for legacy portable-scope campaigns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.backfill.execution_policy import lease_expires_at
from dpone.backfill.portable_scope_runtime import is_postgres_mssql_backfill_route
from dpone.backfill.runtime_execution import BackfillChunkExecutor
from dpone.backfill.state import CHUNK_STATUS_SUCCESS, BackfillLedger
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
from dpone.runtime.governance.mssql_hook_replay_policy import bind_replay_safe_mssql_hook_graph
from dpone.runtime.state.mssql_route_preflight import MssqlSessionIdentity
from dpone.runtime.state.mssql_target_identity import (
    resolve_mssql_physical_target_identity,
)

if TYPE_CHECKING:
    from dpone.backfill.portable_scope_campaign import CampaignPortableScopeBinding

_PROOF_OWNER = "dpone-backfill-portable-scope-history-proof"
_PROOF_LOAD_ID = "dpone-backfill-portable-scope-history-proof"


class BackfillPortableScopeHistoryError(RuntimeError):
    """Legacy execution history cannot authorize a new column contract."""


@dataclass(frozen=True, slots=True)
class BackfillPortableScopeHistoryCandidate:
    """Exact attempt and complete candidate scope set for one planned campaign."""

    attempt: Any
    scope_hashes: frozenset[bytes]
    chunk_scope_hashes: tuple[tuple[int, bytes], ...]


@dataclass(frozen=True, slots=True)
class MssqlBackfillPortableScopeHistoryProof:
    """Compare immutable MSSQL history with a newly certified legacy binding."""

    state: Any
    source: Any
    sink: Any
    state_storage: Any
    run_context: Any
    dag_id: str | None
    target_resolver: Any = None

    def __call__(
        self,
        *,
        load_config: Any,
        ledger: BackfillLedger,
        chunks: tuple[Any, ...],
        campaign_binding: CampaignPortableScopeBinding,
    ) -> None:
        """Authorize only a legacy binding compatible with every old operation."""

        if (
            ledger.portable_scope_column_contract is not None
            or campaign_binding.identity_bound
            or not is_postgres_mssql_backfill_route(load_config)
        ):
            return
        candidate = self.build_candidate(
            load_config=load_config,
            ledger=ledger,
            chunks=chunks,
            campaign_binding=campaign_binding,
        )
        history = self.state.operation_history(candidate.attempt)
        if history is None or not history.scope_hashes:
            if _ledger_has_execution_evidence(ledger):
                raise BackfillPortableScopeHistoryError("backfill.portable_scope_history.operation_history_missing")
            return
        if history.attempt_key != candidate.attempt.attempt_key:
            raise BackfillPortableScopeHistoryError("backfill.portable_scope_history.attempt_identity_mismatch")
        if not history.scope_hashes.issubset(candidate.scope_hashes):
            raise BackfillPortableScopeHistoryError("backfill.portable_scope_history.operation_scope_mismatch")
        required_receipts = _required_receipt_scopes(ledger, candidate)
        if not required_receipts.issubset(history.receipt_scope_hashes):
            raise BackfillPortableScopeHistoryError("backfill.portable_scope_history.committed_receipt_missing")

    def build_candidate(
        self,
        *,
        load_config: Any,
        ledger: BackfillLedger,
        chunks: tuple[Any, ...],
        campaign_binding: CampaignPortableScopeBinding,
    ) -> BackfillPortableScopeHistoryCandidate:
        """Build scopes through the same production chunk-config constructor."""

        _require_exact_planned_chunks(ledger, chunks)
        if ledger.plan_hash is None:
            raise BackfillPortableScopeHistoryError("backfill.portable_scope_history.plan_hash_missing")
        expiry = lease_expires_at(load_config)
        if not chunks:
            raise BackfillPortableScopeHistoryError("backfill.portable_scope_history.candidate_plan_empty")
        first = self._chunk_config(
            load_config,
            chunks[0],
            ledger=ledger,
            expiry=expiry,
            campaign_binding=campaign_binding,
        )
        invocation = invocation_identity(self.run_context, first, dag_id=self.dag_id)
        chunk_scopes = [(chunks[0].index, operation_request(first, invocation).scope_hash)]
        for chunk in chunks[1:]:
            chunk_scopes.append(
                (
                    chunk.index,
                    operation_request(
                        self._chunk_config(
                            load_config,
                            chunk,
                            ledger=ledger,
                            expiry=expiry,
                            campaign_binding=campaign_binding,
                        ),
                        invocation,
                    ).scope_hash,
                )
            )
        scope_hashes = {scope for _index, scope in chunk_scopes}
        if len(scope_hashes) != len(chunks):
            raise BackfillPortableScopeHistoryError("backfill.portable_scope_history.candidate_scope_collision")
        physical = self._resolve_target(first)
        source_identity = resolve_source_physical_identity(self.source, first)
        attempt = build_mssql_attempt_request(
            first,
            invocation=invocation,
            target_identity=physical.digest,
            source_identity=source_identity,
            load_id=_PROOF_LOAD_ID,
            request_coordinates=attempt_target_coordinates(
                first,
                physical_target=physical,
            ),
        )
        return BackfillPortableScopeHistoryCandidate(
            attempt,
            frozenset(scope_hashes),
            tuple(chunk_scopes),
        )

    @staticmethod
    def _chunk_config(
        load_config: Any,
        chunk: Any,
        *,
        ledger: BackfillLedger,
        expiry: Any,
        campaign_binding: CampaignPortableScopeBinding,
    ) -> Any:
        return bind_replay_safe_mssql_hook_graph(
            BackfillChunkExecutor.build_chunk_load_config(
                load_config,
                chunk,
                run_key=ledger.run_key,
                plan_hash=ledger.plan_hash,
                owner=_PROOF_OWNER,
                lease_expires_at_utc=expiry,
                portable_scope_campaign=campaign_binding,
            )
        )

    def _resolve_target(self, load_config: Any) -> Any:
        require_binding = getattr(self.state_storage, "require_database_authority_binding", None)
        if not callable(require_binding):
            raise BackfillPortableScopeHistoryError(
                "backfill.portable_scope_history.database_authority_binding_required"
            )
        require_binding()
        verify_authority = getattr(self.state_storage, "verify_database_authority", None)
        if not callable(verify_authority):
            raise BackfillPortableScopeHistoryError(
                "backfill.portable_scope_history.database_authority_verifier_required"
            )
        verify_authority(self.sink.connector)
        database, schema, table = live_target_coordinates(load_config)
        resolver = self.target_resolver or resolve_read_only_atomic_target
        return resolver(
            self.sink.connector,
            self.state_storage,
            database=database,
            schema=schema,
            table=table,
        )


def build_mssql_backfill_portable_scope_history_proof(
    *,
    source: Any,
    sink: Any,
    run_context: Any,
    dag_id: str | None,
    state: Any | None = None,
) -> MssqlBackfillPortableScopeHistoryProof:
    """Compose the parent-only proof for single- or multi-worker execution."""

    state_storage = getattr(sink, "state_storage", None)
    if state is None:
        from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState

        state = MssqlGenericTransactionState.from_state_storage(state_storage)
    return MssqlBackfillPortableScopeHistoryProof(
        state=state,
        source=source,
        sink=sink,
        state_storage=state_storage,
        run_context=run_context,
        dag_id=dag_id,
    )


def resolve_read_only_atomic_target(
    target_connector: Any,
    state_storage: Any,
    *,
    database: str,
    schema: str,
    table: str,
) -> Any:
    """Resolve the child-path physical identity without permission-probe DML."""

    state_connector = getattr(state_storage, "connector", None)
    if getattr(state_storage, "atomicity", None) != "target_atomic" or state_connector is None:
        raise BackfillPortableScopeHistoryError("backfill.portable_scope_history.target_atomic_state_required")
    target_session = MssqlSessionIdentity.read(target_connector)
    state_session = MssqlSessionIdentity.read(state_connector)
    if target_session.topology != state_session.topology:
        raise BackfillPortableScopeHistoryError("backfill.portable_scope_history.instance_or_replica_mismatch")
    if target_session.principal != state_session.principal:
        raise BackfillPortableScopeHistoryError("backfill.portable_scope_history.effective_principal_mismatch")
    return resolve_mssql_physical_target_identity(
        target_connector,
        session=target_session,
        database=database,
        schema=schema,
        table=table,
    )


def _require_exact_planned_chunks(ledger: BackfillLedger, chunks: tuple[Any, ...]) -> None:
    planned = tuple((item.index, item.start, item.end, item.idempotency_key) for item in chunks)
    persisted = tuple((item.index, item.start, item.end, item.idempotency_key) for item in ledger.chunks)
    if planned != persisted:
        raise BackfillPortableScopeHistoryError("backfill.portable_scope_history.ledger_plan_mismatch")


def _ledger_has_execution_evidence(ledger: BackfillLedger) -> bool:
    if ledger.publication is not None or ledger.xmin_handoff is not None:
        return True
    return any(
        record.status == CHUNK_STATUS_SUCCESS or record.rows_loaded > 0 or bool(record.execution_evidence)
        for record in ledger.chunks
    )


def _required_receipt_scopes(
    ledger: BackfillLedger,
    candidate: BackfillPortableScopeHistoryCandidate,
) -> frozenset[bytes]:
    scopes_by_index = dict(candidate.chunk_scope_hashes)
    if ledger.publication is not None or ledger.xmin_handoff is not None:
        return candidate.scope_hashes
    return frozenset(
        scopes_by_index[record.index]
        for record in ledger.chunks
        if record.status == CHUNK_STATUS_SUCCESS or record.rows_loaded > 0 or bool(record.execution_evidence)
    )


__all__ = [
    "BackfillPortableScopeHistoryCandidate",
    "BackfillPortableScopeHistoryError",
    "MssqlBackfillPortableScopeHistoryProof",
    "build_mssql_backfill_portable_scope_history_proof",
    "resolve_read_only_atomic_target",
]
