"""Operation-owner fencing and atomic generic MSSQL commit receipts."""

from __future__ import annotations

from contextlib import suppress
from datetime import UTC, datetime  # type: ignore[attr-defined]
from typing import TYPE_CHECKING, Any

from dpone.runtime.state.mssql_generic_operation_sql import claim_operation_params, claim_operation_sql
from dpone.runtime.state.mssql_generic_state_base import MssqlGenericStateBase
from dpone.runtime.state.mssql_generic_transaction_errors import normalize_operation_claim_error
from dpone.runtime.state.mssql_generic_transaction_names import (
    ATTEMPT_TABLE,
    FENCE_TABLE,
    OPERATION_TABLE,
    RECEIPT_TABLE,
)
from dpone.runtime.state.mssql_generic_transaction_rows import binary, operation_from_rows, receipt_from_row

if TYPE_CHECKING:
    from dpone.contracts.mssql_transaction_governance import (
        MssqlGenericCommitReceipt,
        MssqlOperationRequest,
        MssqlPayloadCommitEvidence,
        MssqlReceiptMetrics,
        MssqlSourceLifecycleEvidence,
        MssqlTransactionAttempt,
        MssqlTransactionOperation,
    )


class MssqlOperationClaimOutcomeUnknown(RuntimeError):
    """Operation claim ACK was lost and current ownership is unprovable."""

    code = "mssql_transaction.operation_claim_outcome_unknown"

    def __init__(self) -> None:
        super().__init__(self.code)


class MssqlGenericOperationState(MssqlGenericStateBase):
    """Own operation epochs, target fences, and immutable commit receipts."""

    def claim_or_replay(
        self,
        attempt: MssqlTransactionAttempt,
        request: MssqlOperationRequest,
    ) -> MssqlTransactionOperation | MssqlGenericCommitReceipt:
        """Atomically observe a receipt or claim the current owner epoch."""

        started = False
        commit_attempted = False
        try:
            self.connector.begin()
            started = True
            rows = self.connector.get_records(
                claim_operation_sql(
                    self.operation_table,
                    self.receipt_table,
                    self.fence_table,
                    self.attempt_table,
                ),
                claim_operation_params(attempt, request),
                as_dict=True,
            )
            outcome = self._claim_outcome(attempt, request, rows, connector=self.connector)
            commit_attempted = True
            self.connector.commit_transaction()
            started = False
            return outcome
        except Exception as exc:
            if commit_attempted:
                self.close(self.connector)
                probed = self.probe_claim_outcome_fresh(attempt, request)
                if probed is not None:
                    return probed
                raise MssqlOperationClaimOutcomeUnknown() from exc
            if started:
                with suppress(Exception):
                    self.connector.rollback()
            normalized = normalize_operation_claim_error(exc)
            if normalized is not None:
                raise normalized from exc
            raise

    def probe_claim_outcome_fresh(
        self,
        attempt: MssqlTransactionAttempt,
        request: MssqlOperationRequest,
    ) -> MssqlTransactionOperation | MssqlGenericCommitReceipt | None:
        with self.fresh_sessions.open(self.connector) as session:
            receipt = self.receipt_by_key(request.operation_key(attempt), connector=session)
            if receipt is not None:
                return receipt
            return self.probe_operation(attempt, request, connector=session)

    def _claim_outcome(
        self,
        attempt: MssqlTransactionAttempt,
        request: MssqlOperationRequest,
        rows: Any,
        *,
        connector: Any,
    ) -> MssqlTransactionOperation | MssqlGenericCommitReceipt:
        values = list(rows or ())
        if len(values) != 1:
            raise RuntimeError("mssql_transaction.operation_missing_or_ambiguous")
        if values[0].get("committed_receipt_id") is not None:
            receipt = self.receipt_by_key(request.operation_key(attempt), connector=connector)
            if receipt is None:
                raise RuntimeError("mssql_transaction.receipt_marker_unverified")
            return receipt
        return operation_from_rows(attempt, request, values)

    def probe_operation(
        self,
        attempt: MssqlTransactionAttempt,
        request: MssqlOperationRequest,
        *,
        connector: Any | None = None,
    ) -> MssqlTransactionOperation | None:
        session = connector or self.connector
        rows = session.get_records(
            f"""
            SELECT operation_key, attempt_key, scope_hash, current_epoch,
                   current_owner_digest, lease_expires_at_utc
            FROM {self.operation_table}
            WHERE operation_key = ?
            """,
            (request.operation_key(attempt),),
            as_dict=True,
        )
        if not rows:
            return None
        return operation_from_rows(attempt, request, rows)

    def assert_current(
        self,
        executor: Any,
        operation: MssqlTransactionOperation,
        *,
        require_unexpired_lease: bool = True,
    ) -> None:
        """Hold target/owner locks and validate the admission-time lease once."""

        rows = executor.get_records(
            f"""
            SELECT f.current_generation, f.current_attempt_key,
                   f.current_route_fingerprint, a.generation,
                   o.scope_hash, o.current_epoch, o.current_owner_digest,
                   o.lease_expires_at_utc,
                   CASE WHEN o.lease_expires_at_utc IS NULL
                             OR o.lease_expires_at_utc > SYSUTCDATETIME()
                        THEN 1 ELSE 0 END AS lease_is_current
            FROM {self.fence_table} AS f WITH (HOLDLOCK)
            INNER JOIN {self.attempt_table} AS a WITH (HOLDLOCK)
                ON a.target_identity = f.target_identity
               AND a.attempt_key = f.current_attempt_key
            INNER JOIN {self.operation_table} AS o WITH (UPDLOCK, HOLDLOCK)
                ON o.attempt_key = a.attempt_key
            WHERE f.target_identity = ? AND o.operation_key = ?
            """,
            (operation.attempt.target_identity, operation.operation_key),
            as_dict=True,
        )
        if len(rows) != 1:
            raise RuntimeError("mssql_transaction.fence_missing_or_ambiguous")
        row = rows[0]
        exact = (
            int(row.get("current_generation") or 0) == operation.attempt.generation
            and int(row.get("generation") or 0) == operation.attempt.generation
            and binary(row.get("current_attempt_key")) == operation.attempt.attempt_key
            and binary(row.get("current_route_fingerprint")) == operation.attempt.route_fingerprint
            and binary(row.get("scope_hash")) == operation.scope_hash
            and int(row.get("current_epoch") or 0) == operation.epoch
            and binary(row.get("current_owner_digest")) == operation.owner_digest
            and (not require_unexpired_lease or int(row.get("lease_is_current") or 0) == 1)
        )
        if not exact:
            raise RuntimeError("mssql_transaction.stale_source_or_operation_generation")

    def insert_receipt(
        self,
        executor: Any,
        operation: MssqlTransactionOperation,
        *,
        load_id: str,
        payload_evidence: MssqlPayloadCommitEvidence,
        source_lifecycle: MssqlSourceLifecycleEvidence,
        mutation_plan_sha256: bytes,
        target_before_sha256: bytes,
        target_after_sha256: bytes,
        loaded_at_utc: datetime,
        metrics: MssqlReceiptMetrics,
    ) -> MssqlGenericCommitReceipt:
        attempt = operation.attempt
        executor.execute_query(
            f"""
            INSERT INTO {self.receipt_table} (
                receipt_id, operation_key, attempt_key, scope_hash,
                operation_epoch, owner_digest, load_id,
                payload_manifest_sha256, declared_rows, actual_raw_rows,
                actual_native_rows, native_contract_sha256,
                mutation_plan_sha256, target_before_sha256, target_after_sha256,
                extraction_started_at_utc, extraction_completed_at_utc,
                extraction_clock_authority, snapshot_acquired_at_utc,
                snapshot_authority, source_token_sha256, loaded_at_utc,
                inserted_rows, updated_rows, total_rows,
                staging_rows, replaced_rows, soft_deleted_rows, reactivated_rows,
                unchanged_rows, hard_deleted_rows, active_rows, committed_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
            """,
            (
                operation.receipt_id,
                operation.operation_key,
                attempt.attempt_key,
                operation.scope_hash,
                operation.epoch,
                operation.owner_digest,
                load_id,
                payload_evidence.manifest_sha256,
                payload_evidence.declared_rows,
                payload_evidence.actual_raw_rows,
                payload_evidence.actual_native_rows,
                payload_evidence.native_contract_sha256,
                mutation_plan_sha256,
                target_before_sha256,
                target_after_sha256,
                _datetime2_utc(source_lifecycle.extraction_started_at_utc),
                _datetime2_utc(source_lifecycle.extraction_completed_at_utc),
                source_lifecycle.clock_authority,
                (
                    _datetime2_utc(source_lifecycle.snapshot_acquired_at_utc)
                    if source_lifecycle.snapshot_acquired_at_utc is not None
                    else None
                ),
                source_lifecycle.snapshot_authority,
                source_lifecycle.source_token_sha256,
                _datetime2_utc(loaded_at_utc),
                metrics.inserted_rows,
                metrics.updated_rows,
                metrics.total_rows,
                metrics.staging_rows,
                metrics.replaced_rows,
                metrics.soft_deleted_rows,
                metrics.reactivated_rows,
                metrics.unchanged_rows,
                metrics.hard_deleted_rows,
                metrics.active_rows,
            ),
        )
        receipt = self.probe_receipt(operation, connector=executor, require_owner=True)
        if receipt is None:
            raise RuntimeError("mssql_transaction.receipt_insert_unverified")
        return receipt

    def probe_receipt_fresh(
        self,
        operation: MssqlTransactionOperation,
    ) -> MssqlGenericCommitReceipt | None:
        with self.fresh_sessions.open(self.connector) as session:
            return self.probe_receipt(operation, connector=session, require_owner=True)

    def probe_receipt(
        self,
        operation: MssqlTransactionOperation,
        *,
        connector: Any | None = None,
        require_owner: bool = False,
    ) -> MssqlGenericCommitReceipt | None:
        receipt = self.receipt_by_key(operation.operation_key, connector=connector)
        if receipt is None:
            return None
        attempt = operation.attempt
        exact = (
            receipt.receipt_id == operation.receipt_id
            and receipt.operation_key == operation.operation_key
            and receipt.attempt_key == attempt.attempt_key
            and receipt.target_identity == attempt.target_identity
            and receipt.generation == attempt.generation
            and receipt.scope_hash == operation.scope_hash
            and receipt.route_fingerprint == attempt.route_fingerprint
            and receipt.strategy == attempt.request.strategy
        )
        if not exact:
            raise RuntimeError("mssql_transaction.receipt_mismatch")
        if require_owner and (
            receipt.operation_epoch != operation.epoch or receipt.owner_digest != operation.owner_digest
        ):
            raise RuntimeError("mssql_transaction.receipt_owner_mismatch")
        return receipt

    def receipt_by_key(
        self,
        operation_key: bytes,
        *,
        connector: Any | None = None,
    ) -> MssqlGenericCommitReceipt | None:
        session = connector or self.connector
        # Transactional finalizer callers already own the operation-scoped
        # application lock; other callers read only immutable committed rows.
        # READ COMMITTED therefore avoids retaining an absent-key range lock
        # that can deadlock unrelated parallel chunk receipt inserts.
        rows = session.get_records(
            f"""
            SELECT r.receipt_id, r.operation_key, r.attempt_key,
                   a.target_identity, a.generation, r.scope_hash,
                   r.operation_epoch, r.owner_digest, a.route_fingerprint,
                   r.load_id, a.strategy, r.payload_manifest_sha256,
                   r.declared_rows, r.actual_raw_rows, r.actual_native_rows,
                   r.native_contract_sha256, r.mutation_plan_sha256,
                   r.target_before_sha256, r.target_after_sha256,
                   r.extraction_started_at_utc,
                   r.extraction_completed_at_utc, r.extraction_clock_authority,
                   r.snapshot_acquired_at_utc, r.snapshot_authority,
                   r.source_token_sha256,
                   r.loaded_at_utc, r.committed_at_utc,
                   r.inserted_rows, r.updated_rows, r.total_rows,
                   r.staging_rows, r.replaced_rows, r.soft_deleted_rows, r.reactivated_rows,
                   r.unchanged_rows, r.hard_deleted_rows, r.active_rows
            FROM {self.receipt_table} AS r WITH (READCOMMITTED)
            INNER JOIN {self.attempt_table} AS a ON a.attempt_key = r.attempt_key
            WHERE r.operation_key = ?
            """,
            (operation_key,),
            as_dict=True,
        )
        if not rows:
            return None
        if len(rows) != 1:
            raise RuntimeError("mssql_transaction.receipt_ambiguous")
        return receipt_from_row(rows[0])

    @property
    def fence_table(self) -> str:
        return self.qualified(FENCE_TABLE)

    @property
    def attempt_table(self) -> str:
        return self.qualified(ATTEMPT_TABLE)

    @property
    def operation_table(self) -> str:
        return self.qualified(OPERATION_TABLE)

    @property
    def receipt_table(self) -> str:
        return self.qualified(RECEIPT_TABLE)


def _datetime2_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise RuntimeError("mssql_transaction.datetime2_utc_required")
    return value.astimezone(UTC).replace(tzinfo=None)


__all__ = ["MssqlGenericOperationState", "MssqlOperationClaimOutcomeUnknown"]
