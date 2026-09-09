"""Recover a shadow-initial chunk from its atomic SQL Server receipt."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.backfill.state import CHUNK_STATUS_RUNNING, CHUNK_STATUS_SUCCESS
from dpone.runtime.etl.mssql_transaction_identity import (
    is_mssql_generic_commit_receipt,
    is_mssql_transaction_operation,
)


class MssqlBackfillReceiptRecoveryError(RuntimeError):
    """Receipt evidence cannot safely become a campaign-ledger success."""


@dataclass(frozen=True, slots=True)
class MssqlBackfillReceiptRecoveryEvidence:
    """Redacted proof that one target receipt was projected exactly once."""

    receipt_id: str
    operation_key: str
    chunk_index: int
    rows: int
    load_id: str
    already_projected: bool


class MssqlShadowInitialReceiptRecovery:
    """Promote an atomic append receipt without source COPY or target DML.

    The generic receipt was inserted in the same SQL Server transaction as
    the shadow append.  This service only projects that immutable authority
    into the append-only backfill ledger under its current chunk-owner CAS.
    An absent receipt is not success; malformed or non-append metrics fail
    closed and remain unavailable for publication.
    """

    def __init__(self, transaction_state: Any) -> None:
        self._transaction_state = transaction_state

    def promote_owned(
        self,
        operation: Any,
        *,
        store: Any,
        run_key: str,
        chunk_index: int,
        owner: str,
    ) -> MssqlBackfillReceiptRecoveryEvidence | None:
        """Probe a fresh session and CAS-promote one exact committed chunk."""

        receipt = self._transaction_state.probe_receipt_fresh(operation)
        if receipt is None:
            return None
        self._require_operation_receipt_identity(operation, receipt)
        rows = self._require_exact_append_receipt(receipt)
        current = store.load(run_key)
        if current is None:
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.campaign_missing")
        record = current.chunk(chunk_index)
        if record.status == CHUNK_STATUS_SUCCESS:
            self._require_matching_projection(record, receipt=receipt, rows=rows)
            return self._evidence(receipt, chunk_index=chunk_index, rows=rows, already_projected=True)
        if record.status != CHUNK_STATUS_RUNNING or record.lease_owner != owner:
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.chunk_owner_mismatch")

        record.status = CHUNK_STATUS_SUCCESS
        record.error = None
        record.finished_at = receipt.committed_at_utc.isoformat()
        record.rows_extracted = rows
        record.rows_loaded = rows
        record.load_id = receipt.load_id
        record.execution_evidence = {
            "mssql_receipt_recovery": {
                "schema": "dpone.mssql.receipt-recovery-evidence.v1",
                "status": "committed",
                "source_io_replayed": False,
                "rows": rows,
            }
        }
        if not store.complete_chunk_if_owned(run_key, record, owner=owner):
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.chunk_owner_cas_lost")
        return self._evidence(receipt, chunk_index=chunk_index, rows=rows, already_projected=False)

    @staticmethod
    def _require_operation_receipt_identity(operation: Any, receipt: Any) -> None:
        """Defend the ledger projection boundary even if a state adapter is faulty."""

        if not is_mssql_transaction_operation(operation):
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.operation_invalid")
        if not is_mssql_generic_commit_receipt(receipt):
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.invalid_type")
        attempt = operation.attempt
        exact = (
            receipt.receipt_id == operation.receipt_id
            and receipt.operation_key == operation.operation_key
            and receipt.attempt_key == attempt.attempt_key
            and receipt.target_identity == attempt.target_identity
            and receipt.generation == attempt.generation
            and receipt.scope_hash == operation.scope_hash
            and receipt.operation_epoch == operation.epoch
            and receipt.owner_digest == operation.owner_digest
            and receipt.route_fingerprint == attempt.route_fingerprint
            and receipt.strategy == attempt.request.strategy
        )
        if not exact:
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.operation_identity_mismatch")

    @staticmethod
    def _require_exact_append_receipt(receipt: Any) -> int:
        if not is_mssql_generic_commit_receipt(receipt):
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.invalid_type")
        if receipt.strategy != "backfill":
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.strategy_mismatch")
        payload = receipt.payload_evidence
        payload_counts = (payload.declared_rows, payload.actual_raw_rows, payload.actual_native_rows)
        if not all(MssqlShadowInitialReceiptRecovery._is_non_negative_int(value) for value in payload_counts):
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.payload_count_invalid")
        if len(set(payload_counts)) != 1:
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.payload_count_mismatch")
        payload_rows = payload.actual_native_rows
        metrics = receipt.metrics
        zero_or_none = (
            metrics.replaced_rows,
            metrics.soft_deleted_rows,
            metrics.reactivated_rows,
            metrics.unchanged_rows,
            metrics.hard_deleted_rows,
        )
        required_metrics = (
            metrics.staging_rows,
            metrics.inserted_rows,
            metrics.updated_rows,
            metrics.total_rows,
        )
        optional_counts = (*zero_or_none, metrics.active_rows)
        optional_metrics_valid = all(
            value is None or MssqlShadowInitialReceiptRecovery._is_non_negative_int(value) for value in optional_counts
        )
        if not all(MssqlShadowInitialReceiptRecovery._is_non_negative_int(value) for value in required_metrics):
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.metric_count_invalid")
        if not optional_metrics_valid:
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.metric_count_invalid")

        # Under shadow-append authority, the MSSQL strategy intentionally
        # defines total_rows as this chunk's inserted cardinality.  It is not
        # the cumulative cardinality of the still-private shadow table.
        exact = (
            metrics.staging_rows == payload_rows
            and metrics.inserted_rows == payload_rows
            and metrics.updated_rows == 0
            and metrics.total_rows == payload_rows
            and all(value in (None, 0) for value in zero_or_none)
        )
        if not exact:
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.metrics_mismatch")
        return payload_rows

    @staticmethod
    def _is_non_negative_int(value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0

    @staticmethod
    def _require_matching_projection(record: Any, *, receipt: Any, rows: int) -> None:
        exact = (
            record.rows_extracted == rows and record.rows_loaded == rows and record.load_id in (None, receipt.load_id)
        )
        if not exact:
            raise MssqlBackfillReceiptRecoveryError("mssql_backfill_receipt.ledger_projection_mismatch")

    @staticmethod
    def _evidence(
        receipt: Any,
        *,
        chunk_index: int,
        rows: int,
        already_projected: bool,
    ) -> MssqlBackfillReceiptRecoveryEvidence:
        return MssqlBackfillReceiptRecoveryEvidence(
            receipt_id=receipt.receipt_id,
            operation_key=receipt.operation_key.hex(),
            chunk_index=chunk_index,
            rows=rows,
            load_id=receipt.load_id,
            already_projected=already_projected,
        )


__all__ = [
    "MssqlBackfillReceiptRecoveryError",
    "MssqlBackfillReceiptRecoveryEvidence",
    "MssqlShadowInitialReceiptRecovery",
]
