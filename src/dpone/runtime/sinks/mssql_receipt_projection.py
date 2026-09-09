"""Pure projection of a durable generic MSSQL receipt into LoadResult."""

from __future__ import annotations

from dpone.contracts.mssql_transaction_governance import (
    MssqlCleanupDisposition,
    MssqlGenericCommitReceipt,
)
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult


def load_result_from_mssql_receipt(
    receipt: MssqlGenericCommitReceipt,
    *,
    outcome: AtomicCommitOutcome,
) -> LoadResult:
    """Return exact committed metrics without reading or mutating the source."""

    metrics = receipt.metrics
    replay = outcome == AtomicCommitOutcome.REPLAY_SUPPRESSED
    return LoadResult(
        inserted_rows=metrics.inserted_rows,
        updated_rows=metrics.updated_rows,
        total_rows=metrics.total_rows,
        staging_rows=metrics.staging_rows,
        replaced_rows=metrics.replaced_rows,
        soft_deleted_rows=metrics.soft_deleted_rows,
        reactivated_rows=metrics.reactivated_rows,
        unchanged_rows=metrics.unchanged_rows,
        hard_deleted_rows=metrics.hard_deleted_rows,
        active_rows=metrics.active_rows,
        commit_receipt_id=receipt.receipt_id,
        commit_outcome=outcome,
        cleanup_disposition=MssqlCleanupDisposition.COMMITTED_SECONDARY_ONLY,
        reconciliation_metrics={"mssql_transaction_replay_suppressed": True} if replay else None,
    )


__all__ = ["load_result_from_mssql_receipt"]
