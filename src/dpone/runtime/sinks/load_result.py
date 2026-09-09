"""Sink load result contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone._compat import StrEnum

if TYPE_CHECKING:
    from dpone.contracts.mssql_transaction_governance import MssqlCleanupDisposition
    from dpone.runtime.governance.service import QualityGateReceipt


class AtomicCommitOutcome(StrEnum):
    """Receipt-backed outcomes accepted for snapshot-envelope loads."""

    COMMITTED = "committed"
    COMMITTED_AFTER_RECEIPT_PROBE = "committed_after_receipt_probe"
    REPLAY_SUPPRESSED = "replay_suppressed"


@dataclass(frozen=True)
class LoadResult:
    """Result returned by a sink load strategy."""

    inserted_rows: int
    updated_rows: int
    total_rows: int
    state: Any | None = None
    staging_rows: int | None = None
    soft_deleted_rows: int | None = None
    reactivated_rows: int | None = None
    unchanged_rows: int | None = None
    hard_deleted_rows: int | None = None
    active_rows: int | None = None
    commit_receipt_id: str | None = None
    commit_outcome: AtomicCommitOutcome | None = None
    replaced_rows: int | None = None
    deleted_lookback_rows: int | None = None
    reconciliation_metrics: dict[str, Any] | None = None
    partition_validation_results: dict[str, Any] | None = None
    quality_gate_receipt: QualityGateReceipt | None = None
    cleanup_disposition: MssqlCleanupDisposition | None = None


__all__ = ["AtomicCommitOutcome", "LoadResult"]
