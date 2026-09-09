"""Process-local terminal policy for checkpointed native transfers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

from dpone.runtime.commit_unknown import CommitUnknownError, classify_commit_unknown
from dpone.runtime.native_transfer_quality_scope import NativeTransferQualityScope
from dpone.runtime.native_transfer_row_authority import (
    canonical_non_negative_int,
    stamp_single_partition_loader_rows,
)
from dpone.runtime.sinks.load_result import LoadResult


@dataclass
class NativeTransferTerminalState:
    """Facts used to classify target durability without leaking target data."""

    committed_partition_ids: set[str] = field(default_factory=set)
    checkpoint_batch_id: str | None = None
    guard_armed: bool = False
    target_invocation_started: bool = False
    target_returned_success: bool = False
    target_rows: int | None = None


@dataclass(frozen=True)
class PreCheckpointQualityScope:
    """Expose process-local target rows until durable checkpoints exist."""

    delegate: NativeTransferQualityScope
    terminal_state: NativeTransferTerminalState

    def projected_snapshots(self, staged_rows: object) -> Any:
        return self.delegate.projected_snapshots(staged_rows)

    def resume_snapshots(self) -> Any:
        return self.delegate.resume_snapshots()

    def committed_snapshots(self, checkpoint_store: Any | None = None) -> Any:
        if not self.terminal_state.target_returned_success:
            return self.delegate.committed_snapshots(checkpoint_store)
        return self.delegate.projected_snapshots(self.terminal_state.target_rows)


def record_native_transfer_target_success(context: Any, load_result: LoadResult) -> None:
    """Record target success in memory without advancing durable checkpoints."""

    context.terminal_state.target_returned_success = True
    stamp_single_partition_loader_rows(tuple(context.active_artifacts or ()), load_result)
    context.terminal_state.target_rows = _target_success_rows(context, load_result)


def native_transfer_quality_scope(context: Any) -> PreCheckpointQualityScope:
    """Build the quality projection used before checkpoint publication."""

    return PreCheckpointQualityScope(
        delegate=NativeTransferQualityScope.from_context(context),
        terminal_state=context.terminal_state,
    )


def classify_native_transfer_commit_unknown(context: Any) -> CommitUnknownError | None:
    """Classify the interval where target durability is not proven."""

    if not context.enabled:
        return None
    state = context.terminal_state
    expected_checkpoint_ids = (
        frozenset(decision.partition.transfer_partition_id for decision in context.resume_plan.retry)
        | context.reactivated_partition_ids
    )
    return classify_commit_unknown(
        target_invocation_started=state.target_invocation_started,
        target_returned_success=state.target_returned_success,
        expected_checkpoint_ids=expected_checkpoint_ids,
        committed_checkpoint_ids=frozenset(state.committed_partition_ids),
    )


def _target_success_rows(context: Any, load_result: LoadResult) -> int | None:
    if (staging_rows := canonical_non_negative_int(load_result.staging_rows)) is not None:
        return staging_rows
    rows = tuple(
        canonical_non_negative_int(getattr(artifact, "rows_loaded", None)) for artifact in context.active_artifacts
    )
    if not rows or None in rows:
        return None
    return sum(cast(tuple[int, ...], rows))


__all__ = [
    "CommitUnknownError",
    "NativeTransferTerminalState",
    "PreCheckpointQualityScope",
    "classify_native_transfer_commit_unknown",
    "native_transfer_quality_scope",
    "record_native_transfer_target_success",
]
