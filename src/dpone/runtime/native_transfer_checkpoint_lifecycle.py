"""Fail-closed checkpoint transitions for native partitioned transfers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.lineage.artifact_checksum import ArtifactChecksumService
    from dpone.runtime.lineage.partition_checkpoint_store import PartitionCheckpointStore


from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dpone.runtime.lineage.partition_checkpoint import (
    PartitionCheckpoint,
    PartitionCheckpointStatus,
)
from dpone.runtime.lineage.partition_resume import (
    NativeTransferTargetCommitGuard,
    build_native_transfer_checkpoint_batch_id,
)
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.native_transfer_plan_runtime import NativeTransferPlanRuntimeAdapter
from dpone.runtime.native_transfer_row_authority import (
    actual_export_rows,
    authority_diagnostics,
    canonical_non_negative_int,
)

DEFAULT_SAFE_FAILURE_CODE = "native_transfer_failed"

_UTC = timezone.utc  # noqa: UP017 - current mypy target still includes Python 3.10.


class NativeTransferCheckpointLifecycle:
    """Persist guard-aware batches without replacing a primary store error."""

    def __init__(
        self,
        *,
        checkpoint_store: PartitionCheckpointStore | None,
        checksum_service: ArtifactChecksumService,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._checkpoint_store = checkpoint_store
        self._checksum_service = checksum_service
        self._clock = clock or _utc_now

    def mark_exported(self, artifacts: tuple[Any, ...]) -> tuple[str, ...]:
        """Persist legacy-compatible extraction state before the target guard."""

        if not artifacts or self._checkpoint_store is None:
            return ()
        transition_at = _utc_now()
        checkpoints = tuple(
            self._checkpoint_for_artifact(
                artifact,
                _status("exported"),
                error=None,
                transition_at=transition_at,
                target_commit_guard=None,
            )
            for artifact in artifacts
        )
        self._checkpoint_store.upsert_many(checkpoints)
        return _partition_ids(checkpoints)

    def arm(self, context: Any) -> tuple[str, ...]:
        """Persist the complete guard batch before target invocation."""

        if not context.active_artifacts or self._checkpoint_store is None:
            return ()
        transition_at = self._transition_at()
        base = self._context_checkpoints(
            context,
            _status("exported"),
            error=None,
            transition_at=transition_at,
            target_commit_guard=None,
        )
        batch_id = build_native_transfer_checkpoint_batch_id(
            context.run_id,
            _partition_ids(base),
        )
        context.terminal_state.checkpoint_batch_id = batch_id
        checkpoints = _with_guard(
            base,
            self._guard(context, phase="armed", batch_id=batch_id),
        )
        self._checkpoint_store.upsert_many(checkpoints)
        context.terminal_state.guard_armed = True
        return _partition_ids(checkpoints)

    def commit(self, context: Any) -> tuple[str, ...]:
        """Promote one guarded batch and reconcile only exact durable candidates."""

        if not context.active_artifacts or self._checkpoint_store is None:
            return ()
        batch_id = self._required_batch_id(context)
        transition_at = self._transition_at()
        candidates = self._context_checkpoints(
            context,
            _status("committed"),
            error=None,
            transition_at=transition_at,
            target_commit_guard=self._guard(
                context,
                phase="checkpoint_committed",
                batch_id=batch_id,
            ),
        )
        try:
            self._checkpoint_store.upsert_many(candidates)
        except Exception:
            self._handle_partial_commit(context, candidates)
            raise
        committed = _partition_ids(candidates)
        context.terminal_state.committed_partition_ids.update(committed)
        return committed

    def mark_failed(
        self,
        context: Any,
        *,
        error: str,
        phase: str,
    ) -> tuple[str, ...]:
        """Persist a bounded failure phase selected from process-local facts."""

        if not context.active_artifacts or self._checkpoint_store is None:
            return ()
        transition_at = self._transition_at()
        base = self._context_checkpoints(
            context,
            _status("failed"),
            error=error,
            transition_at=transition_at,
            target_commit_guard=None,
        )
        batch_id = context.terminal_state.checkpoint_batch_id
        if batch_id is None:
            batch_id = build_native_transfer_checkpoint_batch_id(
                context.run_id,
                _partition_ids(base),
            )
            context.terminal_state.checkpoint_batch_id = batch_id
        checkpoints = _with_guard(
            base,
            self._guard(context, phase=phase, batch_id=batch_id),
        )
        self._checkpoint_store.upsert_many(checkpoints)
        return _partition_ids(checkpoints)

    def _handle_partial_commit(
        self,
        context: Any,
        candidates: tuple[PartitionCheckpoint, ...],
    ) -> None:
        """Best-effort reconciliation must never replace the commit exception."""

        if self._checkpoint_store is None:
            return
        try:
            latest = {
                checkpoint.transfer_partition_id: checkpoint for checkpoint in self._checkpoint_store.list_latest()
            }
        except Exception:
            return
        committed = {
            candidate.transfer_partition_id
            for candidate in candidates
            if latest.get(candidate.transfer_partition_id) == candidate
        }
        context.terminal_state.committed_partition_ids.update(committed)
        unresolved_ids = {
            candidate.transfer_partition_id
            for candidate in candidates
            if candidate.transfer_partition_id not in committed
        }
        if not unresolved_ids:
            return
        try:
            marker_at = self._transition_at()
            markers = self._context_checkpoints(
                context,
                _status("failed"),
                error=DEFAULT_SAFE_FAILURE_CODE,
                transition_at=marker_at,
                target_commit_guard=self._guard(
                    context,
                    phase="checkpoint_incomplete",
                    batch_id=self._required_batch_id(context),
                ),
            )
            self._checkpoint_store.upsert_many(
                tuple(marker for marker in markers if marker.transfer_partition_id in unresolved_ids)
            )
        except Exception:
            return

    def _context_checkpoints(
        self,
        context: Any,
        status: PartitionCheckpointStatus,
        *,
        error: str | None,
        transition_at: datetime,
        target_commit_guard: Mapping[str, Any] | None,
    ) -> tuple[PartitionCheckpoint, ...]:
        artifact = context.payload.artifact
        if isinstance(artifact, PartitionedTransferPlanArtifact):
            return NativeTransferPlanRuntimeAdapter.checkpoints(
                context.load_config,
                artifact,
                status,
                error=error,
                transition_at=transition_at,
                target_commit_guard=target_commit_guard,
            )
        return tuple(
            self._checkpoint_for_artifact(
                item,
                status,
                error=error,
                transition_at=transition_at,
                target_commit_guard=target_commit_guard,
            )
            for item in context.active_artifacts
        )

    def _checkpoint_for_artifact(
        self,
        artifact: Any,
        status: PartitionCheckpointStatus,
        *,
        error: str | None,
        transition_at: datetime,
        target_commit_guard: Mapping[str, Any] | None,
    ) -> PartitionCheckpoint:
        artifact_sha256 = getattr(artifact, "artifact_sha256", None)
        if artifact_sha256 is None:
            artifact_sha256 = self._checksum_service.checksum(artifact)
            setattr(artifact, "artifact_sha256", artifact_sha256)
        source_rows = actual_export_rows(artifact)
        target_rows = canonical_non_negative_int(getattr(artifact, "rows_loaded", None))
        diagnostics: dict[str, Any] = {"artifact_sha256": artifact_sha256}
        if error:
            diagnostics["error"] = error
        if target_commit_guard is not None:
            diagnostics["target_commit_guard"] = dict(target_commit_guard)
        diagnostics = authority_diagnostics(
            source_rows=source_rows,
            target_rows=target_rows,
            base=diagnostics,
        )
        file_path = Path(artifact.file_path)
        return PartitionCheckpoint(
            transfer_partition_id=str(getattr(artifact, "transfer_partition_id")),
            status=status,
            query_hash=str(getattr(artifact, "query_hash")),
            schema_hash=str(getattr(artifact, "schema_hash")),
            source_table=str(getattr(artifact, "source_table")),
            target_table=str(getattr(artifact, "target_table")),
            partition_bounds=dict(getattr(artifact, "partition_bounds")),
            started_at=transition_at,
            completed_at=(
                transition_at
                if status
                in {
                    PartitionCheckpointStatus.COMMITTED,
                    PartitionCheckpointStatus.FAILED,
                }
                else None
            ),
            rows_exported=source_rows,
            bytes_exported=file_path.stat().st_size if file_path.exists() else None,
            diagnostics=diagnostics,
        )

    def _guard(
        self,
        context: Any,
        *,
        phase: str,
        batch_id: str,
    ) -> dict[str, str]:
        return NativeTransferTargetCommitGuard(
            checkpoint_batch_id=batch_id,
            phase=phase,
            strategy=_context_strategy(context),
        ).to_dict()

    def _required_batch_id(self, context: Any) -> str:
        batch_id = context.terminal_state.checkpoint_batch_id
        if batch_id is None:
            raise RuntimeError("native_transfer_target_commit_guard_not_armed")
        return batch_id

    def _transition_at(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("native_transfer_transition_timestamp_invalid")
        return value.astimezone(_UTC)


def safe_failure_code(value: str | None) -> str:
    """Accept bounded internal codes and reject untrusted exception text."""

    if (
        isinstance(value, str)
        and 1 <= len(value) <= 128
        and all(character.isalnum() or character == "_" for character in value)
    ):
        return value
    return DEFAULT_SAFE_FAILURE_CODE


def _with_guard(
    checkpoints: tuple[PartitionCheckpoint, ...],
    guard: Mapping[str, Any],
) -> tuple[PartitionCheckpoint, ...]:
    return tuple(
        replace(
            checkpoint,
            diagnostics={
                **checkpoint.diagnostics,
                "target_commit_guard": dict(guard),
            },
        )
        for checkpoint in checkpoints
    )


def _partition_ids(
    checkpoints: tuple[PartitionCheckpoint, ...],
) -> tuple[str, ...]:
    return tuple(checkpoint.transfer_partition_id for checkpoint in checkpoints)


def _context_strategy(context: Any) -> str:
    artifact = context.payload.artifact
    strategy = getattr(artifact, "strategy", None)
    if strategy is None and context.active_artifacts:
        strategy = getattr(context.active_artifacts[0], "strategy", None)
    if strategy is None:
        strategy = context.load_config.load_strategy.value
    return str(strategy)


def _utc_now() -> datetime:
    return datetime.now(_UTC)


def _status(value: str) -> PartitionCheckpointStatus:
    return PartitionCheckpointStatus(value)


__all__ = [
    "DEFAULT_SAFE_FAILURE_CODE",
    "NativeTransferCheckpointLifecycle",
    "safe_failure_code",
]
