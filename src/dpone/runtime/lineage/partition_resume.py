"""Checkpoint-driven partition resume planning."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, cast

from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpointStatus

if TYPE_CHECKING:
    from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint
    from dpone.runtime.lineage.partition_checkpoint_store import PartitionCheckpointStore
TARGET_COMMIT_GUARD_KEY = "target_commit_guard"
TARGET_COMMIT_GUARD_SCHEMA_VERSION = "dpone.native_transfer.target_commit_guard.v1"
TARGET_COMMIT_GUARD_RECOVERY = "manual_reconciliation_required"

_GUARD_PHASE_STATUS = {
    "armed": PartitionCheckpointStatus.EXPORTED,
    "failed_before_target": PartitionCheckpointStatus.FAILED,
    "checkpoint_committed": PartitionCheckpointStatus.COMMITTED,
    "checkpoint_incomplete": PartitionCheckpointStatus.FAILED,
    "target_outcome_unknown": PartitionCheckpointStatus.FAILED,
}
_UNRESOLVED_GUARD_PHASES = frozenset(
    {
        "armed",
        "checkpoint_incomplete",
        "target_outcome_unknown",
    }
)


class NativeTransferTargetCommitGuardError(RuntimeError):
    """Fail closed without echoing untrusted checkpoint diagnostics."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class NativeTransferTargetCommitGuard:
    """Validated bounded guard stored in open checkpoint diagnostics."""

    checkpoint_batch_id: str
    phase: str
    strategy: str | None

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: PartitionCheckpoint,
    ) -> NativeTransferTargetCommitGuard | None:
        diagnostics = checkpoint.diagnostics
        if TARGET_COMMIT_GUARD_KEY not in diagnostics:
            return None
        raw_guard = diagnostics[TARGET_COMMIT_GUARD_KEY]
        if not isinstance(raw_guard, Mapping):
            raise _invalid_guard()
        schema_version = raw_guard.get("schema_version")
        checkpoint_batch_id = raw_guard.get("checkpoint_batch_id")
        phase = raw_guard.get("phase")
        recovery = raw_guard.get("recovery")
        strategy = raw_guard.get("strategy")
        if (
            schema_version != TARGET_COMMIT_GUARD_SCHEMA_VERSION
            or not _is_sha256(checkpoint_batch_id)
            or not isinstance(phase, str)
            or recovery != TARGET_COMMIT_GUARD_RECOVERY
            or strategy is not None
            and not _is_bounded_strategy(strategy)
            or _GUARD_PHASE_STATUS.get(phase) is not checkpoint.status
        ):
            raise _invalid_guard()
        return cls(
            checkpoint_batch_id=cast(str, checkpoint_batch_id),
            phase=phase,
            strategy=strategy,
        )

    @property
    def requires_manual_reconciliation(self) -> bool:
        return self.phase in _UNRESOLVED_GUARD_PHASES

    def applies_to_strategy(self, strategy: str) -> bool:
        return self.strategy is None or self.strategy == strategy

    def to_dict(self) -> dict[str, str]:
        payload = {
            "schema_version": TARGET_COMMIT_GUARD_SCHEMA_VERSION,
            "checkpoint_batch_id": self.checkpoint_batch_id,
            "phase": self.phase,
            "recovery": TARGET_COMMIT_GUARD_RECOVERY,
        }
        if self.strategy is not None:
            payload["strategy"] = self.strategy
        return payload


def build_native_transfer_checkpoint_batch_id(
    run_id: str,
    partition_ids: tuple[str, ...],
) -> str:
    """Correlate one attempt using its run and sorted active partition IDs."""

    encoded = json.dumps(
        {
            "partition_ids": sorted(partition_ids),
            "run_id": run_id,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"sha256:{hashlib.sha256(encoded.encode('utf-8')).hexdigest()}"


@dataclass(frozen=True)
class PlannedTransferPartition:
    """A partition expected in the current native transfer attempt."""

    transfer_partition_id: str
    source_table: str
    target_table: str
    strategy: str
    query_hash: str
    schema_hash: str
    partition_bounds: dict[str, Any]
    artifact_sha256: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PartitionResumeDecision:
    """Resume action for one planned partition."""

    partition: PlannedTransferPartition
    action: str
    reason: str
    checkpoint: PartitionCheckpoint | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition": self.partition.to_dict(),
            "action": self.action,
            "reason": self.reason,
            "checkpoint_status": self.checkpoint.status.value if self.checkpoint else None,
        }


@dataclass(frozen=True)
class PartitionResumePlan:
    """Skip/retry decision for a transfer attempt."""

    decisions: tuple[PartitionResumeDecision, ...]

    @property
    def skip(self) -> tuple[PartitionResumeDecision, ...]:
        return tuple(decision for decision in self.decisions if decision.action == "skip")

    @property
    def retry(self) -> tuple[PartitionResumeDecision, ...]:
        return tuple(decision for decision in self.decisions if decision.action == "retry")

    @property
    def summary(self) -> dict[str, int]:
        return {"skip": len(self.skip), "retry": len(self.retry)}

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary,
            "decisions": [decision.to_dict() for decision in self.decisions],
        }


class PartitionResumePlanner:
    """Build deterministic skip/retry decisions from durable checkpoints."""

    def __init__(self, *, require_artifact_checksum: bool = False) -> None:
        self._require_artifact_checksum = require_artifact_checksum

    def plan(
        self,
        planned_partitions: tuple[PlannedTransferPartition, ...],
        checkpoint_store: PartitionCheckpointStore,
    ) -> PartitionResumePlan:
        latest_checkpoints = checkpoint_store.list_latest()
        self._validate_route_guards(planned_partitions, latest_checkpoints)
        latest = {checkpoint.transfer_partition_id: checkpoint for checkpoint in latest_checkpoints}
        decisions = tuple(
            self._decision(partition, latest.get(partition.transfer_partition_id)) for partition in planned_partitions
        )
        return PartitionResumePlan(decisions=decisions)

    @staticmethod
    def _validate_route_guards(
        planned_partitions: tuple[PlannedTransferPartition, ...],
        checkpoints: tuple[PartitionCheckpoint, ...],
    ) -> None:
        routes = {
            (
                partition.source_table,
                partition.target_table,
                partition.strategy,
            )
            for partition in planned_partitions
        }
        if not routes:
            return
        invalid = False
        unresolved = False
        for checkpoint in checkpoints:
            matching_strategies = {
                strategy
                for source_table, target_table, strategy in routes
                if checkpoint.source_table == source_table and checkpoint.target_table == target_table
            }
            if not matching_strategies or TARGET_COMMIT_GUARD_KEY not in checkpoint.diagnostics:
                continue
            raw_guard = checkpoint.diagnostics[TARGET_COMMIT_GUARD_KEY]
            raw_strategy = raw_guard.get("strategy") if isinstance(raw_guard, Mapping) else None
            if isinstance(raw_strategy, str) and raw_strategy not in matching_strategies:
                continue
            try:
                guard = NativeTransferTargetCommitGuard.from_checkpoint(checkpoint)
            except NativeTransferTargetCommitGuardError:
                invalid = True
                continue
            if guard is None or not any(guard.applies_to_strategy(item) for item in matching_strategies):
                continue
            unresolved = unresolved or guard.requires_manual_reconciliation
        if invalid:
            raise _invalid_guard()
        if unresolved:
            raise NativeTransferTargetCommitGuardError("native_transfer_manual_reconciliation_required")

    def _decision(
        self,
        partition: PlannedTransferPartition,
        checkpoint: PartitionCheckpoint | None,
    ) -> PartitionResumeDecision:
        if checkpoint is None:
            return PartitionResumeDecision(partition=partition, action="retry", reason="missing_checkpoint")
        if checkpoint.status is not PartitionCheckpointStatus.COMMITTED:
            return PartitionResumeDecision(
                partition=partition,
                action="retry",
                reason=f"latest_status_{checkpoint.status.value}",
                checkpoint=checkpoint,
            )
        if checkpoint.query_hash != partition.query_hash or checkpoint.schema_hash != partition.schema_hash:
            return PartitionResumeDecision(
                partition=partition,
                action="retry",
                reason="query_or_schema_hash_changed",
                checkpoint=checkpoint,
            )
        checkpoint_artifact_sha256 = checkpoint.diagnostics.get("artifact_sha256")
        if self._require_artifact_checksum and not checkpoint_artifact_sha256:
            return PartitionResumeDecision(
                partition=partition,
                action="retry",
                reason="missing_artifact_checksum",
                checkpoint=checkpoint,
            )
        if (
            self._require_artifact_checksum
            and partition.artifact_sha256
            and checkpoint_artifact_sha256 != partition.artifact_sha256
        ):
            return PartitionResumeDecision(
                partition=partition,
                action="retry",
                reason="artifact_checksum_changed",
                checkpoint=checkpoint,
            )
        return PartitionResumeDecision(
            partition=partition,
            action="skip",
            reason="committed_matching_checkpoint",
            checkpoint=checkpoint,
        )


def _invalid_guard() -> NativeTransferTargetCommitGuardError:
    return NativeTransferTargetCommitGuardError("native_transfer_target_commit_guard_invalid")


def _is_sha256(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        return False
    digest = value.removeprefix("sha256:")
    return len(digest) == 64 and all(character in "0123456789abcdef" for character in digest)


def _is_bounded_strategy(value: object) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 128
        and all(character.isalnum() or character == "_" for character in value)
    )


__all__ = [
    "NativeTransferTargetCommitGuard",
    "NativeTransferTargetCommitGuardError",
    "PartitionResumeDecision",
    "PartitionResumePlan",
    "PartitionResumePlanner",
    "PlannedTransferPartition",
    "TARGET_COMMIT_GUARD_KEY",
    "build_native_transfer_checkpoint_batch_id",
]
