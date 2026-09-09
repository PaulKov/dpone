"""Logical quality snapshots for checkpointed native-transfer workloads."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint
    from dpone.runtime.lineage.partition_checkpoint_store import PartitionCheckpointStore
    from dpone.runtime.lineage.partition_resume import PartitionResumeDecision, PlannedTransferPartition


import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from dpone.governance.quality import QualityProbeSnapshot
from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpointStatus
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.native_transfer_plan_runtime import NativeTransferPlanRuntimeAdapter
from dpone.runtime.native_transfer_row_authority import (
    actual_export_rows,
    canonical_non_negative_int,
    slice_export_rows_by_key,
    sum_slice_export_rows,
    trusted_checkpoint_rows,
)

_SCOPE_KIND = "dpone.native_transfer.quality_scope.v1"


@dataclass(frozen=True, slots=True)
class NativeTransferQualityScopeSummary:
    """Bounded evidence for one logical native quality scope."""

    digest: str
    planned_count: int
    active_count: int
    skipped_committed_count: int
    reactivated_count: int
    source_rows: int | None
    target_rows: int | None
    kind: str = _SCOPE_KIND

    def to_jsonable(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "digest": self.digest,
            "planned_count": self.planned_count,
            "active_count": self.active_count,
            "skipped_committed_count": self.skipped_committed_count,
            "reactivated_count": self.reactivated_count,
            "source_rows": self.source_rows,
            "target_rows": self.target_rows,
        }


@dataclass(frozen=True, slots=True)
class NativeTransferQualitySnapshots:
    """Source and target probes plus their safe logical-scope evidence."""

    source: QualityProbeSnapshot
    target: QualityProbeSnapshot
    scope_summary: NativeTransferQualityScopeSummary

    @property
    def summary(self) -> NativeTransferQualityScopeSummary:
        """Compatibility-friendly shorthand for consumers rendering evidence."""

        return self.scope_summary


@dataclass(frozen=True, slots=True)
class NativeTransferQualityScope:
    """Aggregate planned native partitions exactly once for quality gates."""

    _planned: tuple[PlannedTransferPartition, ...] = field(repr=False)
    _decisions: tuple[PartitionResumeDecision, ...] = field(repr=False)
    _active_items: tuple[tuple[str, object], ...] = field(repr=False)
    _active_ids: frozenset[str] = field(repr=False)
    _skipped_ids: frozenset[str] = field(repr=False)
    _reactivated_ids: frozenset[str] = field(repr=False)
    _plan_artifact: Any | None = field(
        default=None,
        compare=False,
        repr=False,
    )
    _checkpoint_store: PartitionCheckpointStore | None = field(
        default=None,
        compare=False,
        repr=False,
    )

    @classmethod
    def from_context(cls, context: object) -> NativeTransferQualityScope:
        """Derive scope identity and attempt rows from one prepared native context."""

        resume_plan = getattr(context, "resume_plan", None)
        decisions = tuple(getattr(resume_plan, "decisions", ()) or ())
        planned = tuple(decision.partition for decision in decisions)
        planned_ids = tuple(partition.transfer_partition_id for partition in planned)
        if len(planned_ids) != len(set(planned_ids)):
            raise ValueError("native quality scope contains duplicate partition identities")
        skipped_ids = frozenset(
            decision.partition.transfer_partition_id for decision in decisions if decision.action == "skip"
        )
        reactivated_ids = frozenset(str(value) for value in (getattr(context, "reactivated_partition_ids", ()) or ()))
        retry_ids = {decision.partition.transfer_partition_id for decision in decisions if decision.action == "retry"}
        active_ids = frozenset(retry_ids | set(reactivated_ids))
        if not reactivated_ids.issubset(skipped_ids) or not active_ids.issubset(planned_ids):
            raise ValueError("native quality scope partition sets are inconsistent")
        active_items = _active_partition_items(context)
        active_item_ids = tuple(partition_id for partition_id, _ in active_items)
        if len(active_item_ids) != len(active_ids) or set(active_item_ids) != set(active_ids):
            active_items = tuple((partition_id, None) for partition_id in sorted(active_ids))
        payload = getattr(context, "payload", None)
        artifact = getattr(payload, "artifact", None)
        plan_artifact = artifact if _has_slice_evidence(artifact) else None
        return cls(
            _planned=planned,
            _decisions=decisions,
            _active_items=active_items,
            _active_ids=active_ids,
            _skipped_ids=skipped_ids,
            _reactivated_ids=reactivated_ids,
            _plan_artifact=plan_artifact,
            _checkpoint_store=getattr(context, "checkpoint_store", None),
        )

    @classmethod
    def from_slice_evidence_artifact(cls, artifact: object) -> NativeTransferQualityScope | None:
        """Build a logical scope for lazy exports that publish per-slice evidence."""

        if not _has_slice_evidence(artifact):
            return None
        return cls(
            _planned=(),
            _decisions=(),
            _active_items=(),
            _active_ids=frozenset(),
            _skipped_ids=frozenset(),
            _reactivated_ids=frozenset(),
            _plan_artifact=artifact,
            _checkpoint_store=None,
        )

    def projected_snapshots(self, staged_rows: object) -> NativeTransferQualitySnapshots:
        """Project pre-finalize rows from active work plus non-reactivated skips."""

        committed_source, committed_target = self._skipped_committed_counts()
        if not self._active_ids:
            active_source = (
                sum_slice_export_rows(getattr(self._plan_artifact, "slice_evidence", ()))
                if self._plan_artifact is not None
                else None
            )
        else:
            active_source = _sum_partition_rows(
                self._active_ids,
                dict(_active_source_rows(self._active_items, self._plan_artifact)),
            )
        source_rows = _add_available(committed_source, active_source)
        target_rows = _add_available(
            committed_target,
            canonical_non_negative_int(staged_rows),
        )
        return self._snapshots(source_rows=source_rows, target_rows=target_rows)

    def resume_snapshots(self) -> NativeTransferQualitySnapshots:
        """Validate all planned rows from matching committed skip decisions."""

        source_rows, target_rows = self._decision_committed_counts()
        return self._snapshots(source_rows=source_rows, target_rows=target_rows)

    def committed_snapshots(
        self,
        checkpoint_store: PartitionCheckpointStore | None = None,
    ) -> NativeTransferQualitySnapshots:
        """Read the latest matching committed count for every planned partition."""

        store = checkpoint_store or self._checkpoint_store
        source_rows, target_rows = self._latest_committed_counts(store)
        return self._snapshots(source_rows=source_rows, target_rows=target_rows)

    def _skipped_committed_counts(self) -> tuple[int | None, int | None]:
        planned_by_id = {partition.transfer_partition_id: partition for partition in self._planned}
        decisions_by_id = {decision.partition.transfer_partition_id: decision for decision in self._decisions}
        source_values: list[object] = []
        target_values: list[object] = []
        for partition_id in sorted(self._skipped_ids - self._reactivated_ids):
            decision = decisions_by_id.get(partition_id)
            planned = planned_by_id.get(partition_id)
            checkpoint = decision.checkpoint if decision is not None else None
            if planned is None or not _matching_committed(checkpoint, planned):
                return None, None
            assert checkpoint is not None
            source_rows, target_rows = trusted_checkpoint_rows(checkpoint)
            if source_rows is None or target_rows is None:
                return None, None
            source_values.append(source_rows)
            target_values.append(target_rows)
        return _sum_values(source_values), _sum_values(target_values)

    def _decision_committed_counts(self) -> tuple[int | None, int | None]:
        decisions_by_id = {decision.partition.transfer_partition_id: decision for decision in self._decisions}
        source_values: list[object] = []
        target_values: list[object] = []
        for planned in self._planned:
            decision = decisions_by_id.get(planned.transfer_partition_id)
            checkpoint = decision.checkpoint if decision is not None else None
            if decision is None or decision.action != "skip" or not _matching_committed(checkpoint, planned):
                return None, None
            assert checkpoint is not None
            source_rows, target_rows = trusted_checkpoint_rows(checkpoint)
            if source_rows is None or target_rows is None:
                return None, None
            source_values.append(source_rows)
            target_values.append(target_rows)
        return _sum_values(source_values), _sum_values(target_values)

    def _latest_committed_counts(
        self,
        checkpoint_store: PartitionCheckpointStore | None,
    ) -> tuple[int | None, int | None]:
        if checkpoint_store is None:
            return None, None
        latest = {checkpoint.transfer_partition_id: checkpoint for checkpoint in checkpoint_store.list_latest()}
        source_values: list[object] = []
        target_values: list[object] = []
        for planned in self._planned:
            checkpoint = latest.get(planned.transfer_partition_id)
            if not _matching_committed(checkpoint, planned):
                return None, None
            assert checkpoint is not None
            source_rows, target_rows = trusted_checkpoint_rows(checkpoint)
            if source_rows is None or target_rows is None:
                return None, None
            source_values.append(source_rows)
            target_values.append(target_rows)
        return _sum_values(source_values), _sum_values(target_values)

    def _snapshots(
        self,
        *,
        source_rows: int | None,
        target_rows: int | None,
    ) -> NativeTransferQualitySnapshots:
        summary = NativeTransferQualityScopeSummary(
            digest=_scope_digest(
                planned=(partition.transfer_partition_id for partition in self._planned),
                active=self._active_ids,
                skipped=self._skipped_ids,
                reactivated=self._reactivated_ids,
            ),
            planned_count=len(self._planned),
            active_count=len(self._active_ids),
            skipped_committed_count=len(self._skipped_ids),
            reactivated_count=len(self._reactivated_ids),
            source_rows=source_rows,
            target_rows=target_rows,
        )
        return NativeTransferQualitySnapshots(
            source=QualityProbeSnapshot(row_count=source_rows, typed_hash=None),
            target=QualityProbeSnapshot(row_count=target_rows, typed_hash=None),
            scope_summary=summary,
        )


def _active_partition_items(context: object) -> tuple[tuple[str, object], ...]:
    active_artifacts = tuple(getattr(context, "active_artifacts", ()) or ())
    payload = getattr(context, "payload", None)
    artifact = getattr(payload, "artifact", None)
    if isinstance(artifact, PartitionedTransferPlanArtifact):
        planned = NativeTransferPlanRuntimeAdapter.planned_partitions(
            getattr(context, "load_config"),
            artifact,
        )
        return tuple(
            (partition.transfer_partition_id, item) for partition, item in zip(planned, active_artifacts, strict=False)
        )
    return tuple(
        (
            str(getattr(item, "transfer_partition_id", "")),
            item,
        )
        for item in active_artifacts
    )


def _active_source_rows(
    active_items: tuple[tuple[str, object], ...],
    plan_artifact: Any | None,
) -> tuple[tuple[str, object], ...]:
    if plan_artifact is None:
        return tuple(
            (partition_id, actual_export_rows(item) if item is not None else None)
            for partition_id, item in active_items
        )
    observed_rows = _observed_plan_source_rows(plan_artifact)
    if observed_rows is None:
        return tuple((partition_id, None) for partition_id, _ in active_items)
    rows: list[tuple[str, object]] = []
    for partition_id, item in active_items:
        partition_index = canonical_non_negative_int(getattr(item, "partition_index", None))
        slice_index = canonical_non_negative_int(getattr(item, "slice_index", None))
        key = (partition_index, slice_index) if partition_index is not None and slice_index is not None else None
        rows.append((partition_id, observed_rows.get(key) if key is not None else None))
    return tuple(rows)


def _observed_plan_source_rows(
    artifact: Any | None,
) -> dict[tuple[int, int], int] | None:
    if artifact is None or not _has_slice_evidence(artifact):
        return None
    return slice_export_rows_by_key(getattr(artifact, "slice_evidence", ()))


def has_slice_evidence(artifact: object | None) -> bool:
    return artifact is not None and hasattr(artifact, "slice_evidence")


def _has_slice_evidence(artifact: object | None) -> bool:
    return has_slice_evidence(artifact)


def _matching_committed(
    checkpoint: PartitionCheckpoint | None,
    planned: PlannedTransferPartition,
) -> bool:
    return bool(
        checkpoint is not None
        and checkpoint.status is PartitionCheckpointStatus.COMMITTED
        and checkpoint.transfer_partition_id == planned.transfer_partition_id
        and checkpoint.query_hash == planned.query_hash
        and checkpoint.schema_hash == planned.schema_hash
        and checkpoint.source_table == planned.source_table
        and checkpoint.target_table == planned.target_table
    )


def _sum_partition_rows(
    partition_ids: frozenset[str],
    rows_by_id: dict[str, object],
) -> int | None:
    if any(partition_id not in rows_by_id for partition_id in partition_ids):
        return None
    return _sum_values([rows_by_id[partition_id] for partition_id in sorted(partition_ids)])


def _sum_values(values: list[object]) -> int | None:
    if not values:
        return 0
    canonical = [canonical_non_negative_int(value) for value in values]
    if any(value is None for value in canonical):
        return None
    return sum(value for value in canonical if value is not None)


def _add_available(left: int | None, right: int | None) -> int | None:
    if left is None or right is None:
        return None
    return left + right


def _scope_digest(
    *,
    planned: Any,
    active: Any,
    skipped: Any,
    reactivated: Any,
) -> str:
    payload = {
        "planned": sorted(str(value) for value in planned),
        "active": sorted(str(value) for value in active),
        "skipped": sorted(str(value) for value in skipped),
        "reactivated": sorted(str(value) for value in reactivated),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


__all__ = [
    "NativeTransferQualityScope",
    "NativeTransferQualityScopeSummary",
    "NativeTransferQualitySnapshots",
    "has_slice_evidence",
]
