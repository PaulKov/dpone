"""Statistics-aware snapshot partition planning.

The planner consumes source statistics and emits logical partitions. Source
adapters own metadata collection; the planner itself is connector-neutral.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any

SNAPSHOT_PARTITION_PLAN_SCHEMA_VERSION = "dpone.native_transfer.snapshot_partition_plan.v1"


@dataclass(frozen=True, slots=True)
class HistogramStep:
    """One source histogram step normalized across database engines."""

    range_hi_key: Any
    equal_rows: float
    range_rows: float
    distinct_range_rows: float | None = None
    lower_key: Any | None = None

    @property
    def estimated_rows(self) -> float:
        return max(0.0, float(self.equal_rows or 0) + float(self.range_rows or 0))


@dataclass(frozen=True, slots=True)
class SourceStatistic:
    """Statistics snapshot for one potential boundary column."""

    source_type: str
    table: str
    column: str
    total_rows: int
    steps: Sequence[HistogramStep]
    null_rows: int = 0
    confidence: str = "low"
    source_is_view: bool = False


@dataclass(frozen=True, slots=True)
class SnapshotPartitionPlannerPolicy:
    """User-facing statistics partition planning policy."""

    mode: str = "auto"
    stats_source: str = "auto"
    skew_policy: str = "split_hot_ranges"
    max_hot_partition_factor: float = 2.0
    min_partition_rows: int = 50_000
    target_rows: int | None = None
    max_partitions: int = 16
    null_bucket: str = "separate"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> SnapshotPartitionPlannerPolicy:
        raw = value if isinstance(value, Mapping) else {}
        return cls(
            mode=str(raw.get("mode") or "auto").strip().lower(),
            stats_source=str(raw.get("stats_source") or "auto").strip().lower(),
            skew_policy=str(raw.get("skew_policy") or "split_hot_ranges").strip().lower(),
            max_hot_partition_factor=float(raw.get("max_hot_partition_factor") or 2.0),
            min_partition_rows=int(raw.get("min_partition_rows") or 50_000),
            target_rows=int(raw["target_rows"]) if raw.get("target_rows") is not None else None,
            max_partitions=max(1, int(raw.get("max_partitions") or 16)),
            null_bucket=str(raw.get("null_bucket") or "separate").strip().lower(),
        )


@dataclass(frozen=True, slots=True)
class SnapshotPartition:
    """One logical snapshot partition."""

    partition_id: str
    kind: str
    column: str
    lower_bound: Any | None
    upper_bound: Any | None
    estimated_rows: int
    include_lower: bool = True
    include_upper: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SnapshotPartitionPlan:
    """Machine-readable statistics partition evidence."""

    planner: str
    boundary_column: str
    stats_confidence: str
    partitions: tuple[SnapshotPartition, ...]
    max_parallel_exports: int
    warnings: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()

    def to_evidence(self) -> dict[str, Any]:
        return {
            "schema_version": SNAPSHOT_PARTITION_PLAN_SCHEMA_VERSION,
            "planner": self.planner,
            "boundary_column": self.boundary_column,
            "stats_confidence": self.stats_confidence,
            "max_parallel_exports": self.max_parallel_exports,
            "partitions": [partition.to_dict() for partition in self.partitions],
            "warnings": list(self.warnings),
            "blockers": list(self.blockers),
        }


class SnapshotPartitionPlanner:
    """Build balanced logical partitions from source statistics."""

    def plan(
        self,
        statistic: SourceStatistic,
        policy: SnapshotPartitionPlannerPolicy | None = None,
    ) -> SnapshotPartitionPlan:
        effective = policy or SnapshotPartitionPlannerPolicy()
        warnings = _base_warnings(statistic)
        if statistic.confidence != "high" or not statistic.steps:
            return SnapshotPartitionPlan(
                planner="statistics",
                boundary_column=statistic.column,
                stats_confidence=statistic.confidence,
                partitions=tuple(_null_partition(statistic, effective)),
                max_parallel_exports=1,
                warnings=tuple(warnings + ["source_stats_low_confidence"]),
            )

        target_rows = _target_rows(statistic, effective)
        partitions: list[SnapshotPartition] = []
        partitions.extend(_null_partition(statistic, effective))
        remaining = max(1, effective.max_partitions - len(partitions))
        range_partitions, range_warnings = _range_partitions(statistic, effective, target_rows, remaining)
        partitions.extend(range_partitions)
        warnings.extend(range_warnings)
        return SnapshotPartitionPlan(
            planner="statistics",
            boundary_column=statistic.column,
            stats_confidence=statistic.confidence,
            partitions=tuple(partitions),
            max_parallel_exports=max(1, min(len(partitions), effective.max_partitions)),
            warnings=tuple(dict.fromkeys(warnings)),
        )


def _base_warnings(statistic: SourceStatistic) -> list[str]:
    warnings: list[str] = []
    if statistic.source_is_view:
        warnings.append("source_stats_view_route")
    return warnings


def _null_partition(
    statistic: SourceStatistic,
    policy: SnapshotPartitionPlannerPolicy,
) -> list[SnapshotPartition]:
    if not statistic.null_rows or policy.null_bucket != "separate":
        return []
    return [
        SnapshotPartition(
            partition_id="null",
            kind="null",
            column=statistic.column,
            lower_bound=None,
            upper_bound=None,
            estimated_rows=int(statistic.null_rows),
        )
    ]


def _range_partitions(
    statistic: SourceStatistic,
    policy: SnapshotPartitionPlannerPolicy,
    target_rows: int,
    remaining: int,
) -> tuple[list[SnapshotPartition], list[str]]:
    warnings: list[str] = []
    partitions: list[SnapshotPartition] = []
    for step in statistic.steps:
        if len(partitions) >= remaining:
            break
        step_rows = int(math.ceil(step.estimated_rows))
        hot = step_rows > target_rows * policy.max_hot_partition_factor
        can_split = hot and policy.skew_policy == "split_hot_ranges" and _can_split(step)
        if can_split:
            warnings.append("snapshot_hot_range_split")
            partitions.extend(
                _split_hot_step(statistic.column, step, step_rows, target_rows, remaining - len(partitions))
            )
            continue
        if hot and policy.skew_policy == "fail_fast":
            warnings.append("snapshot_hot_range_blocked")
        partitions.append(_partition(statistic.column, len(partitions), step.lower_key, step.range_hi_key, step_rows))
    return partitions, warnings


def _split_hot_step(
    column: str,
    step: HistogramStep,
    step_rows: int,
    target_rows: int,
    remaining: int,
) -> list[SnapshotPartition]:
    count = max(1, min(remaining, math.ceil(step_rows / max(1, target_rows))))
    lower = step.lower_key
    upper = step.range_hi_key
    span = upper - lower
    width = span / count
    partitions: list[SnapshotPartition] = []
    for index in range(count):
        part_lower = lower + index * width
        part_upper = upper if index == count - 1 else lower + (index + 1) * width
        partitions.append(
            _partition(column, index, _normalize_number(part_lower), _normalize_number(part_upper), step_rows // count)
        )
    return partitions


def _partition(column: str, index: int, lower: Any, upper: Any, rows: int) -> SnapshotPartition:
    return SnapshotPartition(
        partition_id=f"range_{index:04d}",
        kind="range",
        column=column,
        lower_bound=lower,
        upper_bound=upper,
        estimated_rows=max(0, int(rows)),
    )


def _target_rows(statistic: SourceStatistic, policy: SnapshotPartitionPlannerPolicy) -> int:
    if policy.target_rows:
        return max(1, int(policy.target_rows))
    non_null_rows = max(1, int(statistic.total_rows) - int(statistic.null_rows or 0))
    calculated = math.ceil(non_null_rows / max(1, policy.max_partitions))
    return max(policy.min_partition_rows, calculated)


def _can_split(step: HistogramStep) -> bool:
    return (
        isinstance(step.lower_key, int | float)
        and isinstance(step.range_hi_key, int | float)
        and step.range_hi_key > step.lower_key
    )


def _normalize_number(value: int | float) -> int | float:
    return int(value) if float(value).is_integer() else value


__all__ = [
    "HistogramStep",
    "SNAPSHOT_PARTITION_PLAN_SCHEMA_VERSION",
    "SnapshotPartition",
    "SnapshotPartitionPlan",
    "SnapshotPartitionPlanner",
    "SnapshotPartitionPlannerPolicy",
    "SourceStatistic",
]
