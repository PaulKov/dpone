"""Adaptive partitioning recommendations for native transfers.

This module is deliberately pure and connector-free. It consumes runtime
observations produced by tools or run artifacts and returns recommendations
that source/sink adapters can apply on the next run.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class PartitionRuntimeObservation:
    """Measured runtime evidence for one transfer partition."""

    partition_id: str
    row_count: int
    bytes_count: int | None = None
    duration_seconds: float | None = None
    retry_count: int = 0
    status: str = "committed"

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> PartitionRuntimeObservation:
        return cls(
            partition_id=str(payload.get("partition_id") or payload.get("id") or ""),
            row_count=max(0, int(payload.get("row_count", 0) or 0)),
            bytes_count=_optional_int(payload.get("bytes_count") or payload.get("bytes")),
            duration_seconds=_optional_float(payload.get("duration_seconds") or payload.get("seconds")),
            retry_count=max(0, int(payload.get("retry_count", 0) or 0)),
            status=str(payload.get("status") or "committed"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AdaptivePartitioningOptions:
    """Policy knobs for next-run partition recommendations."""

    enabled: bool = False
    target_rows_per_partition: int | None = None
    max_partitions: int = 1
    export_workers: int = 1
    load_workers: int = 1
    max_skew_ratio: float = 3.0
    retry_split_factor: int = 2
    min_rows_per_partition: int = 1_000

    @classmethod
    def from_mapping(
        cls, payload: dict[str, Any] | None, *, defaults: dict[str, Any] | None = None
    ) -> AdaptivePartitioningOptions:
        raw = dict(payload or {})
        base = dict(defaults or {})
        return cls(
            enabled=_bool(raw.get("enabled", False)),
            target_rows_per_partition=_optional_int(
                raw.get("target_rows_per_partition") or base.get("target_rows_per_partition")
            ),
            max_partitions=max(1, int(raw.get("max_partitions") or base.get("max_partitions") or 1)),
            export_workers=max(1, int(raw.get("export_workers") or base.get("export_workers") or 1)),
            load_workers=max(1, int(raw.get("load_workers") or base.get("load_workers") or 1)),
            max_skew_ratio=max(1.0, float(raw.get("max_skew_ratio", 3.0) or 3.0)),
            retry_split_factor=max(1, int(raw.get("retry_split_factor", 2) or 2)),
            min_rows_per_partition=max(1, int(raw.get("min_rows_per_partition", 1_000) or 1_000)),
        )


@dataclass(frozen=True, slots=True)
class AdaptivePartitioningPlan:
    """Explainable next-run partitioning recommendation."""

    enabled: bool
    recommended_target_rows_per_partition: int | None
    recommended_max_partitions: int
    recommended_export_workers: int
    recommended_load_workers: int
    actions: tuple[str, ...]
    actions_by_partition: dict[str, str]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "recommended_target_rows_per_partition": self.recommended_target_rows_per_partition,
            "recommended_max_partitions": self.recommended_max_partitions,
            "recommended_export_workers": self.recommended_export_workers,
            "recommended_load_workers": self.recommended_load_workers,
            "actions": list(self.actions),
            "actions_by_partition": dict(self.actions_by_partition),
            "warnings": list(self.warnings),
        }


class AdaptivePartitioningService:
    """Build adaptive partition recommendations from observed partitions."""

    def plan(
        self,
        *,
        options: AdaptivePartitioningOptions,
        observations: tuple[PartitionRuntimeObservation, ...],
    ) -> AdaptivePartitioningPlan:
        if not options.enabled:
            return AdaptivePartitioningPlan(
                enabled=False,
                recommended_target_rows_per_partition=options.target_rows_per_partition,
                recommended_max_partitions=options.max_partitions,
                recommended_export_workers=options.export_workers,
                recommended_load_workers=options.load_workers,
                actions=(),
                actions_by_partition={},
                warnings=(),
            )
        if not observations:
            return AdaptivePartitioningPlan(
                enabled=True,
                recommended_target_rows_per_partition=options.target_rows_per_partition,
                recommended_max_partitions=options.max_partitions,
                recommended_export_workers=options.export_workers,
                recommended_load_workers=options.load_workers,
                actions=("collect_partition_observability",),
                actions_by_partition={},
                warnings=("No partition runtime observations were provided; collecting observability only.",),
            )

        row_counts = [max(1, item.row_count) for item in observations]
        baseline_rows = max(1, min(row_counts))
        actions_by_partition: dict[str, str] = {}
        warnings: list[str] = []
        split_factor = 1
        for observation in observations:
            skew_ratio = observation.row_count / baseline_rows
            failed = observation.status.lower() in {"failed", "error"} or observation.retry_count > 0
            if failed:
                actions_by_partition[observation.partition_id] = "split_retry_partition"
                split_factor = max(split_factor, options.retry_split_factor)
            elif skew_ratio > options.max_skew_ratio:
                actions_by_partition[observation.partition_id] = "split_skewed_partition"
                split_factor = max(split_factor, 2)

        if any(action == "split_skewed_partition" for action in actions_by_partition.values()):
            warnings.append("Partition row-count skew exceeded threshold; split skewed partitions on next run.")
        if any(action == "split_retry_partition" for action in actions_by_partition.values()):
            warnings.append("Failed or retried partitions detected; split retry partitions on next run.")

        target_rows = options.target_rows_per_partition
        if target_rows and split_factor > 1:
            target_rows = max(options.min_rows_per_partition, int(target_rows / split_factor))
        actions = tuple(sorted(set(actions_by_partition.values())))
        return AdaptivePartitioningPlan(
            enabled=True,
            recommended_target_rows_per_partition=target_rows,
            recommended_max_partitions=options.max_partitions,
            recommended_export_workers=options.export_workers,
            recommended_load_workers=options.load_workers,
            actions=actions,
            actions_by_partition=actions_by_partition,
            warnings=tuple(warnings),
        )


def _optional_int(value: Any) -> int | None:
    return int(value) if value is not None else None


def _optional_float(value: Any) -> float | None:
    return float(value) if value is not None else None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


__all__ = [
    "AdaptivePartitioningOptions",
    "AdaptivePartitioningPlan",
    "AdaptivePartitioningService",
    "PartitionRuntimeObservation",
]
