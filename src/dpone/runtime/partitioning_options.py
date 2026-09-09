"""Canonical partitioning option normalization.

This module is the only place that understands legacy partitioning aliases.
Runtime planners and source/sink adapters consume ``PartitioningOptions`` so
new integrations do not grow one-off worker or partition knobs.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class PartitionPlannerOptions:
    """Canonical statistics-aware partition planner options."""

    mode: str = "auto"
    stats_source: str = "auto"
    skew_policy: str = "split_hot_ranges"
    max_hot_partition_factor: float = 2.0
    min_partition_rows: int = 50_000
    boundary_type: str = "auto"
    bounds_role: str = "filter"
    temporal_granularity: str = "auto"
    null_bucket: str = "separate"
    low_confidence_policy: str = "conservative"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> PartitionPlannerOptions:
        raw = value if isinstance(value, Mapping) else {}
        return cls(
            mode=_choice(raw.get("mode"), {"auto", "statistics", "probe", "manual"}, "auto"),
            stats_source=_choice(raw.get("stats_source"), {"auto", "dmv", "dbcc", "sample"}, "auto"),
            skew_policy=_choice(raw.get("skew_policy"), {"split_hot_ranges", "warn", "fail_fast"}, "split_hot_ranges"),
            max_hot_partition_factor=float(raw.get("max_hot_partition_factor") or 2.0),
            min_partition_rows=int(raw.get("min_partition_rows") or 50_000),
            boundary_type=_choice(
                raw.get("boundary_type"),
                {"auto", "numeric", "date", "datetime", "datetime2", "datetimeoffset", "rowversion"},
                "auto",
            ),
            bounds_role=_choice(raw.get("bounds_role"), {"filter", "stride"}, "filter"),
            temporal_granularity=_choice(
                raw.get("temporal_granularity"),
                {"auto", "day", "hour", "minute", "second", "millisecond", "microsecond", "100ns"},
                "auto",
            ),
            null_bucket=_choice(raw.get("null_bucket"), {"separate", "include_first", "fail"}, "separate"),
            low_confidence_policy=_choice(
                raw.get("low_confidence_policy"),
                {"conservative", "fail_fast", "single_partition"},
                "conservative",
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "stats_source": self.stats_source,
            "skew_policy": self.skew_policy,
            "max_hot_partition_factor": self.max_hot_partition_factor,
            "min_partition_rows": self.min_partition_rows,
            "boundary_type": self.boundary_type,
            "bounds_role": self.bounds_role,
            "temporal_granularity": self.temporal_granularity,
            "null_bucket": self.null_bucket,
            "low_confidence_policy": self.low_confidence_policy,
        }


@dataclass(frozen=True, slots=True)
class PartitioningOptions:
    """Canonical partitioning options used by transfer/runtime planners."""

    column: str | None
    strategy: str
    bounds: Any = None
    lower_bound: Any = None
    upper_bound: Any = None
    num_partitions: int = 1
    max_partitions: int = 1
    target_rows_per_partition: int | None = None
    planner: PartitionPlannerOptions = field(default_factory=PartitionPlannerOptions)
    export_workers: int = 1
    load_workers: int = 1
    deprecated_aliases: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def bounds_mode(self) -> str:
        return "auto" if self.bounds == "auto" else "manual"


class PartitioningOptionsResolver:
    """Normalize partitioning options and isolate legacy alias handling."""

    @classmethod
    def resolve(cls, options: Mapping[str, Any] | None) -> PartitioningOptions:
        raw = dict(options or {})
        nested = raw.get("partitioning") if isinstance(raw.get("partitioning"), Mapping) else {}
        nested = dict(nested or {})
        deprecated: list[str] = []

        column = nested.get("column")
        if column is None and raw.get("partition_column") is not None:
            column = raw.get("partition_column")
            deprecated.append("source.options.partition_column")

        bounds = nested.get("bounds")
        lower_bound = None
        upper_bound = None
        if isinstance(bounds, Mapping):
            lower_bound = bounds.get("lower")
            upper_bound = bounds.get("upper")
        elif bounds != "auto":
            lower_bound = _legacy_first(raw, deprecated, "lower_bound", "partition_lower_bound")
            upper_bound = _legacy_first(raw, deprecated, "upper_bound", "partition_upper_bound")

        num_partitions = _int_first(
            nested,
            raw,
            deprecated,
            canonical_key="num_partitions",
            aliases=("num_partitions", "partition_count"),
            default=1,
        )
        max_partitions = int(nested.get("max_partitions") or num_partitions or 1)
        target_rows = nested.get("target_rows_per_partition")
        planner = PartitionPlannerOptions.from_mapping(
            nested.get("planner") if isinstance(nested.get("planner"), Mapping) else {}
        )
        export_workers = _int_first(
            nested,
            raw,
            deprecated,
            canonical_key="export_workers",
            aliases=("partition_workers",),
            default=min(max(1, num_partitions), os.cpu_count() or 4),
        )
        load_workers = _int_first(
            nested,
            raw,
            deprecated,
            canonical_key="load_workers",
            aliases=("parallel_load_workers", "partition_load_workers"),
            default=export_workers,
        )

        warnings = tuple(
            f"{alias} is deprecated; use source.options.partitioning.{_canonical_name(alias)}." for alias in deprecated
        )
        return PartitioningOptions(
            column=str(column) if column else None,
            strategy=str(nested.get("strategy") or "range"),
            bounds=bounds,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=max(1, int(num_partitions or 1)),
            max_partitions=max(1, int(max_partitions or 1)),
            target_rows_per_partition=int(target_rows) if target_rows is not None else None,
            planner=planner,
            export_workers=max(1, int(export_workers or 1)),
            load_workers=max(1, int(load_workers or 1)),
            deprecated_aliases=tuple(deprecated),
            warnings=warnings,
        )


def _legacy_first(raw: Mapping[str, Any], deprecated: list[str], *aliases: str) -> Any:
    for alias in aliases:
        if raw.get(alias) is not None:
            deprecated.append(f"source.options.{alias}")
            return raw.get(alias)
    return None


def _int_first(
    nested: Mapping[str, Any],
    raw: Mapping[str, Any],
    deprecated: list[str],
    *,
    canonical_key: str,
    aliases: tuple[str, ...],
    default: int,
) -> int:
    if nested.get(canonical_key) is not None:
        return int(nested[canonical_key])
    for alias in aliases:
        if raw.get(alias) is not None:
            deprecated.append(f"source.options.{alias}")
            return int(raw[alias])
    return int(default)


def _canonical_name(alias: str) -> str:
    return {
        "source.options.partition_column": "column",
        "source.options.partition_workers": "export_workers",
        "source.options.parallel_load_workers": "load_workers",
        "source.options.partition_load_workers": "load_workers",
        "source.options.partition_lower_bound": "bounds.lower",
        "source.options.partition_upper_bound": "bounds.upper",
        "source.options.lower_bound": "bounds.lower",
        "source.options.upper_bound": "bounds.upper",
        "source.options.partition_count": "num_partitions",
    }.get(alias, alias.removeprefix("source.options."))


def _choice(value: Any, allowed: set[str], default: str) -> str:
    normalized = str(value or default).strip().lower()
    return normalized if normalized in allowed else default


__all__ = ["PartitionPlannerOptions", "PartitioningOptions", "PartitioningOptionsResolver"]
