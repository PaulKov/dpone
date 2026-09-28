"""Range partitioning helpers for high-throughput database transfers.

The helpers in this module intentionally know nothing about PostgreSQL,
SQL Server, or ClickHouse connectors. Sources use them to turn Spark-style
``partition_column`` options into deterministic half-open SQL ranges that can
be exported independently and retried independently.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import Any

from dpone.runtime.partitioning_bounds import (
    PartitionBoundaryResolution,
    PartitionBoundaryTypeResolver,
    PartitionBoundKind,
    compare_partition_bounds,
    normalize_partition_bound,
)
from dpone.runtime.partitioning_options import PartitioningOptions, PartitioningOptionsResolver
from dpone.runtime.partitioning_predicates import (
    DefaultPartitionPredicateRenderer,
    PartitionPredicateRenderer,
)


@dataclass(frozen=True)
class RangePartition:
    """One SQL range for a partitioned extract.

    All non-final partitions are half-open: ``[lower, upper)``. The final
    partition is closed on the upper side so a user-provided ``upper_bound`` is
    included exactly once.
    """

    index: int
    lower_bound: Any
    upper_bound: Any
    include_lower: bool = True
    include_upper: bool = False
    is_null_partition: bool = False
    include_nulls: bool = False
    boundary: PartitionBoundaryResolution = PartitionBoundaryResolution(PartitionBoundKind.NUMERIC)

    def predicate(self, quoted_column: str, *, renderer: PartitionPredicateRenderer | None = None) -> str:
        active_renderer = renderer or DefaultPartitionPredicateRenderer()
        return active_renderer.render(self, quoted_column)

    @staticmethod
    def _sql_literal(value: object) -> str:
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, int | float):
            return str(value)
        if isinstance(value, datetime):
            return f"'{value.isoformat(timespec='seconds')}'"
        if isinstance(value, date):
            return f"'{value.isoformat()}'"
        text = str(value).replace("'", "''")
        return f"'{text}'"


@dataclass(frozen=True)
class RangePartitioner:
    """Spark JDBC-style range partition planner."""

    column: str
    lower_bound: Any
    upper_bound: Any
    num_partitions: int
    max_workers: int
    load_workers: int
    strategy: str = "range"
    bounds_role: str = "filter"
    null_bucket: str = "separate"
    boundary: PartitionBoundaryResolution = PartitionBoundaryResolution(PartitionBoundKind.NUMERIC)
    target_rows_per_partition: int | None = None
    source_row_count: int | None = None
    null_count: int | None = None
    warnings: tuple[str, ...] = ()
    explicit_partitions: tuple[RangePartition, ...] = ()
    range_parallelism: object | None = None

    @classmethod
    def from_options(
        cls,
        options: dict[str, Any] | None,
        *,
        bounds_resolver: Callable[[str], tuple[Any, ...]] | None = None,
    ) -> RangePartitioner:
        resolved = PartitioningOptionsResolver.resolve(options)
        column = resolved.column
        target_rows = resolved.target_rows_per_partition
        num_partitions = resolved.num_partitions
        if not column or (
            not resolved.ranges and num_partitions <= 1 and not (resolved.bounds == "auto" and target_rows)
        ):
            return cls(
                "",
                0,
                0,
                1,
                1,
                1,
                warnings=resolved.warnings,
                range_parallelism=resolved.range_parallelism,
            )

        if resolved.ranges:
            return cls._from_explicit_ranges(resolved)

        max_partitions = resolved.max_partitions
        strategy = resolved.strategy
        row_count: int | None = None
        if resolved.bounds == "auto":
            if bounds_resolver is None:
                raise ValueError("partitioning.bounds=auto requires a bounds resolver.")
            lower, upper, row_count, null_count = _unpack_bounds(bounds_resolver(str(column)))
            if target_rows:
                calculated = math.ceil((row_count or 0) / int(target_rows)) if row_count else 1
                num_partitions = max(1, min(max_partitions, calculated))
        else:
            lower = resolved.lower_bound
            upper = resolved.upper_bound
            null_count = None
        if lower is None or upper is None:
            raise ValueError(
                "Partitioned extract requires source.options.partitioning.bounds.lower and "
                "source.options.partitioning.bounds.upper when num_partitions > 1."
            )

        boundary = PartitionBoundaryTypeResolver.resolve(
            boundary_type=resolved.planner.boundary_type,
            sample_values=(lower, upper),
        )
        if boundary.kind == PartitionBoundKind.UUID:
            raise ValueError(
                "UUID/uniqueidentifier partitioning requires explicit ranges; automatic splitting is unsupported."
            )
        lower_bound = _normalize_bound(lower, strategy=strategy, boundary=boundary)
        upper_bound = _normalize_bound(upper, strategy=strategy, boundary=boundary)
        if upper_bound < lower_bound:
            raise ValueError("Partitioned extract upper_bound must be >= lower_bound.")

        workers = resolved.export_workers
        load_workers = resolved.load_workers
        return cls(
            column=str(column),
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
            max_workers=max(1, min(workers, num_partitions)),
            load_workers=max(1, min(load_workers, num_partitions)),
            strategy=strategy,
            bounds_role=resolved.planner.bounds_role,
            null_bucket=resolved.planner.null_bucket,
            boundary=boundary,
            target_rows_per_partition=int(target_rows) if target_rows else None,
            source_row_count=row_count,
            null_count=null_count,
            warnings=resolved.warnings,
            range_parallelism=resolved.range_parallelism,
        )

    @classmethod
    def _from_explicit_ranges(cls, resolved: PartitioningOptions) -> RangePartitioner:
        ranges = resolved.ranges
        samples = tuple(
            value
            for item in ranges
            if not bool(item.get("null"))
            for value in (item.get("lower"), item.get("upper"))
            if value is not None
        )
        boundary = PartitionBoundaryTypeResolver.resolve(
            boundary_type=resolved.planner.boundary_type,
            sample_values=samples,
        )
        partitions: list[RangePartition] = []
        null_seen = False
        for item in ranges:
            if bool(item.get("null")):
                if null_seen:
                    raise ValueError("Explicit ranges may declare the NULL bucket only once.")
                null_seen = True
                if item.get("lower") is None and item.get("upper") is None:
                    continue
            if item.get("lower") is None or item.get("upper") is None:
                raise ValueError("Every explicit non-NULL range requires typed lower and upper bounds.")
            lower = normalize_partition_bound(item["lower"], boundary)
            upper = normalize_partition_bound(item["upper"], boundary)
            include_lower = bool(item.get("include_lower", True))
            include_upper = bool(item.get("include_upper", False))
            comparison = compare_partition_bounds(lower, upper, boundary)
            if comparison > 0 or (comparison == 0 and not (include_lower and include_upper)):
                raise ValueError("Explicit range lower bound must precede its upper bound using source semantics.")
            partitions.append(
                RangePartition(
                    index=len(partitions),
                    lower_bound=lower,
                    upper_bound=upper,
                    include_lower=include_lower,
                    include_upper=include_upper,
                    boundary=boundary,
                )
            )
        if not partitions:
            raise ValueError("Explicit partitioning requires at least one non-NULL range.")
        _validate_explicit_coverage(partitions, gap_policy=resolved.range_parallelism.gap_policy)
        if null_seen and resolved.planner.null_bucket == "fail":
            raise ValueError(f"Partition column {resolved.column!r} declares a NULL range but null_bucket=fail.")
        if null_seen and resolved.planner.null_bucket == "include_first":
            partitions[0] = replace(partitions[0], include_nulls=True)
        elif null_seen:
            partitions = [
                RangePartition(0, None, None, is_null_partition=True, boundary=boundary),
                *[replace(item, index=item.index + 1) for item in partitions],
            ]
        workers = min(resolved.export_workers, len(partitions))
        load_workers = min(resolved.load_workers, len(partitions))
        return cls(
            column=str(resolved.column),
            lower_bound=partitions[0].lower_bound,
            upper_bound=partitions[-1].upper_bound,
            num_partitions=len(partitions),
            max_workers=max(1, workers),
            load_workers=max(1, load_workers),
            strategy=resolved.strategy,
            bounds_role=resolved.planner.bounds_role,
            null_bucket=resolved.planner.null_bucket,
            boundary=boundary,
            warnings=resolved.warnings,
            explicit_partitions=tuple(partitions),
            range_parallelism=resolved.range_parallelism,
        )

    @property
    def enabled(self) -> bool:
        return bool(self.column) and (self.num_partitions > 1 or bool(self.explicit_partitions))

    def partitions(self) -> list[RangePartition]:
        if not self.enabled:
            return []
        if self.explicit_partitions:
            return list(self.explicit_partitions)
        partitions = self._core_partitions()
        if self.bounds_role == "stride":
            partitions = self._with_spark_stride_edges(partitions)
        partitions = self._with_null_partition(partitions)
        return partitions

    def _core_partitions(self) -> list[RangePartition]:
        if self.boundary.kind == PartitionBoundKind.DATE:
            return self._date_partitions()
        if self.boundary.kind in {PartitionBoundKind.DATETIME, PartitionBoundKind.DATETIME_OFFSET}:
            return self._datetime_partitions()
        span = self.upper_bound - self.lower_bound
        step = max(1, math.ceil(span / self.num_partitions))
        partitions: list[RangePartition] = []
        lower = self.lower_bound
        for index in range(self.num_partitions):
            upper = min(self.upper_bound, lower + step)
            include_upper = index == self.num_partitions - 1
            partitions.append(
                RangePartition(
                    index=index,
                    lower_bound=lower,
                    upper_bound=self.upper_bound if include_upper else upper,
                    include_upper=include_upper,
                    boundary=self.boundary,
                )
            )
            lower = upper
            if include_upper:
                break
        return partitions

    def _date_partitions(self) -> list[RangePartition]:
        span = (self.upper_bound - self.lower_bound).days
        step = max(1, math.ceil(span / self.num_partitions))
        partitions: list[RangePartition] = []
        lower = self.lower_bound
        for index in range(self.num_partitions):
            upper = min(self.upper_bound, lower + timedelta(days=step))
            include_upper = index == self.num_partitions - 1 or upper >= self.upper_bound
            partitions.append(
                RangePartition(
                    index=index,
                    lower_bound=lower,
                    upper_bound=self.upper_bound if include_upper else upper,
                    include_upper=include_upper,
                    boundary=self.boundary,
                )
            )
            lower = upper
            if include_upper:
                break
        return partitions

    def _datetime_partitions(self) -> list[RangePartition]:
        span = self.upper_bound - self.lower_bound
        total_microseconds = max(1, math.ceil(span.total_seconds() * 1_000_000))
        step = timedelta(microseconds=max(1, math.ceil(total_microseconds / self.num_partitions)))
        partitions: list[RangePartition] = []
        lower = self.lower_bound
        for index in range(self.num_partitions):
            upper = min(self.upper_bound, lower + step)
            include_upper = index == self.num_partitions - 1 or upper >= self.upper_bound
            partitions.append(
                RangePartition(
                    index=index,
                    lower_bound=lower,
                    upper_bound=self.upper_bound if include_upper else upper,
                    include_upper=include_upper,
                    boundary=self.boundary,
                )
            )
            lower = upper
            if include_upper:
                break
        return partitions

    def _with_spark_stride_edges(self, partitions: list[RangePartition]) -> list[RangePartition]:
        if not partitions:
            return partitions
        if len(partitions) == 1:
            return [replace(partitions[0], lower_bound=None, upper_bound=None, include_upper=False)]
        adjusted = list(partitions)
        adjusted[0] = replace(adjusted[0], lower_bound=None)
        adjusted[-1] = replace(adjusted[-1], upper_bound=None, include_upper=False)
        return adjusted

    def _with_null_partition(self, partitions: list[RangePartition]) -> list[RangePartition]:
        if not self.null_count:
            return partitions
        if self.null_bucket == "fail":
            raise ValueError(f"Partition column {self.column!r} contains NULL values.")
        if self.null_bucket == "include_first":
            return [replace(partitions[0], include_nulls=True), *partitions[1:]]
        if self.null_bucket != "separate":
            return partitions
        null_partition = RangePartition(
            index=0,
            lower_bound=None,
            upper_bound=None,
            is_null_partition=True,
            boundary=self.boundary,
        )
        return [null_partition, *[replace(partition, index=partition.index + 1) for partition in partitions]]

    def wrap_query(
        self,
        base_query: str,
        quoted_column: str,
        partition: RangePartition,
        *,
        renderer: PartitionPredicateRenderer | None = None,
    ) -> str:
        """Wrap a source query with a partition predicate."""

        return f"SELECT * FROM ({base_query}) AS dpone_partitioned WHERE {partition.predicate(quoted_column, renderer=renderer)}"


def _normalize_bound(value: Any, *, strategy: str, boundary: PartitionBoundaryResolution | None = None) -> Any:
    if strategy in {"time_window", "datetime"}:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return normalize_partition_bound(value, boundary or PartitionBoundaryResolution(PartitionBoundKind.NUMERIC))


def _unpack_bounds(value: tuple[Any, ...]) -> tuple[Any, Any, int | None, int | None]:
    lower, upper, row_count, *rest = value
    null_count = rest[0] if rest else None
    return (
        lower,
        upper,
        int(row_count) if row_count is not None else None,
        int(null_count) if null_count is not None else None,
    )


def _validate_explicit_coverage(partitions: list[RangePartition], *, gap_policy: str) -> None:
    for previous, current in zip(partitions, partitions[1:], strict=False):
        comparison = compare_partition_bounds(previous.upper_bound, current.lower_bound, previous.boundary)
        overlapping_boundary = comparison == 0 and previous.include_upper and current.include_lower
        missing_boundary = comparison == 0 and not previous.include_upper and not current.include_lower
        if comparison > 0 or overlapping_boundary:
            raise ValueError("Explicit partition ranges overlap under source comparison semantics.")
        if (comparison < 0 or missing_boundary) and gap_policy != "allow_explicit":
            raise ValueError("Explicit partition ranges contain a gap; set gap_policy=allow_explicit to accept it.")
