"""SQL predicate renderers for typed transfer partitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol

from dpone.runtime.partitioning_bounds import (
    PartitionBoundaryResolution,
    PartitionBoundKind,
    format_datetime_literal,
    format_rowversion_literal,
)


class PartitionLike(Protocol):
    @property
    def lower_bound(self) -> object: ...

    @property
    def upper_bound(self) -> object: ...

    @property
    def include_lower(self) -> bool: ...

    @property
    def include_upper(self) -> bool: ...

    @property
    def is_null_partition(self) -> bool: ...

    @property
    def boundary(self) -> PartitionBoundaryResolution: ...


class PartitionPredicateRenderer(Protocol):
    """Source-specific partition predicate renderer."""

    def render(self, partition: PartitionLike, quoted_column: str) -> str:
        """Render a SQL predicate for one partition."""


@dataclass(frozen=True, slots=True)
class DefaultPartitionPredicateRenderer:
    """Portable ANSI-ish predicate renderer used by generic callers."""

    def render(self, partition: PartitionLike, quoted_column: str) -> str:
        if partition.is_null_partition:
            return f"{quoted_column} IS NULL"
        clauses: list[str] = []
        if partition.lower_bound is not None:
            clauses.append(
                f"{quoted_column} {'>=' if partition.include_lower else '>'} "
                f"{self.literal(partition.lower_bound, partition.boundary)}"
            )
        if partition.upper_bound is not None:
            clauses.append(
                f"{quoted_column} {'<=' if partition.include_upper else '<'} "
                f"{self.literal(partition.upper_bound, partition.boundary)}"
            )
        if not clauses:
            return "1 = 1"
        return " AND ".join(clauses)

    def literal(self, value: object, boundary: PartitionBoundaryResolution) -> str:
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, int | float | Decimal):
            return str(value)
        if isinstance(value, datetime):
            return f"'{format_datetime_literal(value, scale=boundary.scale)}'"
        if isinstance(value, date):
            return f"'{value.isoformat()}'"
        text = str(value).replace("'", "''")
        return f"'{text}'"


@dataclass(frozen=True, slots=True)
class MssqlPartitionPredicateRenderer(DefaultPartitionPredicateRenderer):
    """MSSQL renderer that keeps functions on typed literals, never columns."""

    def literal(self, value: object, boundary: PartitionBoundaryResolution) -> str:
        if boundary.kind == PartitionBoundKind.DATE and isinstance(value, date):
            return f"CONVERT(date, '{value.isoformat()}', 23)"
        if boundary.kind == PartitionBoundKind.DATETIME and isinstance(value, datetime):
            dtype = _mssql_temporal_type(boundary)
            return f"CONVERT({dtype}, '{format_datetime_literal(value, scale=boundary.scale)}', 126)"
        if boundary.kind == PartitionBoundKind.DATETIME_OFFSET and isinstance(value, datetime):
            dtype = _mssql_datetimeoffset_type(boundary)
            rendered = value.isoformat()
            return f"CONVERT({dtype}, '{rendered}', 127)"
        if boundary.kind == PartitionBoundKind.ROWVERSION:
            return format_rowversion_literal(value)
        return DefaultPartitionPredicateRenderer.literal(self, value, boundary)


@dataclass(frozen=True, slots=True)
class PostgresPartitionPredicateRenderer(DefaultPartitionPredicateRenderer):
    """PostgreSQL renderer for typed partition predicates."""

    def literal(self, value: object, boundary: PartitionBoundaryResolution) -> str:
        if boundary.kind == PartitionBoundKind.DATE and isinstance(value, date):
            return f"DATE '{value.isoformat()}'"
        if boundary.kind == PartitionBoundKind.DATETIME and isinstance(value, datetime):
            return f"TIMESTAMP '{format_datetime_literal(value, scale=boundary.scale)}'"
        if boundary.kind == PartitionBoundKind.DATETIME_OFFSET and isinstance(value, datetime):
            return f"TIMESTAMPTZ '{value.isoformat()}'"
        return DefaultPartitionPredicateRenderer.literal(self, value, boundary)


def _mssql_temporal_type(boundary: PartitionBoundaryResolution) -> str:
    source = boundary.source_type
    if source.startswith("datetime2") or not source or source == "timestamp":
        return f"datetime2({boundary.scale if boundary.scale is not None else 7})"
    if source == "smalldatetime":
        return "smalldatetime"
    if source == "datetime":
        return "datetime"
    return source


def _mssql_datetimeoffset_type(boundary: PartitionBoundaryResolution) -> str:
    return f"datetimeoffset({boundary.scale if boundary.scale is not None else 7})"
