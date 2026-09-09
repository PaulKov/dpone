"""Pure construction of canonical portable scopes for backfill chunks.

The planner owns values and boundary semantics, never rendered SQL.  Runtime
catalog admission later binds each exact scope to the PostgreSQL source and
SQL Server target before either dialect renders its parameterized predicate.
"""

from __future__ import annotations

from datetime import datetime

from dpone.backfill.models import BackfillChunkSpec, parse_date_boundary, parse_timestamp_boundary
from dpone.contracts.portable_relation_scope import (
    PortableLiteral,
    PortableRangeBound,
    PortableRangeScope,
)


def build_backfill_chunk_scope(
    spec: BackfillChunkSpec,
    *,
    start: str,
    end: str,
) -> PortableRangeScope:
    """Return the immutable, render-neutral range owned by one chunk."""

    upper_inclusive = spec.kind == "integer"
    if spec.kind == "integer":
        lower_literal = PortableLiteral("integer", int(start))
        upper_literal = PortableLiteral("integer", int(end))
    elif spec.kind == "date":
        lower_literal = PortableLiteral("date", parse_date_boundary(start))
        upper_literal = PortableLiteral("date", parse_date_boundary(end))
    else:
        lower_value = parse_timestamp_boundary(start)
        upper_value = parse_timestamp_boundary(end)
        if _timestamp_literal_kind(lower_value, upper_value) == "timestamptz":
            lower_literal = PortableLiteral("timestamptz", lower_value)
            upper_literal = PortableLiteral("timestamptz", upper_value)
        else:
            lower_literal = PortableLiteral("timestamp", lower_value)
            upper_literal = PortableLiteral("timestamp", upper_value)
    return PortableRangeScope(
        column=spec.column,
        lower=PortableRangeBound(
            lower_literal,
            inclusive=True,
        ),
        upper=PortableRangeBound(
            upper_literal,
            inclusive=upper_inclusive,
        ),
    )


def _timestamp_literal_kind(lower: datetime, upper: datetime) -> str:
    lower_aware = lower.tzinfo is not None and lower.utcoffset() is not None
    upper_aware = upper.tzinfo is not None and upper.utcoffset() is not None
    if lower_aware != upper_aware:
        raise ValueError("backfill.chunk timestamp boundaries must use the same timezone form")
    return "timestamptz" if lower_aware else "timestamp"


__all__ = ["build_backfill_chunk_scope"]
