"""Pure ClickHouse helpers: column-type metadata → fail-closed dict rows.

I/O-free so buffered and streaming extract paths share one mapping contract.
An empty column-name list would zip every tuple to ``{}`` and silently load
NULL business columns downstream while lineage may still attach.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any


def column_names_from_types(column_types: Sequence[Any]) -> list[str]:
    """Extract ordered column names from ``with_column_types`` metadata."""

    names = [str(entry[0]) for entry in column_types]
    if not names:
        raise RuntimeError("ClickHouse result mapping could not resolve column names")
    return names


def row_as_dict(column_names: Sequence[str], row: Sequence[Any]) -> dict[str, Any]:
    """Zip one row to a dict; fail closed when arity drifts."""

    try:
        return dict(zip(column_names, row, strict=True))
    except ValueError as exc:
        raise RuntimeError(
            "ClickHouse result mapping row arity mismatch: "
            f"expected {len(column_names)} columns {list(column_names)!r}, "
            f"got {len(row)} values"
        ) from exc


def rows_as_dicts(
    column_names: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> list[dict[str, Any]]:
    """Map an iterable of tuples to dict rows with shared column names."""

    return [row_as_dict(column_names, row) for row in rows]


__all__ = [
    "column_names_from_types",
    "row_as_dict",
    "rows_as_dicts",
]
