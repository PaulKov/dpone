"""Read-only SQL aggregate queries over already validated window identifiers.

This module owns neither connections nor publication decisions. The caller
supplies quoted identifiers and the half-open predicate from its schema policy.
Histogram memory scales with calendar days; it is not a constant-memory claim.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import timezone
from typing import Any, Protocol


class WindowQueryReader(Protocol):
    """Only the two read operations needed by aggregate verification."""

    def get_records(self, query: str, *, as_dict: bool = False) -> list[Any]: ...
    def get_records_iterator(self, query: str) -> Iterator[Any]: ...


def read_window_metrics(
    reader: WindowQueryReader,
    *,
    table_sql: str,
    column_sql: str,
    predicate_sql: str,
    columns: Sequence[tuple[str, str]],
    window_only: bool,
) -> dict[str, Any]:
    """Read counts/bounds/NULLs, then ordered UTC days, closing the day cursor.

    ``columns`` pairs output names with their validated SQL identifier forms.
    SQL fragments are internal inputs, never unchecked user-authored SQL.
    """
    column = column_sql
    predicate = predicate_sql
    where = f" WHERE {predicate}" if window_only else ""
    null_sql = ", ".join(f"countIf(isNull({quoted}))" for _, quoted in columns)
    aggregate = reader.get_records(
        f"SELECT count(), minOrNull({column}), maxOrNull({column}), "
        f"countIf(isNull({column}) OR NOT {predicate}), {null_sql} "
        f"FROM {table_sql}{where}"
    )[0]

    def timestamp(value: Any) -> str | None:
        if value is None:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)  # noqa: UP017
        return value.isoformat()

    day_where = where + (" AND " if where else " WHERE ") + f"isNotNull({column})"
    day_rows = reader.get_records_iterator(
        f"SELECT formatDateTime({column}, '%Y-%m-%d', 'UTC'), count() FROM {table_sql}"
        f"{day_where} GROUP BY formatDateTime({column}, '%Y-%m-%d', 'UTC') "
        f"ORDER BY formatDateTime({column}, '%Y-%m-%d', 'UTC')"
    )
    try:
        days = {str(day): int(count) for day, count in day_rows}
    finally:
        close = getattr(day_rows, "close", None)
        if close:
            close()
    return {
        "row_count": int(aggregate[0]),
        "min_window_utc": timestamp(aggregate[1]),
        "max_window_utc": timestamp(aggregate[2]),
        "outside_window": int(aggregate[3]),
        "null_counts": {name: int(count) for (name, _), count in zip(columns, aggregate[4:], strict=True)},
        "utc_day_counts": days,
    }
