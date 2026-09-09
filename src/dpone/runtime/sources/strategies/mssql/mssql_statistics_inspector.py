"""SQL Server statistics adapter for snapshot partition planning."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.runtime.snapshot_partition_planner import HistogramStep, SourceStatistic


class MssqlStatisticsQueryBuilder:
    """Render metadata-only SQL Server histogram inspection SQL."""

    def histogram_query(self, *, database: str | None, schema: str, table: str, column: str) -> str:
        db_prefix = f"{database}." if database else ""
        database_literal = f"N'{database}'" if database else "DB_NAME()"
        return f"""
SELECT
    stat.name AS statistic_name,
    hist.step_number,
    CONVERT(nvarchar(4000), hist.range_high_key) AS range_high_key,
    hist.equal_rows,
    hist.range_rows,
    hist.distinct_range_rows,
    hist.average_range_rows
FROM {db_prefix}sys.stats AS stat
JOIN {db_prefix}sys.stats_columns AS stat_col
  ON stat.object_id = stat_col.object_id AND stat.stats_id = stat_col.stats_id
JOIN {db_prefix}sys.columns AS col
  ON col.object_id = stat_col.object_id AND col.column_id = stat_col.column_id
CROSS APPLY sys.dm_db_stats_histogram(OBJECT_ID(N'{schema}.{table}'), stat.stats_id) AS hist
WHERE stat.object_id = OBJECT_ID(N'{schema}.{table}')
  AND DB_ID({database_literal}) IS NOT NULL
  AND col.name = N'{column}'
ORDER BY hist.step_number
""".strip()


class MssqlSourceStatisticsInspector:
    """Inspect SQL Server statistics through lightweight metadata queries."""

    def __init__(self, connector: Any, query_builder: MssqlStatisticsQueryBuilder | None = None) -> None:
        self._connector = connector
        self._query_builder = query_builder or MssqlStatisticsQueryBuilder()

    def inspect_histogram(
        self,
        *,
        database: str | None,
        schema: str,
        table: str,
        column: str,
        total_rows: int = 0,
        null_rows: int = 0,
        source_is_view: bool = False,
    ) -> SourceStatistic:
        rows = self._connector.get_records(
            self._query_builder.histogram_query(database=database, schema=schema, table=table, column=column),
            as_dict=True,
        )
        return SourceStatistic(
            source_type="mssql",
            table=f"{schema}.{table}",
            column=column,
            total_rows=int(total_rows or _estimated_rows(rows)),
            null_rows=int(null_rows or 0),
            confidence="high" if rows and not source_is_view else "low",
            source_is_view=source_is_view,
            steps=_steps(rows),
        )


def _steps(rows: list[Mapping[str, Any]]) -> tuple[HistogramStep, ...]:
    steps: list[HistogramStep] = []
    previous: Any | None = None
    for row in rows:
        high = _typed_key(_first(row, "range_high_key", "RANGE_HI_KEY"))
        steps.append(
            HistogramStep(
                lower_key=previous,
                range_hi_key=high,
                equal_rows=float(_first(row, "equal_rows", "EQ_ROWS") or 0),
                range_rows=float(_first(row, "range_rows", "RANGE_ROWS") or 0),
                distinct_range_rows=float(_first(row, "distinct_range_rows", "DISTINCT_RANGE_ROWS") or 0),
            )
        )
        previous = high
    return tuple(steps)


def _estimated_rows(rows: list[Mapping[str, Any]]) -> int:
    return int(
        sum(
            float(_first(row, "equal_rows", "EQ_ROWS") or 0) + float(_first(row, "range_rows", "RANGE_ROWS") or 0)
            for row in rows
        )
    )


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


def _typed_key(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value
    try:
        parsed = float(text)
    except ValueError:
        return value
    return int(parsed) if parsed.is_integer() else parsed


__all__ = ["MssqlSourceStatisticsInspector", "MssqlStatisticsQueryBuilder"]
