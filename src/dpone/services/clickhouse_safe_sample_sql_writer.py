"""ClickHouse safe-sample SQL writer adapter.

The writer builds a secret-free insert plan for an already prepared temporary
ClickHouse target and delegates physical execution to an injected client. Rows
stay in memory and are intentionally omitted from the serializable plan view.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.services.safe_sample_execution_common import MssqlSafeSampleBatch, non_negative_int, redact_mapping


@dataclass(frozen=True, slots=True)
class ClickHouseSafeSampleInsertPlan:
    """Secret-free ClickHouse insert plan for a bounded safe-sample batch."""

    sql: str
    rows: tuple[Mapping[str, Any], ...]
    temporary_table: Mapping[str, str]
    sink: Mapping[str, Any]

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.clickhouse-safe-sample-insert-plan.v1",
            "sql": self.sql,
            "row_count": self.row_count,
            "temporary_table": dict(self.temporary_table),
            "sink": dict(self.sink),
        }


class ClickHouseSafeSampleSqlClient(Protocol):
    """Execute a secret-free ClickHouse safe-sample insert plan."""

    def insert(self, plan: ClickHouseSafeSampleInsertPlan) -> Mapping[str, Any]:
        """Return row count, byte count, and safe diagnostics for ``plan``."""


class ClickHouseSafeSampleSqlWriter:
    """Build a ClickHouse insert plan and delegate execution to an injected client."""

    def __init__(self, *, client: ClickHouseSafeSampleSqlClient) -> None:
        self._client = client

    def write(self, request: dict[str, Any], batch: MssqlSafeSampleBatch) -> Mapping[str, Any]:
        plan = _clickhouse_insert_plan(request, batch)
        result = dict(self._client.insert(plan))
        diagnostics = {
            "insert_template": "clickhouse_native_values_v1",
            "temporary_table": dict(plan.temporary_table),
        }
        client_diagnostics = redact_mapping(result.get("diagnostics"))
        if client_diagnostics:
            diagnostics["client"] = client_diagnostics
        return {
            "rows_written": non_negative_int(result.get("rows_written")),
            "bytes_written": non_negative_int(result.get("bytes_written")),
            "diagnostics": diagnostics,
        }


def _clickhouse_insert_plan(
    request: Mapping[str, Any],
    batch: MssqlSafeSampleBatch,
) -> ClickHouseSafeSampleInsertPlan:
    sink = _sink_section(request)
    if sink.get("type") != "clickhouse":
        raise ValueError("ClickHouse safe sample writer requires sink.type=clickhouse")
    temporary_table = _temporary_table(sink)
    if not temporary_table["name"]:
        raise ValueError("ClickHouse safe sample writer requires sink.temporary_table.name")
    columns = _insert_columns(batch.rows)
    column_clause = ""
    if columns:
        column_clause = " (" + ", ".join(_quote_clickhouse_identifier(column) for column in columns) + ")"
    return ClickHouseSafeSampleInsertPlan(
        sql=f"INSERT INTO {_clickhouse_table_name(temporary_table)}{column_clause} VALUES",
        rows=tuple(batch.rows),
        temporary_table=temporary_table,
        sink=sink,
    )


def _insert_columns(rows: tuple[Mapping[str, Any], ...]) -> tuple[str, ...]:
    if not rows:
        return ()
    columns = _row_columns(rows[0])
    if not columns:
        raise ValueError("ClickHouse safe sample rows must contain at least one column")
    expected = frozenset(columns)
    if any(frozenset(_row_columns(row)) != expected for row in rows[1:]):
        raise ValueError("ClickHouse safe sample rows must use the same column set")
    return tuple(sorted(columns))


def _row_columns(row: Mapping[str, Any]) -> tuple[str, ...]:
    if any(not isinstance(column, str) or not column for column in row):
        raise ValueError("ClickHouse safe sample row column names must be non-empty strings")
    return tuple(row)


def _sink_section(request: Mapping[str, Any]) -> dict[str, Any]:
    sink = request.get("sink")
    return dict(sink) if isinstance(sink, Mapping) else {}


def _temporary_table(sink: Mapping[str, Any]) -> dict[str, str]:
    table = sink.get("temporary_table")
    if not isinstance(table, Mapping):
        return {"schema": "", "name": ""}
    return {"schema": str(table.get("schema") or ""), "name": str(table.get("name") or "")}


def _clickhouse_table_name(table: Mapping[str, str]) -> str:
    schema = str(table.get("schema") or "")
    name = str(table.get("name") or "")
    if schema:
        return f"{_quote_clickhouse_identifier(schema)}.{_quote_clickhouse_identifier(name)}"
    return _quote_clickhouse_identifier(name)


def _quote_clickhouse_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


__all__ = ["ClickHouseSafeSampleInsertPlan", "ClickHouseSafeSampleSqlClient", "ClickHouseSafeSampleSqlWriter"]
