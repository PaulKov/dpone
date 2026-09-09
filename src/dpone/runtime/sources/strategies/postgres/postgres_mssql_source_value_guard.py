"""Snapshot-consistent PostgreSQL value-domain guard for SQL Server routes."""

from __future__ import annotations

import re
import secrets
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from psycopg import sql


class PostgresMssqlSourceValueError(ValueError):
    """A typed PostgreSQL value cannot be represented by SQL Server."""

    code = "DPONE_POSTGRES_MSSQL_SOURCE_VALUE_UNREPRESENTABLE"

    def __init__(self, *, column: str, source_type: str) -> None:
        self.column = column
        self.source_type = source_type
        super().__init__(f"postgres_mssql.source_value_unrepresentable:{column}:{source_type}")


@dataclass(frozen=True, slots=True)
class _GuardedColumn:
    name: str
    source_type: str
    predicate: str
    uses_interval: bool


@dataclass(frozen=True, slots=True)
class _PostgresMssqlGuardedCopy:
    """One COPY query with an exact, sanitized source-domain failure map."""

    query_sql: Any
    guarded_columns: tuple[_GuardedColumn, ...]
    marker_namespace: str

    def translated_error(self, error: BaseException) -> PostgresMssqlSourceValueError | None:
        """Translate only a marker-bearing PostgreSQL invalid-cast failure."""

        if not isinstance(error, Exception) or _sqlstate(error) != "22P02":
            return None
        if not self.marker_namespace:
            return None
        matched = re.search(
            rf"{re.escape(self.marker_namespace)}(?P<ordinal>\d+)\b",
            _error_text(error),
        )
        if matched is None:
            return None
        ordinal = int(matched.group("ordinal"))
        if not 0 <= ordinal < len(self.guarded_columns):
            return None
        guarded = self.guarded_columns[ordinal]
        return PostgresMssqlSourceValueError(column=guarded.name, source_type=guarded.source_type)


class PostgresMssqlSourceValueGuard:
    """Reject temporal values that the MSSQL native domain cannot encode.

    The runtime fuses the guard projection into COPY under the caller-owned
    repeatable-read lease, so validation and export observe one statement and
    one snapshot scan. ``validate`` remains available for explicit diagnostic
    probes; the production export path uses :meth:`guard_copy_query`.
    """

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def validate(
        self,
        query_sql: Any,
        relation_schema: Sequence[tuple[str, str]],
        *,
        params: tuple[object, ...] = (),
    ) -> None:
        """Validate the exact filtered source query within the owned snapshot."""

        columns = tuple(
            guarded
            for name, source_type in relation_schema
            if (guarded := _guarded_column(str(name), str(source_type))) is not None
        )
        if not columns:
            return
        cases = " ".join(f"WHEN {guarded.predicate} THEN {_literal(guarded.name)}" for guarded in columns)
        predicates = " OR ".join(guarded.predicate for guarded in columns)
        if isinstance(query_sql, sql.Composable) or params:
            inner = query_sql if isinstance(query_sql, sql.Composable) else sql.SQL(str(query_sql))
            statement: Any = sql.SQL(
                f"SELECT CASE {cases} END AS offending_column "
                "FROM ({}) AS dpone_source_guard "
                f"WHERE {predicates} LIMIT 1"
            ).format(inner)
        else:
            statement = (
                f"SELECT CASE {cases} END AS offending_column "
                f"FROM ({query_sql}) AS dpone_source_guard "
                f"WHERE {predicates} LIMIT 1"
            )
        rows = (
            self._connector.get_records(statement, params=params, as_dict=True)
            if params
            else self._connector.get_records(statement, as_dict=True)
        )
        if not rows:
            return
        column = str(rows[0]["offending_column"])
        by_name = {guarded.name: guarded.source_type for guarded in columns}
        source_type = by_name.get(column)
        if source_type is None:  # pragma: no cover - CASE/query construction invariant.
            raise RuntimeError("postgres_mssql.source_value_guard_invalid_result")
        raise PostgresMssqlSourceValueError(column=column, source_type=source_type)

    def guard_copy_query(
        self,
        query_sql: Any,
        relation_schema: Sequence[tuple[str, str]],
    ) -> _PostgresMssqlGuardedCopy:
        """Fuse exact temporal-domain validation into the one PostgreSQL COPY scan.

        Invalid branches deliberately evaluate a marker-only cast that PostgreSQL
        reports with SQLSTATE ``22P02``. The marker maps back to a column without
        embedding the offending source value in dpone's public error.
        """

        guarded_columns = tuple(
            guarded
            for name, source_type in relation_schema
            if (guarded := _guarded_column(str(name), str(source_type))) is not None
        )
        if not guarded_columns:
            return _PostgresMssqlGuardedCopy(
                query_sql=query_sql,
                guarded_columns=(),
                marker_namespace="",
            )
        marker_namespace = f"dpone_pg_mssql_copy_guard_v1_{secrets.token_hex(16)}_"
        by_name = {guarded.name: (ordinal, guarded) for ordinal, guarded in enumerate(guarded_columns)}
        expressions = [
            _copy_projection(
                str(name),
                by_name.get(str(name)),
                marker_namespace=marker_namespace,
            )
            for name, _source_type in relation_schema
        ]
        if isinstance(query_sql, sql.Composable):
            guarded_query: Any = sql.SQL("SELECT {} FROM ({}) AS dpone_source_guard").format(
                sql.SQL(", ").join(sql.SQL(expression) for expression in expressions),
                query_sql,
            )
        else:
            guarded_query = f"SELECT {', '.join(expressions)} FROM ({query_sql}) AS dpone_source_guard"
        return _PostgresMssqlGuardedCopy(
            query_sql=guarded_query,
            guarded_columns=guarded_columns,
            marker_namespace=marker_namespace,
        )


def _guarded_column(name: str, source_type: str) -> _GuardedColumn | None:
    normalized = " ".join(source_type.strip().lower().split())
    value = f"dpone_source_guard.{_identifier(name)}"
    if normalized == "date":
        predicate = (
            f"({value} IS NOT NULL AND (NOT isfinite({value}) "
            f"OR {value} < DATE '0001-01-01' OR {value} >= DATE '10000-01-01'))"
        )
    elif normalized.startswith("timestamp") and (
        "with time zone" in normalized or normalized.startswith("timestamptz")
    ):
        utc = f"({value} AT TIME ZONE 'UTC')"
        predicate = (
            f"({value} IS NOT NULL AND (NOT isfinite({value}) "
            f"OR {utc} < TIMESTAMP '0001-01-01 00:00:00' "
            f"OR {utc} >= TIMESTAMP '10000-01-01 00:00:00'))"
        )
    elif normalized.startswith("timestamp"):
        predicate = (
            f"({value} IS NOT NULL AND (NOT isfinite({value}) "
            f"OR {value} < TIMESTAMP '0001-01-01 00:00:00' "
            f"OR {value} >= TIMESTAMP '10000-01-01 00:00:00'))"
        )
    elif normalized.startswith("time") and "with time zone" not in normalized:
        predicate = f"({value} IS NOT NULL AND {value} >= TIME '24:00:00')"
    else:
        return None
    return _GuardedColumn(name, source_type, predicate, normalized != "date")


def _copy_projection(
    name: str,
    guarded: tuple[int, _GuardedColumn] | None,
    *,
    marker_namespace: str,
) -> str:
    value = f"dpone_source_guard.{_identifier(name)}"
    if guarded is None:
        return f"{value} AS {_identifier(name)}"
    ordinal, column = guarded
    marker = _literal(f"{marker_namespace}{ordinal}")
    # LEFT(..., 0) keeps the marker independent of the source value while also
    # preventing PostgreSQL from constant-folding the invalid cast in a branch
    # that is not selected.
    trap = f"CAST({marker} || LEFT(CAST({value} AS text), 0) AS integer)"
    invalid_value = f"{value} + ({trap} * INTERVAL '0 seconds')" if column.uses_interval else f"{value} + {trap}"
    expression = f"CASE WHEN {column.predicate} THEN {invalid_value} ELSE {value} END"
    return f"{expression} AS {_identifier(name)}"


def _sqlstate(error: Exception) -> str | None:
    direct = getattr(error, "sqlstate", None)
    if direct is not None:
        return str(direct)
    return str(getattr(getattr(error, "diag", None), "sqlstate", "") or "") or None


def _error_text(error: Exception) -> str:
    primary = getattr(getattr(error, "diag", None), "message_primary", None)
    return str(primary) if primary is not None else str(error)


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


__all__ = [
    "PostgresMssqlSourceValueError",
    "PostgresMssqlSourceValueGuard",
]
