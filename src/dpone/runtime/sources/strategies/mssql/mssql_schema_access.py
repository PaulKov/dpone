"""Compatibility accessors for SQL Server source metadata and rendered SQL."""

from __future__ import annotations

from typing import Any

from dpone.runtime.sources.strategies.mssql.mssql_queryout_helpers import database as _database
from dpone.runtime.sources.strategies.mssql.mssql_queryout_helpers import schema_label


def source_database(schema: str, database_name: str | None) -> str | None:
    return _database(schema, database_name)


def fetch_schema(connector: Any, schema: str, table: str, *, database: str | None) -> list[tuple[str, str]]:
    try:
        return list(connector.fetch_schema(schema, table, database=database))
    except TypeError:
        return list(connector.fetch_schema(schema_label(schema, database), table))


def prune_schema(
    schema: list[tuple[str, str]],
    columns: list[str] | tuple[str, ...] | str | None,
) -> list[tuple[str, str]]:
    """Return schema restricted to an explicit source column allowlist."""

    if not columns:
        return schema
    requested = [columns] if isinstance(columns, str) else list(columns)
    by_name = {column: dtype for column, dtype in schema}
    missing = [column for column in requested if column not in by_name]
    if missing:
        raise ValueError(f"Unknown source columns: {', '.join(missing)}")
    return [(column, by_name[column]) for column in requested]


def build_select_query(
    connector: Any,
    schema: str,
    table: str,
    columns: list[str],
    *,
    database: str | None,
) -> str:
    try:
        return str(connector.build_select_query(schema, table, columns, database=database))
    except TypeError:
        return str(connector.build_select_query(schema_label(schema, database), table, columns))


def get_max_column_value(
    connector: Any,
    schema: str,
    table: str,
    column: str,
    *,
    database: str | None,
) -> Any | None:
    try:
        return connector.get_max_column_value(schema, table, column, database=database)
    except TypeError:
        return connector.get_max_column_value(schema_label(schema, database), table, column)


__all__ = ["build_select_query", "fetch_schema", "get_max_column_value", "prune_schema", "source_database"]
