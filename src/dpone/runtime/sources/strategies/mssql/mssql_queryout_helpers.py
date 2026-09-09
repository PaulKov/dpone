"""Small helpers for MSSQL queryout artifact builders."""

from __future__ import annotations

from typing import Any

from dpone.runtime.support.type_mapping.mssql_clickhouse import (
    MssqlClickHouseTypePolicy,
    schema_with_temporal_companion_columns,
)


def type_policy(load_config: Any) -> MssqlClickHouseTypePolicy:
    return MssqlClickHouseTypePolicy.from_config((load_config.options or {}).get("type_fidelity"))


def output_schema(load_config: Any, schema: list[tuple[str, str]]) -> list[tuple[str, str]]:
    return schema_with_temporal_companion_columns(schema, type_policy(load_config))


def database(schema: str, source_database: str | None) -> str | None:
    return None if "." in str(schema) else source_database


def schema_label(schema: str, source_database: str | None) -> str:
    if source_database and "." not in str(schema):
        return f"{source_database}.{schema}"
    return str(schema)


def table_label(schema: str, table: str, source_database: str | None) -> str:
    return f"{schema_label(schema, source_database)}.{table}"


def qualified_name(connector: Any, schema: str, table: str, *, source_database: str | None) -> str:
    try:
        return str(connector.qualified_name(schema, table, database=source_database))
    except TypeError:
        return str(connector.qualified_name(schema_label(schema, source_database), table))


__all__ = ["database", "output_schema", "qualified_name", "schema_label", "table_label", "type_policy"]
