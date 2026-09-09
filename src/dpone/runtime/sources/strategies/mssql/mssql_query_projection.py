"""MSSQL query projection helpers for bulk exports."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

from dpone.runtime.sources.strategies.mssql.mssql_queryout_helpers import database as _database
from dpone.runtime.sources.strategies.mssql.mssql_queryout_helpers import qualified_name as _qualified_name
from dpone.runtime.support.bulk_text_codec import BulkTextCodec, is_bulk_text_type
from dpone.runtime.support.clickhouse_tsv_codec import ClickHouseTabSeparatedCodec


def wrap_mssql_bulk_text_query(
    connector: Any,
    query: str,
    schema: list[tuple[str, str]],
    codec: BulkTextCodec,
) -> str:
    """Wrap a SELECT so text values are safe for MSSQL bcp character files."""

    expressions = []
    for column, dtype in schema:
        source_column = f"dpone_src.{connector.quote_identifier(column)}"
        expression = codec.mssql_encode_expression(source_column) if is_bulk_text_type(dtype) else source_column
        expressions.append(f"{expression} AS {connector.quote_identifier(column)}")
    return f"SELECT {', '.join(expressions)} FROM ({query}) AS dpone_src"


def wrap_clickhouse_tabseparated_query(
    connector: Any,
    query: str,
    schema: list[tuple[str, str]],
    codec: ClickHouseTabSeparatedCodec,
) -> str:
    """Wrap a SELECT so values are encoded for direct ClickHouse TSV loading."""

    expressions = []
    for column, dtype in schema:
        source_column = f"dpone_src.{connector.quote_identifier(column)}"
        expressions.append(
            f"{codec.mssql_select_expression(source_column, text_column=is_bulk_text_type(dtype), source_type=dtype)} "
            f"AS {connector.quote_identifier(column)}"
        )
        for generated_column, expression in codec.mssql_generated_select_expressions(column, source_column, dtype):
            expressions.append(f"{expression} AS {connector.quote_identifier(generated_column)}")
    return f"SELECT {', '.join(expressions)} FROM ({query}) AS dpone_src"


def should_materialize_queryout_projection(load_config: Any, query: str) -> bool:
    """Return whether a wide ClickHouse queryout projection should use a temporary view."""

    mode = str(load_config.options.get("mssql_queryout_projection", "auto")).lower()
    if mode in {"off", "inline", "false", "0"}:
        return False
    if mode in {"view", "materialized_view", "true", "1"}:
        return True
    threshold = int(load_config.options.get("mssql_queryout_projection_threshold", 60000))
    return len(query) >= threshold


def materialize_queryout_projection(
    connector: Any,
    load_config: Any,
    query: str,
    schema: list[tuple[str, str]],
) -> tuple[str, Callable[[], None]]:
    """Create a temporary projection view and return a shorter query plus cleanup hook."""

    view_schema = load_config.source_schema
    view_name = f"__dpone__bcp_{uuid.uuid4().hex}"
    qualified_view = _qualified_name(
        connector,
        view_schema,
        view_name,
        source_database=_database(view_schema, load_config.source_database),
    )
    connector.execute_query(f"CREATE VIEW {qualified_view} AS {query}")
    select_columns = ", ".join(connector.quote_identifier(column) for column, _ in schema)
    short_query = f"SELECT {select_columns} FROM {qualified_view}"

    def cleanup() -> None:
        connector.execute_query(f"DROP VIEW IF EXISTS {qualified_view}")

    return short_query, cleanup


__all__ = [
    "materialize_queryout_projection",
    "should_materialize_queryout_projection",
    "wrap_clickhouse_tabseparated_query",
    "wrap_mssql_bulk_text_query",
]
