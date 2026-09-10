"""Prepared SQL preserves explicit order, typed values and UNION ALL rows."""

import sqlite3
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_object_name import quote_mssql_identifier
from dpone.runtime.sinks.mssql_native_prepared_insert import build_prepared_insert
from dpone.runtime.sinks.strategies.mssql.mssql_native_lineage import MssqlNativeLineageProjection
from dpone.runtime.sinks.strategies.mssql.mssql_native_schema import ResolvedMssqlNativeSchema


def plain_projection(schema):
    types = dict(schema)
    resolved = ResolvedMssqlNativeSchema(
        types, dict(types), {name: True for name in types}, {}, {name: name for name in types}
    )
    lineage = MssqlNativeLineageProjection.resolve(
        SimpleNamespace(options={"lineage": False}),
        SimpleNamespace(extraction_started_at=datetime(2026, 1, 1, tzinfo=UTC)),
    )
    return resolved, lineage


def test_explicit_order_and_quoted_columns_golden():
    schema = (("text]雪", "nvarchar(50)"), ("id", "int"))
    resolved, lineage = plain_projection(schema)
    source = "SELECT [text]]雪], [id] FROM [db].[stage].[a] UNION ALL SELECT [text]]雪], [id] FROM [db].[stage].[b]"
    sql = build_prepared_insert(
        target_sql="[db].[stage].[prepared]",
        source_sql=source,
        business_schema=schema,
        resolved=resolved,
        lineage=lineage,
        quote_identifier=quote_mssql_identifier,
    )
    assert sql == (
        "INSERT INTO [db].[stage].[prepared] ([text]]雪], [id]) "
        "SELECT r.[text]]雪] AS [text]]雪], r.[id] AS [id] FROM (" + source + ") AS r"
    )


@pytest.mark.parametrize("rows", [[], [(7, "O'Brien 雪", b"\x00\xff")], [(7, None, b"")] * 3])
def test_union_all_empty_and_duplicate_row_semantics_with_local_sql(rows):
    """Execute the portable business-only subset; this is not MSSQL evidence."""
    schema = (("id", "int"), ("text", "nvarchar(50)"), ("bin", "varbinary(50)"))
    resolved, lineage = plain_projection(schema)
    sql = build_prepared_insert(
        target_sql="[prepared]",
        source_sql="SELECT [id], [text], [bin] FROM [a] UNION ALL SELECT [id], [text], [bin] FROM [b]",
        business_schema=schema,
        resolved=resolved,
        lineage=lineage,
        quote_identifier=quote_mssql_identifier,
    )
    with sqlite3.connect(":memory:") as connection:
        for table in ("a", "b", "prepared"):
            connection.execute(f"CREATE TABLE [{table}] (id INTEGER, text TEXT, bin BLOB)")
        connection.executemany("INSERT INTO a VALUES (?, ?, ?)", rows)
        connection.executemany("INSERT INTO b VALUES (?, ?, ?)", rows)
        connection.execute(sql)
        assert connection.execute("SELECT id, text, bin FROM prepared").fetchall() == rows + rows
    assert "UNION ALL" in sql
    assert "UPDATE" not in sql and "DISTINCT" not in sql and "WHERE" not in sql


def test_source_column_identifier_is_quoted_without_interpreting_sql():
    name = "x]; DROP TABLE target;--"
    schema = ((name, "int"),)
    resolved, lineage = plain_projection(schema)
    sql = build_prepared_insert(
        target_sql="[db].[stage].[prepared]",
        source_sql=f"SELECT {quote_mssql_identifier(name)} FROM [db].[stage].[a]",
        business_schema=schema,
        resolved=resolved,
        lineage=lineage,
        quote_identifier=quote_mssql_identifier,
    )
    assert f"r.{quote_mssql_identifier(name)} AS {quote_mssql_identifier(name)}" in sql


def test_empty_column_identifier_fails_in_injected_quoter():
    schema = (("", "int"),)
    resolved, lineage = plain_projection(schema)
    with pytest.raises(ValueError, match="identifier must not be empty"):
        build_prepared_insert(
            target_sql="[db].[stage].[prepared]",
            source_sql="SELECT 1",
            business_schema=schema,
            resolved=resolved,
            lineage=lineage,
            quote_identifier=quote_mssql_identifier,
        )
