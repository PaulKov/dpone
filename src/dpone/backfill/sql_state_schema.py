"""Exact causal-order schema contract for SQL backfill journals."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

JOURNAL_SCHEMA_ERROR = "DPONE_BACKFILL_JOURNAL_SCHEMA_MIGRATION_REQUIRED"
_SHAPE_QUERY_MARKER = "dpone_backfill_journal_shape"


class BackfillJournalSchemaError(ValueError):
    """Raised when an existing journal cannot provide causal row ordering."""


def journal_shape_query(*, dialect: str, schema: str, table: str) -> str:
    """Render one read-only catalog query for the dialect-owned ID column."""

    schema_literal = _literal(schema)
    table_literal = _literal(table)
    if dialect == "mssql":
        object_literal = _literal(f"{schema}.{table}")
        return f"""
        /* {_SHAPE_QUERY_MARKER}:mssql */
        SELECT c.name AS column_name, t.name AS data_type,
               c.is_nullable,
               COLUMNPROPERTY(c.object_id, c.name, 'IsIdentity') AS is_identity
        FROM sys.columns AS c
        INNER JOIN sys.types AS t ON t.user_type_id = c.user_type_id
        WHERE c.object_id = OBJECT_ID(N'{object_literal}')
          AND c.name = N'journal_id'
        """
    if dialect == "postgres":
        return f"""
        /* {_SHAPE_QUERY_MARKER}:postgres */
        SELECT column_name, data_type, is_nullable, is_identity, identity_generation
        FROM information_schema.columns
        WHERE table_schema = '{schema_literal}'
          AND table_name = '{table_literal}'
          AND column_name = 'journal_id'
        """
    if dialect == "clickhouse":
        return f"""
        /* {_SHAPE_QUERY_MARKER}:clickhouse */
        SELECT c.name AS column_name, c.type AS data_type,
               c.default_kind, c.default_expression, t.engine_full
        FROM system.columns AS c
        INNER JOIN system.tables AS t
          ON t.database = c.database AND t.name = c.table
        WHERE c.database = '{schema_literal}'
          AND c.table = '{table_literal}'
          AND c.name = 'journal_id'
        """
    raise BackfillJournalSchemaError(f"{JOURNAL_SCHEMA_ERROR}: unsupported dialect {dialect!r}")


def validate_journal_shape(
    rows: Sequence[Any],
    *,
    dialect: str,
    schema: str,
    table: str,
) -> None:
    """Reject missing, duplicate, or non-causal journal-ID definitions."""

    if len(rows) != 1 or not isinstance(rows[0], Mapping):
        _reject(dialect, schema, table, "journal_id is missing or duplicated")
    row = rows[0]
    if row.get("column_name") != "journal_id":
        _reject(dialect, schema, table, "journal_id has the wrong catalog spelling")
    if dialect == "mssql":
        valid = (
            row.get("data_type") == "bigint"
            and row.get("is_nullable") in {False, 0}
            and row.get("is_identity") in {True, 1}
        )
    elif dialect == "postgres":
        valid = (
            row.get("data_type") == "bigint"
            and row.get("is_nullable") == "NO"
            and row.get("is_identity") == "YES"
            and row.get("identity_generation") == "ALWAYS"
        )
    elif dialect == "clickhouse":
        expression = str(row.get("default_expression") or "").replace(" ", "")
        engine = str(row.get("engine_full") or "").replace(" ", "")
        valid = (
            row.get("data_type") == "UInt64"
            and row.get("default_kind") == "DEFAULT"
            and expression.startswith("generateSnowflakeID(")
            and "ReplacingMergeTree(journal_id)" in engine
        )
    else:
        valid = False
    if not valid:
        _reject(dialect, schema, table, "journal_id is not the exact dialect-owned causal identity")


def _reject(dialect: str, schema: str, table: str, reason: str) -> None:
    raise BackfillJournalSchemaError(
        f"{JOURNAL_SCHEMA_ERROR}: {dialect} {schema}.{table}: {reason}; "
        "automatic migration is forbidden because historical timestamp ties are causally ambiguous"
    )


def _literal(value: str) -> str:
    return value.replace("'", "''")


__all__ = [
    "BackfillJournalSchemaError",
    "JOURNAL_SCHEMA_ERROR",
    "journal_shape_query",
    "validate_journal_shape",
]
