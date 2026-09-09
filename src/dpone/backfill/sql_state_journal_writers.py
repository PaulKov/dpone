"""Dialect-owned append adapters for the backfill SQL journal.

Connector ``execute_query`` ports intentionally have different parameter
contracts.  PostgreSQL and SQL Server execute one parameterized statement,
while the ClickHouse driver accepts a batch of rows after ``INSERT ... VALUES``.
Keeping that decision in explicit adapters prevents a nested ClickHouse row
batch from leaking into DB-API connectors.
"""

from __future__ import annotations

from typing import Any, Protocol

JournalRow = tuple[Any, ...]


def campaign_insert_prefix(table: str) -> str:
    """Render the dialect-neutral campaign column list before row markers."""

    return (
        f"INSERT INTO {table} "
        "(run_key, dataset, inner_mode, status, plan_hash, config_hash, chunk_config_json, details_json) VALUES"
    )


def chunk_insert_prefix(table: str) -> str:
    """Render the dialect-neutral chunk column list before row markers."""

    return (
        f"INSERT INTO {table} "
        "(run_key, chunk_index, status, start_value, end_value, idempotency_key, run_id, load_id, error, details_json) "
        "VALUES"
    )


class BackfillJournalWriter(Protocol):
    """Append one immutable journal row using a connector-native call shape."""

    def append_row(self, connector: Any, insert_prefix: str, row: JournalRow) -> None: ...


class MssqlBackfillJournalWriter:
    """Append one SQL Server row through pyodbc positional markers."""

    def append_row(self, connector: Any, insert_prefix: str, row: JournalRow) -> None:
        _append_parameterized(connector, insert_prefix, row, marker="?")


class PostgresBackfillJournalWriter:
    """Append one PostgreSQL row through psycopg positional markers."""

    def append_row(self, connector: Any, insert_prefix: str, row: JournalRow) -> None:
        _append_parameterized(connector, insert_prefix, row, marker="%s")


class ClickHouseBackfillJournalWriter:
    """Append one ClickHouse row through its explicit row-batch API."""

    def append_row(self, connector: Any, insert_prefix: str, row: JournalRow) -> None:
        _require_row(row)
        connector.execute_query(insert_prefix, [row])


def _append_parameterized(
    connector: Any,
    insert_prefix: str,
    row: JournalRow,
    *,
    marker: str,
) -> None:
    _require_row(row)
    placeholders = ", ".join(marker for _ in row)
    connector.execute_query(f"{insert_prefix} ({placeholders})", row)


def _require_row(row: JournalRow) -> None:
    if not isinstance(row, tuple) or not row:
        raise ValueError("backfill SQL journal row must be a non-empty tuple")


__all__ = [
    "BackfillJournalWriter",
    "ClickHouseBackfillJournalWriter",
    "JournalRow",
    "MssqlBackfillJournalWriter",
    "PostgresBackfillJournalWriter",
    "campaign_insert_prefix",
    "chunk_insert_prefix",
]
