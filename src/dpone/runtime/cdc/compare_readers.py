"""Live CDC compare readers for source tables and ClickHouse CDC logs."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from dpone.runtime.cdc.compare_models import CdcCompareRow


class MssqlCdcCompareReader:
    """Read bounded current-state rows from a SQL Server source table."""

    def __init__(
        self,
        *,
        connector: Any,
        source_schema: str,
        source_table: str,
        columns: Sequence[str],
        unique_key: Sequence[str],
        max_rows: int = 10000,
    ) -> None:
        self._connector = connector
        self._source_schema = source_schema
        self._source_table = source_table
        self._columns = tuple(columns)
        self._unique_key = tuple(unique_key)
        self._max_rows = max_rows

    def read_rows(self) -> tuple[CdcCompareRow, ...]:
        rows = self._connector.get_records(self._query(), (self._max_rows,), as_dict=True)
        return tuple(CdcCompareRow.from_payload(unique_key=self._unique_key, payload=row) for row in rows)

    def _query(self) -> str:
        columns_sql = ", ".join(_mssql_identifier(column) for column in self._columns)
        order_sql = ", ".join(_mssql_identifier(column) for column in self._unique_key)
        return (
            f"SELECT TOP (?) {columns_sql}\n"
            f"FROM {_mssql_identifier(self._source_schema)}.{_mssql_identifier(self._source_table)}\n"
            f"ORDER BY {order_sql}"
        )


class ClickHouseCdcLogCompareReader:
    """Read latest current-state rows from an append-only ClickHouse CDC log."""

    def __init__(
        self,
        *,
        connector: Any,
        cdc_dataset: str,
        stream_id: str,
        unique_key: Sequence[str],
        max_rows: int = 10000,
    ) -> None:
        self._connector = connector
        self._database, self._table = _split_dataset(cdc_dataset, default_database=str(connector.database))
        self._stream_id = stream_id
        self._unique_key = tuple(unique_key)
        self._max_rows = max_rows

    def read_rows(self) -> tuple[CdcCompareRow, ...]:
        rows = self._connector.get_records(self._query(), as_dict=True)
        result: list[CdcCompareRow] = []
        for row in rows:
            payload = json.loads(str(row["dpone_cdc_payload_json"]))
            result.append(
                CdcCompareRow.from_payload(
                    unique_key=self._unique_key,
                    payload=payload,
                    deleted=bool(int(row.get("dpone_cdc_deleted", 0) or 0)),
                    position=str(row.get("dpone_cdc_position") or ""),
                )
            )
        return tuple(result)

    def _query(self) -> str:
        return f"""
SELECT
    dpone_cdc_payload_json,
    dpone_cdc_deleted,
    dpone_cdc_position
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY dpone_cdc_unique_key_hash
            ORDER BY
                dpone_cdc_ingested_at DESC,
                length(dpone_cdc_position) DESC,
                dpone_cdc_position DESC,
                dpone_cdc_sequence DESC,
                dpone_cdc_event_hash DESC
        ) AS rn
    FROM {_qualified(self._database, self._table)}
    WHERE dpone_cdc_stream_id = {_clickhouse_literal(self._stream_id)}
)
WHERE rn = 1 AND dpone_cdc_deleted = 0
LIMIT {int(self._max_rows)}
""".strip()


def _mssql_identifier(value: str) -> str:
    if not value:
        raise ValueError("MSSQL identifier cannot be empty")
    return "[" + value.replace("]", "]]") + "]"


def _split_dataset(value: str, *, default_database: str) -> tuple[str, str]:
    parts = [part.strip() for part in value.split(".") if part.strip()]
    if len(parts) == 1:
        return default_database, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"ClickHouse CDC dataset must be table or database.table: {value!r}")


def _qualified(database: str, table: str) -> str:
    return f"{_clickhouse_identifier(database)}.{_clickhouse_identifier(table)}"


def _clickhouse_identifier(value: str) -> str:
    if not value:
        raise ValueError("ClickHouse identifier cannot be empty")
    return "`" + value.replace("`", "``") + "`"


def _clickhouse_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


__all__ = ["ClickHouseCdcLogCompareReader", "MssqlCdcCompareReader"]
