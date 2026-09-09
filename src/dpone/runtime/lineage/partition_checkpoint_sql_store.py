"""SQL-backed partition checkpoint store adapters."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint, PartitionCheckpointStatus
from dpone.runtime.lineage.partition_checkpoint_store import _checkpoint_from_payload, _checkpoint_to_payload


class SqlCheckpointConnector(Protocol):
    """Minimal SQL connector protocol used by checkpoint stores."""

    def execute_query(self, sql: str) -> Any:
        """Execute a SQL statement."""

    def get_records(self, sql: str) -> list[tuple[Any, ...]]:
        """Return query records."""


class CheckpointSqlDialect(Protocol):
    """Render SQL for one state backend dialect."""

    def create_table_sql(self, *, schema: str, table: str) -> str:
        """Render idempotent checkpoint table DDL."""

    def upsert_sql(self, *, schema: str, table: str, payload: dict[str, object]) -> str:
        """Render checkpoint upsert SQL."""

    def select_latest_sql(self, *, schema: str, table: str) -> str:
        """Render latest checkpoint read SQL."""


@dataclass(frozen=True)
class MSSQLCheckpointDialect:
    """MSSQL checkpoint table SQL renderer."""

    def create_table_sql(self, *, schema: str, table: str) -> str:
        qualified = _mssql_qualified(schema, table)
        return f"""
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{_sql_literal(schema)}')
    EXEC(N'CREATE SCHEMA [{_mssql_escape(schema)}]');
IF OBJECT_ID(N'{_sql_literal(schema)}.{_sql_literal(table)}', N'U') IS NULL
BEGIN
    CREATE TABLE {qualified} (
        transfer_partition_id nvarchar(64) NOT NULL PRIMARY KEY,
        status nvarchar(32) NOT NULL,
        query_hash nvarchar(256) NOT NULL,
        schema_hash nvarchar(256) NOT NULL,
        payload nvarchar(max) NOT NULL,
        updated_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
""".strip()

    def upsert_sql(self, *, schema: str, table: str, payload: dict[str, object]) -> str:
        qualified = _mssql_qualified(schema, table)
        payload_json = _json_literal(payload)
        return f"""
MERGE {qualified} AS target
USING (
    SELECT
        N'{_sql_literal(str(payload["transfer_partition_id"]))}' AS transfer_partition_id,
        N'{_sql_literal(str(payload["status"]))}' AS status,
        N'{_sql_literal(str(payload["query_hash"]))}' AS query_hash,
        N'{_sql_literal(str(payload["schema_hash"]))}' AS schema_hash,
        N'{payload_json}' AS payload
) AS source
ON target.transfer_partition_id = source.transfer_partition_id
WHEN MATCHED THEN UPDATE SET
    status = source.status,
    query_hash = source.query_hash,
    schema_hash = source.schema_hash,
    payload = source.payload,
    updated_at = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT (transfer_partition_id, status, query_hash, schema_hash, payload)
VALUES (source.transfer_partition_id, source.status, source.query_hash, source.schema_hash, source.payload);
""".strip()

    def select_latest_sql(self, *, schema: str, table: str) -> str:
        return f"SELECT payload FROM {_mssql_qualified(schema, table)} ORDER BY transfer_partition_id"


@dataclass(frozen=True)
class PostgresCheckpointDialect:
    """Postgres checkpoint table SQL renderer."""

    def create_table_sql(self, *, schema: str, table: str) -> str:
        qualified = _pg_qualified(schema, table)
        return f"""
CREATE SCHEMA IF NOT EXISTS "{_pg_escape(schema)}";
CREATE TABLE IF NOT EXISTS {qualified} (
    transfer_partition_id text PRIMARY KEY,
    status text NOT NULL,
    query_hash text NOT NULL,
    schema_hash text NOT NULL,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
""".strip()

    def upsert_sql(self, *, schema: str, table: str, payload: dict[str, object]) -> str:
        qualified = _pg_qualified(schema, table)
        payload_json = _json_literal(payload)
        return f"""
INSERT INTO {qualified} (transfer_partition_id, status, query_hash, schema_hash, payload)
VALUES (
    '{_sql_literal(str(payload["transfer_partition_id"]))}',
    '{_sql_literal(str(payload["status"]))}',
    '{_sql_literal(str(payload["query_hash"]))}',
    '{_sql_literal(str(payload["schema_hash"]))}',
    '{payload_json}'::jsonb
)
ON CONFLICT (transfer_partition_id) DO UPDATE SET
    status = EXCLUDED.status,
    query_hash = EXCLUDED.query_hash,
    schema_hash = EXCLUDED.schema_hash,
    payload = EXCLUDED.payload,
    updated_at = now();
""".strip()

    def select_latest_sql(self, *, schema: str, table: str) -> str:
        return f"SELECT payload::text FROM {_pg_qualified(schema, table)} ORDER BY transfer_partition_id"


@dataclass(frozen=True)
class SqlPartitionCheckpointStore:
    """Partition checkpoint store backed by an existing SQL state connection."""

    connector: SqlCheckpointConnector
    dialect: CheckpointSqlDialect
    schema: str = "etl_state"
    table: str = "dpone_partition_checkpoints"

    def ensure_schema(self) -> None:
        self.connector.execute_query(self.dialect.create_table_sql(schema=self.schema, table=self.table))

    def upsert(self, checkpoint: PartitionCheckpoint) -> None:
        payload = _checkpoint_to_payload(checkpoint)
        self.connector.execute_query(self.dialect.upsert_sql(schema=self.schema, table=self.table, payload=payload))

    def upsert_many(self, checkpoints: tuple[PartitionCheckpoint, ...]) -> None:
        for checkpoint in checkpoints:
            self.upsert(checkpoint)

    def list_latest(self) -> tuple[PartitionCheckpoint, ...]:
        rows = self.connector.get_records(self.dialect.select_latest_sql(schema=self.schema, table=self.table))
        return tuple(_checkpoint_from_payload(_payload_from_row(row)) for row in rows)

    def safe_to_skip(self, *, query_hash: str, schema_hash: str) -> tuple[PartitionCheckpoint, ...]:
        return tuple(
            checkpoint
            for checkpoint in self.list_latest()
            if checkpoint.can_skip(query_hash=query_hash, schema_hash=schema_hash)
        )

    def summary(self) -> dict[str, int]:
        counts = {status.value: 0 for status in PartitionCheckpointStatus}
        for checkpoint in self.list_latest():
            counts[checkpoint.status.value] += 1
        return counts


def _payload_from_row(row: tuple[Any, ...]) -> dict[str, object]:
    value = row[0]
    if isinstance(value, dict):
        return value
    parsed = json.loads(str(value))
    if isinstance(parsed, dict) and "transfer_partition_id" in parsed:
        return parsed
    if isinstance(parsed, dict) and "payload" in parsed:
        return parsed  # compatibility for tests or wrapped payloads
    return parsed


def _json_literal(payload: dict[str, object]) -> str:
    return _sql_literal(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _mssql_qualified(schema: str, table: str) -> str:
    return f"[{_mssql_escape(schema)}].[{_mssql_escape(table)}]"


def _mssql_escape(value: str) -> str:
    return value.replace("]", "]]")


def _pg_qualified(schema: str, table: str) -> str:
    return f'"{_pg_escape(schema)}"."{_pg_escape(table)}"'


def _pg_escape(value: str) -> str:
    return value.replace('"', '""')
