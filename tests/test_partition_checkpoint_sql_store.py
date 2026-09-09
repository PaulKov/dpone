from __future__ import annotations

import json
from datetime import UTC, datetime

from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint, PartitionCheckpointStatus
from dpone.runtime.lineage.partition_checkpoint_sql_store import (
    MSSQLCheckpointDialect,
    PostgresCheckpointDialect,
    SqlPartitionCheckpointStore,
)


class FakeSqlConnector:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.rows: list[tuple[str]] = []

    def execute_query(self, sql: str):
        self.executed.append(sql)

    def get_records(self, sql: str):
        self.executed.append(sql)
        return self.rows


def _checkpoint(status: PartitionCheckpointStatus = PartitionCheckpointStatus.COMMITTED) -> PartitionCheckpoint:
    return PartitionCheckpoint(
        transfer_partition_id="p0",
        status=status,
        query_hash="query-a",
        schema_hash="schema-a",
        source_table="dbo.orders",
        target_table="analytics.orders",
        partition_bounds={"lower": 1, "upper": 100},
        started_at=datetime(2026, 6, 9, tzinfo=UTC),
        completed_at=datetime(2026, 6, 9, 0, 1, tzinfo=UTC),
        rows_exported=100,
        bytes_exported=4096,
        diagnostics={"artifact_sha256": "sha256:abc"},
    )


def _payload(checkpoint: PartitionCheckpoint) -> dict[str, object]:
    return {
        "transfer_partition_id": checkpoint.transfer_partition_id,
        "status": checkpoint.status.value,
        "query_hash": checkpoint.query_hash,
        "schema_hash": checkpoint.schema_hash,
        "source_table": checkpoint.source_table,
        "target_table": checkpoint.target_table,
        "partition_bounds": checkpoint.partition_bounds,
        "started_at": checkpoint.started_at.isoformat(),
        "completed_at": checkpoint.completed_at.isoformat() if checkpoint.completed_at else None,
        "rows_exported": checkpoint.rows_exported,
        "bytes_exported": checkpoint.bytes_exported,
        "diagnostics": checkpoint.diagnostics,
    }


def test_sql_checkpoint_store_upserts_and_reads_latest_payload() -> None:
    connector = FakeSqlConnector()
    checkpoint = _checkpoint()
    connector.rows = [(json.dumps(_payload(checkpoint)),)]
    store = SqlPartitionCheckpointStore(
        connector=connector,
        dialect=MSSQLCheckpointDialect(),
        schema="etl_state",
        table="dpone_partition_checkpoints",
    )

    store.ensure_schema()
    store.upsert(checkpoint)
    loaded = store.list_latest()

    assert any(
        "CREATE TABLE" in sql and "[etl_state].[dpone_partition_checkpoints]" in sql for sql in connector.executed
    )
    assert any("MERGE" in sql and "sha256:abc" in sql for sql in connector.executed)
    assert loaded == (checkpoint,)


def test_postgres_checkpoint_dialect_uses_on_conflict() -> None:
    sql = PostgresCheckpointDialect().upsert_sql(
        schema="etl_state",
        table="dpone_partition_checkpoints",
        payload=_payload(_checkpoint()),
    )

    assert '"etl_state"."dpone_partition_checkpoints"' in sql
    assert "ON CONFLICT" in sql
    assert "EXCLUDED" in sql
