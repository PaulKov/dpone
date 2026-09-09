from __future__ import annotations

from datetime import UTC, datetime

from dpone.runtime.lineage.partition_checkpoint import (
    PartitionCheckpoint,
    PartitionCheckpointStatus,
    build_transfer_partition_id,
)
from dpone.runtime.lineage.partition_checkpoint_store import JsonlPartitionCheckpointStore
from dpone.runtime.lineage.partition_resume import PartitionResumePlanner, PlannedTransferPartition


def _planned(index: int) -> PlannedTransferPartition:
    bounds = {"lower": index * 100 + 1, "upper": (index + 1) * 100}
    return PlannedTransferPartition(
        transfer_partition_id=build_transfer_partition_id(
            source_table="dbo.orders",
            target_table="analytics.orders",
            strategy="full_refresh",
            query_hash="query-a",
            schema_hash="schema-a",
            partition_bounds=bounds,
        ),
        source_table="dbo.orders",
        target_table="analytics.orders",
        strategy="full_refresh",
        query_hash="query-a",
        schema_hash="schema-a",
        partition_bounds=bounds,
    )


def _checkpoint(partition: PlannedTransferPartition, status: PartitionCheckpointStatus, *, query_hash: str = "query-a"):
    return PartitionCheckpoint(
        transfer_partition_id=partition.transfer_partition_id,
        status=status,
        query_hash=query_hash,
        schema_hash=partition.schema_hash,
        source_table=partition.source_table,
        target_table=partition.target_table,
        partition_bounds=partition.partition_bounds,
        started_at=datetime(2026, 6, 9, tzinfo=UTC),
        completed_at=datetime(2026, 6, 9, 0, 1, tzinfo=UTC),
        diagnostics={"artifact_sha256": f"sha256:{partition.transfer_partition_id[:8]}"},
    )


def test_resume_planner_skips_only_committed_matching_partitions(tmp_path) -> None:
    planned = (_planned(0), _planned(1), _planned(2), _planned(3))
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    store.upsert(_checkpoint(planned[0], PartitionCheckpointStatus.COMMITTED))
    store.upsert(_checkpoint(planned[1], PartitionCheckpointStatus.LOADED))
    store.upsert(_checkpoint(planned[2], PartitionCheckpointStatus.COMMITTED, query_hash="query-old"))

    plan = PartitionResumePlanner().plan(planned, store)

    assert [decision.partition.transfer_partition_id for decision in plan.skip] == [planned[0].transfer_partition_id]
    assert [decision.reason for decision in plan.retry] == [
        "latest_status_loaded",
        "query_or_schema_hash_changed",
        "missing_checkpoint",
    ]
    assert plan.summary == {"skip": 1, "retry": 3}


def test_resume_planner_requires_artifact_checksum_when_configured(tmp_path) -> None:
    planned = (_planned(0),)
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    checkpoint = _checkpoint(planned[0], PartitionCheckpointStatus.COMMITTED)
    store.upsert(
        PartitionCheckpoint(
            transfer_partition_id=checkpoint.transfer_partition_id,
            status=checkpoint.status,
            query_hash=checkpoint.query_hash,
            schema_hash=checkpoint.schema_hash,
            source_table=checkpoint.source_table,
            target_table=checkpoint.target_table,
            partition_bounds=checkpoint.partition_bounds,
            diagnostics={},
        )
    )

    plan = PartitionResumePlanner(require_artifact_checksum=True).plan(planned, store)

    assert not plan.skip
    assert [decision.reason for decision in plan.retry] == ["missing_artifact_checksum"]
