from __future__ import annotations

from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint, PartitionCheckpointStatus
from dpone.runtime.lineage.partition_resume import PartitionResumePlanner, PlannedTransferPartition


class _CheckpointStore:
    def __init__(self, checkpoints: list[PartitionCheckpoint]) -> None:
        self._checkpoints = checkpoints

    def list_latest(self) -> list[PartitionCheckpoint]:
        return self._checkpoints


def test_postgres_mssql_resume_does_not_skip_exported_only_partition() -> None:
    checkpoint = PartitionCheckpoint(
        transfer_partition_id="a" * 64,
        status=PartitionCheckpointStatus.EXPORTED,
        query_hash="sha256:q",
        schema_hash="sha256:s",
        source_table="public.orders",
        target_table="dbo.orders",
        partition_bounds={"lower": 1, "upper": 10},
    )
    partition = PlannedTransferPartition(
        transfer_partition_id="a" * 64,
        source_table="public.orders",
        target_table="dbo.orders",
        strategy="full_refresh",
        query_hash="sha256:q",
        schema_hash="sha256:s",
        partition_bounds={"lower": 1, "upper": 10},
    )

    plan = PartitionResumePlanner().plan((partition,), _CheckpointStore([checkpoint]))

    assert checkpoint.can_skip(query_hash="sha256:q", schema_hash="sha256:s") is False
    assert plan.retry[0].reason == "latest_status_exported"


def test_postgres_mssql_resume_skips_only_committed_matching_partition() -> None:
    checkpoint = PartitionCheckpoint(
        transfer_partition_id="b" * 64,
        status=PartitionCheckpointStatus.COMMITTED,
        query_hash="sha256:q",
        schema_hash="sha256:s",
        source_table="public.orders",
        target_table="dbo.orders",
        partition_bounds={"lower": 10, "upper": 20},
    )
    partition = PlannedTransferPartition(
        transfer_partition_id="b" * 64,
        source_table="public.orders",
        target_table="dbo.orders",
        strategy="full_refresh",
        query_hash="sha256:q",
        schema_hash="sha256:s",
        partition_bounds={"lower": 10, "upper": 20},
    )

    plan = PartitionResumePlanner().plan((partition,), _CheckpointStore([checkpoint]))

    assert plan.skip[0].reason == "committed_matching_checkpoint"
