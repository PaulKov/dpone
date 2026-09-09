from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint, PartitionCheckpointStatus
from dpone.strategy_intelligence.fault_injection import (
    FaultInjectionSnapshot,
    FaultInjectionStage,
    NativeTransferFaultInjectionScenario,
    NativeTransferFaultInjectionWorkflow,
)


def _checkpoint(status: PartitionCheckpointStatus, partition: int) -> PartitionCheckpoint:
    return PartitionCheckpoint(
        transfer_partition_id=f"{partition:064x}",
        status=status,
        query_hash="query-a",
        schema_hash="schema-a",
        source_table="dbo.orders",
        target_table="analytics.orders",
        partition_bounds={"lower": partition * 100, "upper": (partition + 1) * 100},
        started_at=datetime(2026, 6, 8, tzinfo=UTC),
        completed_at=datetime(2026, 6, 8, 0, 1, tzinfo=UTC),
        rows_exported=100,
        bytes_exported=4096,
    )


@dataclass
class FakeFaultOperations:
    calls: list[str]

    def inject_failure(self, stage: FaultInjectionStage) -> FaultInjectionSnapshot:
        self.calls.append(f"inject:{stage.value}")
        if stage is FaultInjectionStage.AFTER_EXPORT:
            status = PartitionCheckpointStatus.EXPORTED
        elif stage is FaultInjectionStage.DURING_LOAD:
            status = PartitionCheckpointStatus.LOADED
        else:
            status = PartitionCheckpointStatus.FINALIZED
        return FaultInjectionSnapshot(
            checkpoints=(_checkpoint(status, 0), _checkpoint(PartitionCheckpointStatus.FAILED, 1)),
            row_count=0,
            duplicate_rows=0,
            state_committed=False,
        )

    def retry(self, stage: FaultInjectionStage) -> FaultInjectionSnapshot:
        self.calls.append(f"retry:{stage.value}")
        return FaultInjectionSnapshot(
            checkpoints=(
                _checkpoint(PartitionCheckpointStatus.COMMITTED, 0),
                _checkpoint(PartitionCheckpointStatus.COMMITTED, 1),
            ),
            row_count=200,
            duplicate_rows=0,
            state_committed=True,
        )


def test_fault_injection_workflow_certifies_retry_after_failure() -> None:
    operations = FakeFaultOperations(calls=[])
    scenario = NativeTransferFaultInjectionScenario(
        run_id="run-after-export",
        source_type="mssql",
        sink_type="clickhouse",
        strategy="full_refresh",
        failure_stage=FaultInjectionStage.AFTER_EXPORT,
        expected_rows=200,
        query_hash="query-a",
        schema_hash="schema-a",
    )

    result = NativeTransferFaultInjectionWorkflow().run(scenario, operations)

    assert result.passed
    assert result.safe_to_resume
    assert result.retry_partition_count == 2
    assert operations.calls == ["inject:after_export", "retry:after_export"]


def test_fault_injection_workflow_fails_when_retry_duplicates_rows() -> None:
    class DuplicateOperations(FakeFaultOperations):
        def retry(self, stage: FaultInjectionStage) -> FaultInjectionSnapshot:
            snapshot = super().retry(stage)
            return FaultInjectionSnapshot(
                checkpoints=snapshot.checkpoints,
                row_count=snapshot.row_count,
                duplicate_rows=1,
                state_committed=snapshot.state_committed,
            )

    scenario = NativeTransferFaultInjectionScenario(
        run_id="run-duplicate",
        source_type="mssql",
        sink_type="clickhouse",
        strategy="full_refresh",
        failure_stage=FaultInjectionStage.DURING_LOAD,
        expected_rows=200,
        query_hash="query-a",
        schema_hash="schema-a",
    )

    result = NativeTransferFaultInjectionWorkflow().run(scenario, DuplicateOperations(calls=[]))

    assert not result.passed
    assert any(check["name"] == "no_duplicate_rows_after_retry" and not check["passed"] for check in result.checks)
