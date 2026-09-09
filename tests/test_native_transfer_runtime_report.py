from __future__ import annotations

import json
from datetime import UTC, datetime

from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint, PartitionCheckpointStatus
from dpone.runtime.lineage.partition_resume import (
    PartitionResumeDecision,
    PartitionResumePlan,
    PlannedTransferPartition,
)
from dpone.strategy_intelligence.native_transfer_runtime_report import (
    NativeTransferRuntimeReport,
    NativeTransferRuntimeReportWriter,
)
from dpone.strategy_intelligence.partition_correctness import (
    PartitionCorrectnessObservation,
    PartitionCorrectnessService,
)


def _partition() -> PlannedTransferPartition:
    return PlannedTransferPartition(
        transfer_partition_id="p0",
        source_table="dbo.orders",
        target_table="analytics.orders",
        strategy="full_refresh",
        query_hash="query-a",
        schema_hash="schema-a",
        partition_bounds={"lower": 1, "upper": 100},
    )


def test_runtime_report_writer_persists_resume_and_correctness_evidence(tmp_path) -> None:
    partition = _partition()
    checkpoint = PartitionCheckpoint(
        transfer_partition_id="p0",
        status=PartitionCheckpointStatus.COMMITTED,
        query_hash="query-a",
        schema_hash="schema-a",
        source_table="dbo.orders",
        target_table="analytics.orders",
        partition_bounds={"lower": 1, "upper": 100},
        started_at=datetime(2026, 6, 9, tzinfo=UTC),
        diagnostics={"artifact_sha256": "sha256:abc"},
    )
    correctness = PartitionCorrectnessService().certify(
        (
            PartitionCorrectnessObservation(
                partition_id="p0",
                bounds={"lower": 1, "upper": 100},
                source_count=100,
                target_count=100,
                source_checksum="100:5050:100",
                target_checksum="100:5050:100",
                artifact_sha256="sha256:abc",
            ),
        )
    )
    report = NativeTransferRuntimeReport(
        run_id="run-1",
        source_type="mssql",
        sink_type="clickhouse",
        strategy="full_refresh",
        checkpoint_summary={"committed": 1},
        resume_plan=PartitionResumePlan(
            (PartitionResumeDecision(partition, "skip", "committed_matching_checkpoint", checkpoint),)
        ),
        partition_correctness=correctness,
    )

    json_path, md_path = NativeTransferRuntimeReportWriter(tmp_path).write(report)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = md_path.read_text(encoding="utf-8")
    assert payload["schema_version"] == "dpone.native_transfer.runtime_report.v1"
    assert payload["resume_plan"]["summary"] == {"skip": 1, "retry": 0}
    assert payload["partition_correctness"]["passed"] is True
    assert "Native transfer runtime report" in markdown
