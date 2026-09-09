from __future__ import annotations

import json
from datetime import UTC, datetime

from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint, PartitionCheckpointStatus
from dpone.strategy_intelligence.resume_certification import (
    NativeTransferResumeCertificationRequest,
    NativeTransferResumeCertificationService,
    NativeTransferResumeEvidenceWriter,
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


def test_resume_certification_passes_when_retry_commits_without_duplicates() -> None:
    request = NativeTransferResumeCertificationRequest(
        run_id="run-10m",
        source_type="mssql",
        sink_type="clickhouse",
        strategy="full_refresh",
        failure_stage="after_export",
        expected_rows=200,
        actual_rows_after_retry=200,
        duplicate_rows_after_retry=0,
        state_committed_before_retry=False,
        state_committed_after_retry=True,
        query_hash="query-a",
        schema_hash="schema-a",
        checkpoints_before_retry=(
            _checkpoint(PartitionCheckpointStatus.EXPORTED, 0),
            _checkpoint(PartitionCheckpointStatus.FAILED, 1),
        ),
        checkpoints_after_retry=(
            _checkpoint(PartitionCheckpointStatus.COMMITTED, 0),
            _checkpoint(PartitionCheckpointStatus.COMMITTED, 1),
        ),
    )

    result = NativeTransferResumeCertificationService().certify(request)

    assert result.passed
    assert result.safe_to_resume
    assert result.retry_partition_count == 2
    assert result.skipped_partition_count == 0
    assert result.partition_summary["after"]["committed"] == 2
    assert all(check["passed"] for check in result.checks)


def test_resume_certification_can_skip_only_committed_matching_partitions() -> None:
    request = NativeTransferResumeCertificationRequest(
        run_id="run-partial",
        source_type="mssql",
        sink_type="clickhouse",
        strategy="incremental_append",
        failure_stage="during_load",
        expected_rows=200,
        actual_rows_after_retry=200,
        duplicate_rows_after_retry=0,
        state_committed_before_retry=False,
        state_committed_after_retry=True,
        query_hash="query-a",
        schema_hash="schema-a",
        checkpoints_before_retry=(
            _checkpoint(PartitionCheckpointStatus.COMMITTED, 0),
            _checkpoint(PartitionCheckpointStatus.LOADED, 1),
        ),
        checkpoints_after_retry=(
            _checkpoint(PartitionCheckpointStatus.COMMITTED, 0),
            _checkpoint(PartitionCheckpointStatus.COMMITTED, 1),
        ),
    )

    result = NativeTransferResumeCertificationService().certify(request)

    assert result.passed
    assert result.skipped_partition_count == 1
    assert result.retry_partition_count == 1


def test_resume_certification_fails_when_state_committed_before_success() -> None:
    request = NativeTransferResumeCertificationRequest(
        run_id="run-bad-state",
        source_type="mssql",
        sink_type="clickhouse",
        strategy="full_refresh",
        failure_stage="before_finalizer",
        expected_rows=200,
        actual_rows_after_retry=200,
        duplicate_rows_after_retry=0,
        state_committed_before_retry=True,
        state_committed_after_retry=True,
        query_hash="query-a",
        schema_hash="schema-a",
        checkpoints_before_retry=(
            _checkpoint(PartitionCheckpointStatus.FINALIZED, 0),
            _checkpoint(PartitionCheckpointStatus.FINALIZED, 1),
        ),
        checkpoints_after_retry=(
            _checkpoint(PartitionCheckpointStatus.COMMITTED, 0),
            _checkpoint(PartitionCheckpointStatus.COMMITTED, 1),
        ),
    )

    result = NativeTransferResumeCertificationService().certify(request)

    assert not result.passed
    assert any(check["name"] == "state_not_committed_before_retry" and not check["passed"] for check in result.checks)


def test_resume_evidence_writer_persists_json_and_markdown(tmp_path) -> None:
    request = NativeTransferResumeCertificationRequest(
        run_id="run-evidence",
        source_type="mssql",
        sink_type="clickhouse",
        strategy="full_refresh",
        failure_stage="after_export",
        expected_rows=100,
        actual_rows_after_retry=100,
        duplicate_rows_after_retry=0,
        state_committed_before_retry=False,
        state_committed_after_retry=True,
        query_hash="query-a",
        schema_hash="schema-a",
        checkpoints_before_retry=(_checkpoint(PartitionCheckpointStatus.EXPORTED, 0),),
        checkpoints_after_retry=(_checkpoint(PartitionCheckpointStatus.COMMITTED, 0),),
    )
    result = NativeTransferResumeCertificationService().certify(request)

    json_path, md_path = NativeTransferResumeEvidenceWriter(tmp_path).write(result)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = md_path.read_text(encoding="utf-8")
    assert payload["schema_version"] == "dpone.native_transfer.resume_evidence.v1"
    assert payload["passed"] is True
    assert payload["retry_partition_count"] == 1
    assert "Native transfer resume evidence" in markdown
    assert "mssql" in markdown
