"""Native transfer failure/resume certification models.

The certification layer is intentionally storage-neutral and connector-free. It
verifies the invariants that every native transfer implementation must satisfy:
non-committed partitions are retried, committed matching partitions may be
skipped, state is not advanced before success, and the retry result has no
duplicates or row-count gaps.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint, PartitionCheckpointStatus

SCHEMA_VERSION = "dpone.native_transfer.resume_evidence.v1"
RUNBOOK_PATH = "docs/source-sink/mssql-to-clickhouse.md#failure-resume-certification"
RUNBOOK_MD_LINK = "../../source-sink/mssql-to-clickhouse.md#failure-resume-certification"


@dataclass(frozen=True)
class NativeTransferResumeCertificationRequest:
    """Input for one failure/resume certification scenario."""

    run_id: str
    source_type: str
    sink_type: str
    strategy: str
    failure_stage: str
    expected_rows: int
    actual_rows_after_retry: int
    duplicate_rows_after_retry: int
    state_committed_before_retry: bool
    state_committed_after_retry: bool
    query_hash: str
    schema_hash: str
    checkpoints_before_retry: tuple[PartitionCheckpoint, ...]
    checkpoints_after_retry: tuple[PartitionCheckpoint, ...]


@dataclass(frozen=True)
class NativeTransferResumeCertificationResult:
    """Audit-friendly result for a failure/resume certification scenario."""

    schema_version: str
    created_at: str
    request: NativeTransferResumeCertificationRequest
    passed: bool
    safe_to_resume: bool
    skipped_partition_count: int
    retry_partition_count: int
    partition_summary: dict[str, dict[str, int]]
    checks: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["request"]["checkpoints_before_retry"] = [
            _checkpoint_to_dict(checkpoint) for checkpoint in self.request.checkpoints_before_retry
        ]
        payload["request"]["checkpoints_after_retry"] = [
            _checkpoint_to_dict(checkpoint) for checkpoint in self.request.checkpoints_after_retry
        ]
        payload["runbook"] = RUNBOOK_PATH
        return payload


class NativeTransferResumeCertificationService:
    """Evaluate native transfer failure/resume invariants."""

    def certify(
        self,
        request: NativeTransferResumeCertificationRequest,
    ) -> NativeTransferResumeCertificationResult:
        skipped_before = [
            checkpoint
            for checkpoint in request.checkpoints_before_retry
            if checkpoint.can_skip(query_hash=request.query_hash, schema_hash=request.schema_hash)
        ]
        retried_before = [
            checkpoint
            for checkpoint in request.checkpoints_before_retry
            if not checkpoint.can_skip(query_hash=request.query_hash, schema_hash=request.schema_hash)
        ]
        checks = (
            _check(
                "state_not_committed_before_retry",
                not request.state_committed_before_retry,
                "Source state must not advance after a failed transfer stage.",
            ),
            _check(
                "all_after_retry_partitions_committed",
                all(
                    checkpoint.can_skip(query_hash=request.query_hash, schema_hash=request.schema_hash)
                    for checkpoint in request.checkpoints_after_retry
                ),
                "Every partition must be committed with matching query/schema hashes after retry.",
            ),
            _check(
                "target_count_matches_expected",
                request.actual_rows_after_retry == request.expected_rows,
                "Target row count after retry must match the expected source row count.",
                expected=request.expected_rows,
                actual=request.actual_rows_after_retry,
            ),
            _check(
                "no_duplicate_rows_after_retry",
                request.duplicate_rows_after_retry == 0,
                "Retry must not create duplicate business/partition rows.",
                duplicates=request.duplicate_rows_after_retry,
            ),
            _check(
                "state_committed_after_success",
                request.state_committed_after_retry,
                "State must advance only after finalizer and quality/reconciliation checks pass.",
            ),
        )
        passed = all(bool(check["passed"]) for check in checks)
        return NativeTransferResumeCertificationResult(
            schema_version=SCHEMA_VERSION,
            created_at=datetime.now(UTC).isoformat(),
            request=request,
            passed=passed,
            safe_to_resume=bool(retried_before) and not request.state_committed_before_retry,
            skipped_partition_count=len(skipped_before),
            retry_partition_count=len(retried_before),
            partition_summary={
                "before": _summarize_checkpoints(request.checkpoints_before_retry),
                "after": _summarize_checkpoints(request.checkpoints_after_retry),
            },
            checks=checks,
        )


class NativeTransferResumeEvidenceWriter:
    """Write JSON and Markdown resume certification evidence."""

    def __init__(self, artifact_dir: str | Path) -> None:
        self._artifact_dir = Path(artifact_dir)

    def write(self, result: NativeTransferResumeCertificationResult) -> tuple[Path, Path]:
        self._artifact_dir.mkdir(parents=True, exist_ok=True)
        base = f"native_transfer_resume_{_safe_name(result.request.run_id)}"
        json_path = self._artifact_dir / f"{base}.json"
        md_path = self._artifact_dir / f"{base}.md"
        payload = result.to_dict()
        json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        md_path.write_text(_render_markdown(payload), encoding="utf-8")
        return json_path, md_path


def _check(name: str, passed: bool, message: str, **details: Any) -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "message": message,
        "details": dict(details),
    }


def _summarize_checkpoints(checkpoints: tuple[PartitionCheckpoint, ...]) -> dict[str, int]:
    counts = Counter(str(checkpoint.status) for checkpoint in checkpoints)
    return {status.value: counts.get(status.value, 0) for status in PartitionCheckpointStatus}


def _checkpoint_to_dict(checkpoint: PartitionCheckpoint) -> dict[str, Any]:
    return {
        "transfer_partition_id": checkpoint.transfer_partition_id,
        "status": str(checkpoint.status),
        "query_hash": checkpoint.query_hash,
        "schema_hash": checkpoint.schema_hash,
        "source_table": checkpoint.source_table,
        "target_table": checkpoint.target_table,
        "partition_bounds": dict(checkpoint.partition_bounds),
        "started_at": checkpoint.started_at.isoformat(),
        "completed_at": checkpoint.completed_at.isoformat() if checkpoint.completed_at else None,
        "rows_exported": checkpoint.rows_exported,
        "bytes_exported": checkpoint.bytes_exported,
        "diagnostics": dict(checkpoint.diagnostics),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    request = payload["request"]
    lines = [
        f"# Native transfer resume evidence: {request['run_id']}",
        "",
        f"- schema_version: `{payload['schema_version']}`",
        f"- source_type: `{request['source_type']}`",
        f"- sink_type: `{request['sink_type']}`",
        f"- strategy: `{request['strategy']}`",
        f"- failure_stage: `{request['failure_stage']}`",
        f"- passed: `{payload['passed']}`",
        f"- safe_to_resume: `{payload['safe_to_resume']}`",
        f"- skipped_partition_count: `{payload['skipped_partition_count']}`",
        f"- retry_partition_count: `{payload['retry_partition_count']}`",
        f"- runbook: [MSSQL -> ClickHouse resume runbook]({RUNBOOK_MD_LINK})",
        "",
        "## Checks",
        "",
        "| Check | Passed | Details |",
        "|---|---:|---|",
        *[
            f"| `{check['name']}` | `{check['passed']}` | `{_details(check.get('details') or {})}` |"
            for check in payload["checks"]
        ],
        "",
        "## Partition summary",
        "",
        "| Stage | Planned | Exported | Loaded | Finalized | Committed | Failed |",
        "|---|---:|---:|---:|---:|---:|---:|",
        _summary_row("before", payload["partition_summary"]["before"]),
        _summary_row("after", payload["partition_summary"]["after"]),
        "",
    ]
    return "\n".join(lines)


def _summary_row(stage: str, summary: dict[str, int]) -> str:
    return (
        f"| {stage} | {summary.get('planned', 0)} | {summary.get('exported', 0)} | "
        f"{summary.get('loaded', 0)} | {summary.get('finalized', 0)} | "
        f"{summary.get('committed', 0)} | {summary.get('failed', 0)} |"
    )


def _details(details: dict[str, Any]) -> str:
    if not details:
        return "n/a"
    return ", ".join(f"{key}={value}" for key, value in details.items())


def _safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)
