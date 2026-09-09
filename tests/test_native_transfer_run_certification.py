from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from dpone.contracts.process_types import ProcessResult
from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint, PartitionCheckpointStatus
from dpone.runtime.lineage.partition_checkpoint_store import JsonlPartitionCheckpointStore
from dpone.services.run_manifest import RunManifestResult
from dpone.strategy_intelligence.run_certification import (
    NativeTransferRunCertificationRequest,
    NativeTransferRunCertificationService,
    NativeTransferRunEvidenceWriter,
)


def _run_result(*, run_id: str, inserted_rows: int, final_rows: int = 2) -> RunManifestResult:
    return RunManifestResult(
        manifest="mssql_to_clickhouse.yml",
        process="mssql_to_clickhouse",
        selector=None,
        run_id=run_id,
        passed=True,
        result=ProcessResult(
            status="success",
            inserted_rows=inserted_rows,
            updated_rows=0,
            final_rows=final_rows,
            extracted_rows=final_rows,
            duration_seconds=0.25,
            errors=[],
        ),
    )


def _checkpoint(partition_id: str) -> PartitionCheckpoint:
    return PartitionCheckpoint(
        transfer_partition_id=partition_id,
        status=PartitionCheckpointStatus.COMMITTED,
        query_hash="query-a",
        schema_hash="schema-a",
        source_table="dbo.orders",
        target_table="analytics.orders",
        partition_bounds={"partition": partition_id},
        started_at=datetime(2026, 6, 9, tzinfo=UTC),
        completed_at=datetime(2026, 6, 9, 0, 1, tzinfo=UTC),
        rows_exported=1,
        bytes_exported=128,
        diagnostics={"artifact_sha256": f"sha256:{partition_id}"},
    )


@dataclass
class _RunHarness:
    checkpoint_store: JsonlPartitionCheckpointStore
    report_dir: Path
    calls: int = 0

    def __call__(self, run_id: str) -> RunManifestResult:
        self.calls += 1
        if self.calls == 1:
            self.checkpoint_store.upsert_many((_checkpoint("p0"), _checkpoint("p1")))
            self._write_runtime_report(run_id, skip=0, retry=2, inserted_rows=2)
            return _run_result(run_id=run_id, inserted_rows=2)
        self._write_runtime_report(run_id, skip=2, retry=0, inserted_rows=0)
        return _run_result(run_id=run_id, inserted_rows=0)

    def _write_runtime_report(self, run_id: str, *, skip: int, retry: int, inserted_rows: int) -> None:
        self.report_dir.mkdir(parents=True, exist_ok=True)
        (self.report_dir / f"native_transfer_runtime_{run_id}.json").write_text(
            json.dumps(
                {
                    "schema_version": "dpone.native_transfer.runtime_report.v1",
                    "run_id": run_id,
                    "resume_plan": {"summary": {"skip": skip, "retry": retry}, "decisions": []},
                    "load_result": {"inserted_rows": inserted_rows, "total_rows": 2},
                    "checkpoint_summary": self.checkpoint_store.summary(),
                    "passed": True,
                }
            ),
            encoding="utf-8",
        )


def test_run_certification_proves_second_dpone_run_skips_committed_partitions(tmp_path: Path) -> None:
    checkpoint_store = JsonlPartitionCheckpointStore(tmp_path / "partition_checkpoints.jsonl")
    harness = _RunHarness(checkpoint_store=checkpoint_store, report_dir=tmp_path / ".dpone" / "runs")
    result = NativeTransferRunCertificationService().certify(
        NativeTransferRunCertificationRequest(
            manifest_path=Path("mssql_to_clickhouse.yml"),
            run_once=harness,
            runtime_report_dir=tmp_path / ".dpone" / "runs",
            checkpoint_store=checkpoint_store,
            first_run_id="first",
            second_run_id="second",
        )
    )

    assert result.passed is True
    assert result.first_run.result.inserted_rows == 2
    assert result.second_run.result.inserted_rows == 0
    assert result.second_resume_summary == {"skip": 2, "retry": 0}
    assert result.checks_by_name["first_run_passed"]["passed"] is True
    assert result.checks_by_name["second_run_noop_skip"]["passed"] is True
    assert harness.calls == 2


def test_run_certification_accepts_runtime_reports_named_by_lineage_run_id(tmp_path: Path) -> None:
    checkpoint_store = JsonlPartitionCheckpointStore(tmp_path / "partition_checkpoints.jsonl")
    report_dir = tmp_path / ".dpone" / "runs"
    calls = 0

    def run_once(run_id: str) -> RunManifestResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            checkpoint_store.upsert_many((_checkpoint("p0"), _checkpoint("p1")))
            _write_runtime_report(report_dir, "01LINEAGEFIRST", skip=0, retry=2, inserted_rows=2)
            return _run_result(run_id=run_id, inserted_rows=2)
        _write_runtime_report(report_dir, "01LINEAGESECOND", skip=2, retry=0, inserted_rows=0)
        return _run_result(run_id=run_id, inserted_rows=0)

    result = NativeTransferRunCertificationService().certify(
        NativeTransferRunCertificationRequest(
            manifest_path=Path("mssql_to_clickhouse.yml"),
            run_once=run_once,
            runtime_report_dir=report_dir,
            checkpoint_store=checkpoint_store,
            first_run_id="cli-first",
            second_run_id="cli-second",
        )
    )

    assert result.passed is True
    assert result.first_runtime_report["run_id"] == "01LINEAGEFIRST"
    assert result.second_runtime_report["run_id"] == "01LINEAGESECOND"
    assert result.second_resume_summary == {"skip": 2, "retry": 0}


def test_run_certification_fails_when_second_run_does_not_skip(tmp_path: Path) -> None:
    checkpoint_store = JsonlPartitionCheckpointStore(tmp_path / "partition_checkpoints.jsonl")
    harness = _RunHarness(checkpoint_store=checkpoint_store, report_dir=tmp_path / ".dpone" / "runs")

    def run_once(run_id: str) -> RunManifestResult:
        result = harness(run_id)
        if run_id == "second":
            harness._write_runtime_report(run_id, skip=1, retry=1, inserted_rows=1)
            return _run_result(run_id=run_id, inserted_rows=1)
        return result

    result = NativeTransferRunCertificationService().certify(
        NativeTransferRunCertificationRequest(
            manifest_path=Path("mssql_to_clickhouse.yml"),
            run_once=run_once,
            runtime_report_dir=tmp_path / ".dpone" / "runs",
            checkpoint_store=checkpoint_store,
            first_run_id="first",
            second_run_id="second",
        )
    )

    assert result.passed is False
    assert result.checks_by_name["second_run_noop_skip"]["passed"] is False


def test_run_evidence_writer_persists_json_and_markdown(tmp_path: Path) -> None:
    checkpoint_store = JsonlPartitionCheckpointStore(tmp_path / "partition_checkpoints.jsonl")
    harness = _RunHarness(checkpoint_store=checkpoint_store, report_dir=tmp_path / ".dpone" / "runs")
    result = NativeTransferRunCertificationService().certify(
        NativeTransferRunCertificationRequest(
            manifest_path=Path("mssql_to_clickhouse.yml"),
            run_once=harness,
            runtime_report_dir=tmp_path / ".dpone" / "runs",
            checkpoint_store=checkpoint_store,
            first_run_id="first",
            second_run_id="second",
        )
    )

    json_path, md_path = NativeTransferRunEvidenceWriter(tmp_path / "evidence").write(result)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = md_path.read_text(encoding="utf-8")
    assert payload["schema_version"] == "dpone.native_transfer.run_certification.v1"
    assert payload["passed"] is True
    assert "Native transfer dpone run certification" in markdown


def _write_runtime_report(path: Path, run_id: str, *, skip: int, retry: int, inserted_rows: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / f"native_transfer_runtime_{run_id}.json").write_text(
        json.dumps(
            {
                "schema_version": "dpone.native_transfer.runtime_report.v1",
                "run_id": run_id,
                "resume_plan": {"summary": {"skip": skip, "retry": retry}, "decisions": []},
                "load_result": {"inserted_rows": inserted_rows, "total_rows": 2},
                "checkpoint_summary": {},
                "passed": True,
            }
        ),
        encoding="utf-8",
    )
