from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutionResult


def test_safe_sample_runtime_evidence_writer_writes_redacted_artifact_atomically(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.safe_sample_runtime_evidence import SafeSampleRuntimeEvidenceWriter

    result = SafeSampleRuntimeExecutionResult(
        execution_status="failed",
        data_outcome="unknown",
        release_id="sha256:" + "a" * 64,
        deployment_id="sha256:" + "b" * 64,
        init_fetch={"schema": "dpone.init-fetch-result.v1", "passed": True},
        temporary_target_prepare={"schema": "dpone.temporary-target-lifecycle.v1", "status": "prepared"},
        data_copy={
            "schema": "dpone.safe-sample-data-copy.v1",
            "status": "failed",
            "source_request": {
                "schema": "dpone.safe-sample-source-request.v1",
                "status": "planned",
                "mode": "pushdown",
                "sample_rows": 1000,
                "max_bytes": 1024**3,
                "timeout_seconds": 60,
                "source_read_only": True,
                "full_scan_allowed": False,
                "pii_policy": "masked",
                "target": {
                    "mode": "temporary",
                    "connection_ref": "clickhouse_dev",
                    "temporary_table": {"schema": "dpone_tmp_development", "name": "orders_daily_abc123"},
                    "ttl_seconds": 86400,
                },
                "errors": [],
            },
            "rows_read": 0,
            "rows_written": 0,
            "bytes_read": 0,
            "pii_policy": "masked",
            "diagnostics": {
                "credential_resolution": {
                    "connection_ref": "mssql_dev",
                    "resolver": "vault_kv",
                    "resolved_version": 17,
                }
            },
            "token": "must-not-leak",
            "nested": {"password": "must-not-leak"},
            "errors": [
                {
                    "schema": "dpone.error.v1",
                    "code": "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED",
                    "stage": "data_copy",
                    "severity": "error",
                    "message": "copy failed with token=must-not-leak and password=also-must-not-leak",
                }
            ],
        },
        temporary_target_cleanup={"schema": "dpone.temporary-target-lifecycle.v1", "status": "cleaned"},
        errors=(),
        source_snapshot={
            "pipeline_id": "orders_daily",
            "path": "pipelines/orders_daily/pipeline.yaml",
            "sha256": "sha256:" + "f" * 64,
        },
    )

    report = SafeSampleRuntimeEvidenceWriter().write(result, tmp_path / "evidence")

    payload_path = tmp_path / "evidence" / "safe-sample-runtime-execution.json"
    payload_bytes = payload_path.read_bytes()
    payload = json.loads(payload_bytes)
    write_report = report.to_dict()
    runtime_schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-runtime-execution.schema.json").read_text(encoding="utf-8")
    )
    write_schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-runtime-evidence-write.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(payload, runtime_schema)
    jsonschema.validate(write_report, write_schema)
    assert payload["schema"] == "dpone.safe-sample-runtime-execution.v1"
    assert payload["release_id"] == "sha256:" + "a" * 64
    assert payload["deployment_id"] == "sha256:" + "b" * 64
    assert payload["source_snapshot"] == {
        "pipeline_id": "orders_daily",
        "path": "pipelines/orders_daily/pipeline.yaml",
        "sha256": "sha256:" + "f" * 64,
    }
    assert payload["data_copy"]["diagnostics"]["credential_resolution"] == {
        "connection_ref": "mssql_dev",
        "resolver": "vault_kv",
        "resolved_version": 17,
    }
    assert write_report == {
        "schema": "dpone.safe-sample-runtime-evidence-write.v1",
        "path": "$OUTPUT_ROOT/safe-sample-runtime-execution.json",
        "sha256": "sha256:" + hashlib.sha256(payload_bytes).hexdigest(),
        "bytes": len(payload_bytes),
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
    }
    assert str(tmp_path) not in repr(write_report)
    assert str(Path.home()) not in repr(write_report)
    assert not list((tmp_path / "evidence").glob("*.tmp"))
    artifact_text = payload_path.read_text(encoding="utf-8")
    assert "must-not-leak" not in artifact_text
    assert "also-must-not-leak" not in artifact_text
    assert "details redacted" in artifact_text


def test_safe_sample_runtime_evidence_writer_never_replaces_existing_run_evidence(tmp_path: Path) -> None:
    from dpone.services.safe_sample_runtime_evidence import SafeSampleRuntimeEvidenceWriter

    output_dir = tmp_path / "evidence"
    writer = SafeSampleRuntimeEvidenceWriter()
    first = {"execution_status": "succeeded", "data_outcome": "passed", "marker": "first"}
    second = {"execution_status": "failed", "data_outcome": "unknown", "marker": "second"}

    writer.write(first, output_dir)
    original = (output_dir / "safe-sample-runtime-execution.json").read_bytes()

    with pytest.raises(FileExistsError):
        writer.write(second, output_dir)

    assert (output_dir / "safe-sample-runtime-execution.json").read_bytes() == original
    assert not list(output_dir.glob("*.tmp"))


@pytest.mark.parametrize(
    "filename",
    (
        "../outside.json",
        "/tmp/outside.json",
        "./evidence.json",
        "nested//evidence.json",
        "nested/evidence report.json",
        "C:\\temp\\evidence.json",
        " evidence.json",
    ),
)
def test_safe_sample_runtime_evidence_writer_rejects_unsafe_filename(
    tmp_path: Path,
    filename: str,
) -> None:
    from dpone.services.safe_sample_runtime_evidence import SafeSampleRuntimeEvidenceWriter

    with pytest.raises(ValueError, match="safe relative path"):
        SafeSampleRuntimeEvidenceWriter().write(
            {"execution_status": "succeeded", "data_outcome": "passed"},
            tmp_path / "evidence",
            filename=filename,
        )

    assert not (tmp_path / "outside.json").exists()
