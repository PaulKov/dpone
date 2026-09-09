from __future__ import annotations

import hashlib
import json
from pathlib import Path

from dpone.ops.certification_artifacts import CertificationArtifactReader
from dpone.strategy_intelligence.certification_bundle import (
    CertificationEvidenceInput,
    StrategyCertificationEvidenceBundleWriter,
)


def test_strategy_certification_bundle_writer_aggregates_evidence_files(tmp_path: Path) -> None:
    replay = _write_json(
        tmp_path / "resync_01_evidence.json",
        {
            "schema_version": "dpone.replay.evidence.v1",
            "status": "executed",
            "state_committed": True,
            "status_checks": [{"name": "state_commit", "status": "passed"}],
        },
    )
    matrix = _write_json(
        tmp_path / "certification_report.json",
        {
            "profile": "mock_contract",
            "passed": True,
            "case_count": 200,
            "results": [],
        },
    )
    connector = _write_json(
        tmp_path / "connector_certification.json",
        {
            "passed": True,
            "connectors": {"postgres": {"incremental_merge": {"status": "pass"}}},
        },
    )
    benchmark = _write_json(
        tmp_path / "benchmark_baseline.json",
        {
            "passed": True,
            "metrics": {"throughput_rows_per_second": 100000},
        },
    )
    native_transfer = _write_json(
        tmp_path / "evidence_index.json",
        {
            "schema_version": "dpone.native_transfer.evidence.v1",
            "run_id": "01J00000000000000000000000",
            "contract": {
                "route": "postgres_to_mssql",
                "strategy": "cdc_apply",
                "state_commit_gate": "after_target_finalize_and_quality",
            },
            "artifacts": [
                {"name": "typed_reconciliation.json", "sha256": "a" * 64, "bytes": 123},
                {"name": "state_transition.json", "sha256": "b" * 64, "bytes": 456},
            ],
        },
    )

    artifact = StrategyCertificationEvidenceBundleWriter(tmp_path).write(
        CertificationEvidenceInput(
            bundle_id="oss_rc_2026_06_05",
            replay_evidence=(replay,),
            matrix_artifacts=(matrix,),
            connector_artifacts=(connector,),
            benchmark_artifacts=(benchmark,),
            native_transfer_evidence=(native_transfer,),
            docs_links=("docs/testing/replay-integration.md", "docs/certification-suite.md"),
        )
    )

    assert artifact.json_path == tmp_path / "strategy_certification_bundle.json"
    assert artifact.markdown_path == tmp_path / "strategy_certification_bundle.md"
    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))

    assert payload["schema_version"] == "dpone.strategy.certification_bundle.v1"
    assert payload["evidence_status"] == "PASS"
    assert payload["bundle_id"] == "oss_rc_2026_06_05"
    assert payload["passed"] is True
    assert payload["blockers"] == []
    assert payload["summary"] == {
        "total_items": 5,
        "present_items": 5,
        "passed_items": 5,
        "missing_items": 0,
        "failed_items": 0,
    }
    assert payload["docs_links"] == ["docs/testing/replay-integration.md", "docs/certification-suite.md"]
    assert payload["evidence_items"][0]["kind"] == "replay"
    assert payload["evidence_items"][0]["sha256"] == _sha256(replay)
    assert payload["evidence_items"][1]["kind"] == "matrix"
    assert payload["evidence_items"][1]["summary"]["case_count"] == 200
    native_item = payload["evidence_items"][4]
    assert native_item["kind"] == "native_transfer"
    assert native_item["summary"] == {
        "route": "postgres_to_mssql",
        "strategy": "cdc_apply",
        "artifact_count": 2,
        "state_commit_gate": "after_target_finalize_and_quality",
    }

    markdown = artifact.markdown_path.read_text(encoding="utf-8")
    assert "# Strategy certification evidence bundle: oss_rc_2026_06_05" in markdown
    assert "| replay | present | passed |" in markdown
    assert "| matrix | present | passed |" in markdown
    assert "| native_transfer | present | passed |" in markdown
    assert "docs/testing/replay-integration.md" in markdown
    assert (
        CertificationArtifactReader().read(name="strategy_bundle", path=artifact.json_path, required=True).passed
        is True
    )


def test_strategy_certification_bundle_marks_missing_required_artifact_as_blocker(tmp_path: Path) -> None:
    missing = tmp_path / "missing_replay_evidence.json"

    artifact = StrategyCertificationEvidenceBundleWriter(tmp_path).write(
        CertificationEvidenceInput(bundle_id="missing_case", replay_evidence=(missing,))
    )

    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))

    assert payload["passed"] is False
    assert payload["evidence_status"] == "FAIL"
    assert payload["blockers"] == ["replay.missing:missing_replay_evidence.json"]
    assert payload["summary"]["missing_items"] == 1


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
