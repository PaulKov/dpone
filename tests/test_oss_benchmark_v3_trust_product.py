from __future__ import annotations

import csv
from pathlib import Path

from tools.oss_benchmark.budgets import evaluate_quality_budgets
from tools.oss_benchmark.certification import build_runtime_certification
from tools.oss_benchmark.claims import build_claims_ledger
from tools.oss_benchmark.exports import build_evidence_export_manifest, write_evidence_exports
from tools.oss_benchmark.renderers.budgets import render_quality_budgets_section
from tools.oss_benchmark.renderers.certification import render_runtime_certification_section
from tools.oss_benchmark.renderers.claims import render_claims_ledger_section


def _payload() -> dict:
    return {
        "schema_version": 2,
        "generated_at": "2026-06-30T12:00:00+00:00",
        "release_context": {
            "dpone_version": "0.62.1",
            "release_tag": "v0.62.1",
            "release_sha": "abc123",
            "branch": "codex/release-v0.62.1",
            "dirty": False,
            "resolved_by": "cli",
        },
        "quality_gates": {"status": "passed"},
        "benchmark_release_readiness": {"status": "passed"},
        "projects": [
            {
                "project_id": "dpone",
                "display_name": "dpone",
                "spec": {"slug": "dpone", "name": "dpone"},
                "metric_groups": {"loc_sloc": {"status": "fresh", "last_updated_at": "2026-06-30T12:00:00+00:00"}},
                "loc_without_tests": {"total_lines": 1000, "total_sloc": 820, "max_lines": 390, "max_sloc": 379},
                "coupling": {"avg_clustering": 0.17, "max_ce": 22},
                "industrial_maintainability": {"score": 96},
            }
        ],
    }


def test_claims_ledger_verifies_only_evidence_backed_claims(tmp_path: Path) -> None:
    payload = _payload()
    payload["runtime_certification"] = build_runtime_certification(payload, root=tmp_path)

    ledger = build_claims_ledger(payload, root=tmp_path)

    assert ledger["summary"]["verified"] >= 1
    assert all(claim["evidence_refs"] for claim in ledger["claims"])
    assert all(claim["status"] != "verified" or claim["missing_refs"] == [] for claim in ledger["claims"])
    assert "## Claims Ledger" in render_claims_ledger_section({"claims_ledger": ledger})


def test_claim_with_missing_artifact_is_unverified(tmp_path: Path) -> None:
    payload = _payload()
    payload["quality_gates"] = {"status": "passed"}
    payload["runtime_certification"] = {"scenarios": []}

    ledger = build_claims_ledger(payload, root=tmp_path)

    nested_claim = next(claim for claim in ledger["claims"] if claim["claim_id"] == "nested_lineage_runtime_contract")
    assert nested_claim["status"] == "unverified"
    assert nested_claim["confidence"] == 0
    assert nested_claim["missing_refs"]


def test_runtime_certification_matrix_reports_pass_and_fail(tmp_path: Path) -> None:
    payload = _payload()
    (tmp_path / "docs" / "nested-normalization.md").parent.mkdir(parents=True)
    (tmp_path / "docs" / "nested-normalization.md").write_text("parent_id root_id", encoding="utf-8")
    (tmp_path / "tests" / "test_nested_normalization_contracts.py").parent.mkdir(parents=True)
    (tmp_path / "tests" / "test_nested_normalization_contracts.py").write_text(
        "def test_nested(): pass", encoding="utf-8"
    )

    matrix = build_runtime_certification(payload, root=tmp_path)

    statuses = {scenario["scenario_id"]: scenario["status"] for scenario in matrix["scenarios"]}
    assert statuses["release_gate_certification"] == "passed"
    assert statuses["nested_lineage_contract"] == "passed"
    assert "## Runtime Certification Matrix" in render_runtime_certification_section({"runtime_certification": matrix})


def test_quality_budget_as_code_classifies_warning_and_failure(tmp_path: Path) -> None:
    budget_path = tmp_path / "quality_budgets.yml"
    budget_path.write_text(
        """
global:
  max_sloc: 400
  warn_sloc: 350
  max_avg_clustering: 0.180
layers:
  dpone:
    path_prefixes: ["src/dpone"]
    max_sloc: 400
    warn_sloc: 350
""",
        encoding="utf-8",
    )
    payload = _payload()
    budgets = evaluate_quality_budgets(payload, budget_path=budget_path)

    assert budgets["status"] == "warning"
    assert budgets["summary"]["warning"] >= 1
    assert budgets["summary"]["failed"] == 0
    assert "## Quality Budget As Code" in render_quality_budgets_section({"quality_budgets": budgets})

    payload["projects"][0]["loc_without_tests"]["max_sloc"] = 401
    failed = evaluate_quality_budgets(payload, budget_path=budget_path)
    assert failed["status"] == "failed"
    assert failed["summary"]["failed"] >= 1


def test_evidence_exports_are_bi_ready_csv_files(tmp_path: Path) -> None:
    payload = _payload()
    payload["claims_ledger"] = build_claims_ledger(payload, root=tmp_path)
    payload["runtime_certification"] = build_runtime_certification(payload, root=tmp_path)
    payload["quality_budgets"] = evaluate_quality_budgets(payload, budget_path=tmp_path / "missing.yml")
    payload["release_delta"] = {"projects": {"dpone": {"metrics": {}}}, "blocking_regressions": []}
    payload["evidence_exports"] = build_evidence_export_manifest()

    written = write_evidence_exports(payload, output_dir=tmp_path)

    assert {path.name for path in written} >= {"projects.csv", "claims.csv", "runtime_certification.csv"}
    with (tmp_path / "projects.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["schema_version"] == "2"
    assert rows[0]["release_version"] == "0.62.1"
    assert rows[0]["release_sha"] == "abc123"
