from __future__ import annotations

from pathlib import Path

import yaml

from dpone.ops.live_certification import LiveCertificationAutomationService

ROOT = Path(__file__).resolve().parents[1]


def test_vendor_live_plan_exposes_backfill_matrix_as_first_class_step(tmp_path: Path) -> None:
    report = LiveCertificationAutomationService().build(
        output_dir=tmp_path / "vendor-certification",
        profile="vendor_live",
        row_count=25000,
        include_vendor_live=True,
    )

    backfill_step = next(step for step in report.steps if step.name == "run_vendor_live_backfill_matrix")
    assert "DPONE_MATRIX_RUN_MODE=vendor_live" in backfill_step.command
    assert "DPONE_MATRIX_STRATEGY=backfill" in backfill_step.command
    assert "DPONE_MATRIX_ROW_COUNT=25000" in backfill_step.command
    assert "pytest -m integration_matrix tests/integration/matrix" in backfill_step.command
    assert "backfill_vendor_live_junit.xml" in backfill_step.artifacts
    assert "backfill_vendor_live_junit.xml" in report.required_artifacts


def test_live_certification_workflow_runs_vendor_live_backfill_matrix_only_on_opt_in() -> None:
    workflow = yaml.safe_load((ROOT / ".github" / "workflows" / "live-certification.yml").read_text(encoding="utf-8"))
    inputs = (workflow.get("on") or workflow.get(True))["workflow_dispatch"]["inputs"]

    assert inputs["run_vendor_live"]["default"] is False
    assert "vendor_backfill_source_filter" in inputs
    assert "vendor_backfill_sink_filter" in inputs
    assert "vendor_backfill_case_id_filter" in inputs

    vendor_job = workflow["jobs"]["vendor-live-certification"]
    assert vendor_job["if"] == "${{ inputs.profile == 'vendor_live' && inputs.run_vendor_live == true }}"
    env = vendor_job["env"]
    assert env["DPONE_MATRIX_RUN_MODE"] == "vendor_live"
    assert env["DPONE_MATRIX_STRATEGY"] == "backfill"
    assert env["DPONE_MATRIX_ARTIFACT_DIR"] == "test_artifacts/live_certification_vendor/backfill_matrix"

    script = "\n".join(step.get("run", "") for step in vendor_job["steps"])
    assert "Run vendor live backfill matrix" in {step.get("name", "") for step in vendor_job["steps"]}
    assert "pytest -m integration_matrix tests/integration/matrix" in script
    assert "backfill_vendor_live_junit.xml" in script
