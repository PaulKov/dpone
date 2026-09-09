"""Contracts for bounded pre-release backfill certification."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "backfill-integration.yml"
SHADOW_TEST = ROOT / "tests" / "integration" / "backfill" / "test_postgres_mssql_shadow_initial_integration.py"


def test_workflow_exposes_exact_shadow_initial_smoke_profile() -> None:
    """Operators can run only the production-shaped ten-chunk proof."""

    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    triggers = workflow.get("on") or workflow.get(True)
    dispatch_inputs = triggers["workflow_dispatch"]["inputs"]

    assert dispatch_inputs["profile"]["default"] == "full_matrix"
    assert dispatch_inputs["profile"]["options"] == ["full_matrix", "shadow_initial_smoke"]
    assert dispatch_inputs["expected_commit_sha"]["default"] == ""

    job = workflow["jobs"]["backfill-integration"]
    steps = {step["name"]: step for step in job["steps"]}
    checkout = steps["Checkout"]["with"]
    assert checkout == {"fetch-depth": 0, "persist-credentials": False}
    assert job["env"]["CERT_PROFILE"] == "${{ github.event.inputs.profile || 'full_matrix' }}"

    binding = steps["Verify optional exact commit binding"]["run"]
    assert "refs/remotes/origin/master" in binding
    assert "${GITHUB_SHA}" in binding
    assert "workflow_dispatch" in binding
    assert "refs/heads/master" in binding

    assert steps["Install dependencies"]["run"] == "uv sync --locked --all-extras"
    services = steps["Start disposable local services"]["run"]
    assert '"${CERT_PROFILE}" = "shadow_initial_smoke"' in services
    assert "up -d --wait postgres mssql" in services

    execution = steps["Run backfill integration matrix"]["run"]
    assert "tests/integration/backfill/test_postgres_mssql_shadow_initial_integration.py" in execution
    assert "--min-passed 1" in execution
    assert "--max-skipped 0" in execution
    assert '--profile "${CERT_PROFILE}"' in execution
    assert "-m integration_backfill tests/integration/backfill tests/integration/airflow" in execution
    assert "|| true" not in execution


def test_shadow_smoke_is_bounded_to_ten_chunks_and_four_lanes() -> None:
    """The fast profile must not drift into a weaker one-lane fixture."""

    source = SHADOW_TEST.read_text(encoding="utf-8")
    assert '"parallel_workers": 4' in source
    assert '"max_chunks": 10' in source
    assert '"from": "1"' in source
    assert '"to": "10"' in source
    assert '"step": "1"' in source
    assert '"source_io_replayed": False' in source
    assert '"mssql_receipt_recovery"' in source
