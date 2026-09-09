"""Additive acceptance must preserve the existing path-based validation plan."""

from tools.agent_policy.acceptance_plan import build_plan
from tools.agent_policy.select_checks import plan

from tests.test_acceptance_plan import BEFORE, DOMAIN, commit, git


def test_exact_plan_preserves_every_legacy_command_and_gate(tmp_path):
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.email", "synthetic@example.invalid")
    git(tmp_path, "config", "user.name", "Synthetic tests")
    base = commit(tmp_path, {DOMAIN: BEFORE})
    changes = {
        "src/dpone/runtime/connector_state.py": "# synthetic\n",
        "pyproject.toml": "# synthetic\n",
        ".github/workflows/ci.yml": "# synthetic\n",
        "packages/dpone-airflow-pack/synthetic.py": "# synthetic\n",
    }
    head = commit(tmp_path, changes)
    result = build_plan(tmp_path, base, head)
    assert result["legacy_validation"] == plan(changes)
    assert result["preserve_existing_required_checks"] is True
    assert result["classification"] == "broad"
    assert result["performance_soak"]["release_requirements_unchanged"] is True


def _workflow_jobs():
    from pathlib import Path

    import yaml

    return yaml.safe_load((Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml").read_text())["jobs"]


def test_acceptance_does_not_condition_existing_required_gates():
    jobs = _workflow_jobs()
    for name in ("quality-preflight", "quality-shards", "doctor-import-windows", "postgres-xmin"):
        assert "if" not in jobs[name]
        assert not any("acceptance" in str(dependency) for dependency in jobs[name].get("needs", []))
    assert jobs["quality-shards"]["needs"] == ["quality-preflight"]
    assert jobs["quality"]["needs"] == ["quality-shards"]
    steps = {step.get("name"): step for step in jobs["quality-preflight"]["steps"]}
    for name in (
        "Ruff lint",
        "Ruff format check",
        "Mypy",
        "Import rules",
        "Layer metrics",
        "Module size",
        "Workflow security policy",
        "Build documentation strictly",
        "Build packages",
    ):
        assert "if" not in steps[name]
    assert jobs["acceptance"]["if"] == "always()"


def test_smoke_is_synthetic_and_checks_actual_nonempty_junit():
    jobs = _workflow_jobs()
    smoke = jobs["bounded-window-smoke"]
    assert smoke["if"] == "needs.acceptance-plan.outputs.classification == 'broad'"
    assert smoke["services"]["postgres"]["image"] == "postgres:16-alpine"
    assert smoke["services"]["clickhouse"]["image"] == "clickhouse/clickhouse-server:24.8"
    assert smoke["env"]["DPONE_RUN_INTEGRATION"] == "1"
    assert smoke["env"]["DPONE_IT_CH_HTTP_PORT"] == "8123"
    for name in ("acceptance-contracts", "bounded-window-smoke"):
        scripts = "\n".join(step.get("run", "") for step in jobs[name]["steps"])
        assert "--junitxml=" in scripts
        assert "--verify-junit" in scripts
        assert '--require-test-file "$test_file"' in scripts
        assert "--head-ref" in scripts
        assert "continue-on-error" not in str(jobs[name])
    scripts = "\n".join(step.get("run", "") for step in smoke["steps"])
    for filename in (
        "test_postgres_window_source_integration.py",
        "test_clickhouse_window_target_integration.py",
        "test_bounded_window_route_integration.py",
    ):
        assert filename in scripts
    assert "benchmark" not in scripts and "soak" not in scripts


def test_acceptance_aggregate_rejects_failed_or_skipped_required_work():
    import os
    import subprocess

    script = _workflow_jobs()["acceptance"]["steps"][0]["run"]
    cases = [
        ("broad", "success", "success", "success", True),
        ("broad", "success", "success", "skipped", False),
        ("broad", "success", "success", "failure", False),
        ("schedule_only", "success", "success", "skipped", True),
        ("schedule_only", "success", "failure", "skipped", False),
        ("broad", "failure", "success", "success", False),
        ("unknown", "success", "success", "skipped", False),
    ]
    for classification, plan_result, contract_result, smoke_result, expected in cases:
        result = subprocess.run(
            ["bash", "-c", script],
            env={
                **os.environ,
                "CLASSIFICATION": classification,
                "PLAN_RESULT": plan_result,
                "CONTRACT_RESULT": contract_result,
                "SMOKE_RESULT": smoke_result,
            },
            capture_output=True,
        )
        assert (result.returncode == 0) == expected
