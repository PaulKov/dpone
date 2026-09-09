from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_github_ci_runs_postgres_xmin_integration_job() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))

    preflight = workflow["jobs"]["quality-preflight"]
    assert preflight["env"]["DPONE_TEST_USE_INSTALLED_PACKAGE"] == "1"

    job = workflow["jobs"]["postgres-xmin"]

    assert job["env"]["DPONE_TEST_USE_INSTALLED_PACKAGE"] == "1"
    assert job["services"]["postgres"]["image"] == "postgres:16-alpine"
    assert job["env"]["DPONE_RUN_INTEGRATION"] == "1"
    assert job["env"]["DPONE_IT_PG_HOST"] == "127.0.0.1"
    script = "\n".join(step.get("run", "") for step in job["steps"])
    assert "uv sync --locked --all-extras" in script
    assert "pytest -m integration_postgres_xmin tests/integration/postgres" in script
