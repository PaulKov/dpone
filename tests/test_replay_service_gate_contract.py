from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_service_backed_replay_marker_is_declared() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert "integration_replay_services:" in pyproject


def test_replay_workflow_can_run_service_backed_gate() -> None:
    workflow = (ROOT / ".github" / "workflows" / "replay-integration.yml").read_text(encoding="utf-8")

    assert "run_service_backed_gate" in workflow
    assert "DPONE_RUN_INTEGRATION_REPLAY_SERVICES" in workflow
    assert "docker/docker-compose.integration.yml" in workflow
    assert "msodbcsql18" in workflow
    assert "postgres kafka schema-registry clickhouse mssql" in workflow
    assert "pytest -m integration_replay_services tests/integration/replay" in workflow


def test_replay_service_runbook_documents_local_services() -> None:
    docs = (ROOT / "docs" / "testing" / "replay-integration.md").read_text(encoding="utf-8")

    assert "Service-backed replay gate" in docs
    assert "DPONE_RUN_INTEGRATION_REPLAY_SERVICES=1" in docs
    assert "docker compose -f docker/docker-compose.integration.yml up -d" in docs
    assert "ClickHouse" in docs
    assert "MSSQL" in docs
    assert "integration_replay_services" in docs
