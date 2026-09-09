from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_replay_integration_marker_is_declared() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert "integration_replay:" in pyproject


def test_replay_integration_workflow_is_manual_and_gated() -> None:
    workflow = ROOT / ".github" / "workflows" / "replay-integration.yml"

    assert workflow.exists()

    content = workflow.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in content
    assert "DPONE_RUN_INTEGRATION_REPLAY" in content
    assert "pytest -m integration_replay tests/integration/replay" in content
    assert "test_artifacts/replay_integration" in content


def test_replay_integration_runbook_is_in_docs_nav() -> None:
    runbook = ROOT / "docs" / "testing" / "replay-integration.md"
    mkdocs = ROOT / "mkdocs.yml"

    assert runbook.exists()

    content = runbook.read_text(encoding="utf-8")

    assert "DPONE_RUN_INTEGRATION_REPLAY=1" in content
    assert "integration_replay" in content
    assert "Runbook" in content
    assert "test_artifacts/replay_integration" in content

    assert "testing/replay-integration.md" in mkdocs.read_text(encoding="utf-8")
