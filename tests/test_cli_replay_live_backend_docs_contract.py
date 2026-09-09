from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_cli_reference_documents_live_replay_flags() -> None:
    docs = (ROOT / "docs" / "cli-reference.md").read_text(encoding="utf-8")

    assert "dpone resync" in docs
    assert "--live-backend" in docs
    assert "--connection-id" in docs
    assert "--target-table" in docs


def test_replay_runbook_documents_live_backend_mode() -> None:
    docs = (ROOT / "docs" / "testing" / "replay-integration.md").read_text(encoding="utf-8")

    assert "Live backend CLI mode" in docs
    assert "dpone resync" in docs
    assert "RuntimeReplayBackendFactory" in docs
