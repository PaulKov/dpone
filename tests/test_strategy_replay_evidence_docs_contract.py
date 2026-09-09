from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_replay_runbook_documents_evidence_artifacts() -> None:
    docs = (ROOT / "docs" / "testing" / "replay-integration.md").read_text(encoding="utf-8")

    assert "Replay evidence artifacts" in docs
    assert "_evidence.json" in docs
    assert "_evidence.md" in docs
    assert "row_count_checks" in docs
    assert "status_checks" in docs


def test_developer_docs_describe_replay_evidence_writer() -> None:
    docs = (ROOT / "docs" / "developer-strategy-intelligence.md").read_text(encoding="utf-8")

    assert "ReplayEvidenceWriter" in docs
    assert "dpone.replay.evidence.v1" in docs
