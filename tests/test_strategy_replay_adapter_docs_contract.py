from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _doc(path: str) -> str:
    return (ROOT / "docs" / path).read_text(encoding="utf-8")


def test_replay_adapter_docs_explain_sequence_and_state_commit_safety() -> None:
    user_doc = _doc("strategy-intelligence.md")
    developer_doc = _doc("developer-strategy-intelligence.md")

    assert "Replay adapters" in user_doc
    assert "validate staging" in user_doc
    assert "execute finalizer" in user_doc
    assert "commit state" in user_doc
    assert "ReplayBackend" in developer_doc
    assert "DbReplayAdapter" in developer_doc
    assert "KafkaReplayAdapter" in developer_doc
    assert "state commit" in developer_doc
