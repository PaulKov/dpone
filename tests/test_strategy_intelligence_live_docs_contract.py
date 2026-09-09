from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _doc(path: str) -> str:
    return (ROOT / "docs" / path).read_text(encoding="utf-8")


def test_strategy_intelligence_live_execution_docs_cover_replay_and_adaptive_runtime() -> None:
    user_doc = _doc("strategy-intelligence.md")
    developer_doc = _doc("developer-strategy-intelligence.md")
    cli_ref = _doc("cli-reference.md")

    assert "dpone resync" in user_doc
    assert "dpone resume" in user_doc
    assert "Adaptive runtime batching" in user_doc
    assert "Live preflight" in user_doc
    assert "AdaptiveBatchController" in developer_doc
    assert "LivePreflightService" in developer_doc
    assert "ReplayExecutionService" in developer_doc
    assert "dpone resync" in cli_ref
    assert "dpone resume" in cli_ref
