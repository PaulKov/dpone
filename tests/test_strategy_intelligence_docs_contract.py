from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _doc(path: str) -> str:
    return (ROOT / "docs" / path).read_text(encoding="utf-8")


def test_strategy_intelligence_docs_cover_ux_architecture_and_runbooks() -> None:
    user_doc = _doc("strategy-intelligence.md")
    developer_doc = _doc("developer-strategy-intelligence.md")
    load_strategies = _doc("load-strategies.md")

    assert "dpone strategy advise" in user_doc
    assert "dpone plan --explain-strategy" in user_doc
    assert "dpone strategy preflight" in user_doc
    assert "dpone strategy repair-plan" in user_doc
    assert "dpone strategy certification-artifact" in user_doc
    assert "Strategy Advisor" in user_doc
    assert "Repair / resume UX" in user_doc
    assert "Native fast paths" in user_doc
    assert "dpone.strategy_intelligence" in developer_doc
    assert "StrategyAdvisor" in developer_doc
    assert "StrategyAutoCompiler" in developer_doc
    assert "Strategy Intelligence" in load_strategies
