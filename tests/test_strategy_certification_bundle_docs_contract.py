from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_certification_suite_docs_describe_strategy_bundle() -> None:
    docs = (ROOT / "docs" / "certification-suite.md").read_text(encoding="utf-8")

    assert "Strategy certification evidence bundle" in docs
    assert "dpone strategy certification-bundle" in docs
    assert "dpone.strategy.certification_bundle.v1" in docs
    assert "--strategy-certification-bundle" in docs
    assert "--require-strategy-certification" in docs


def test_cli_reference_documents_strategy_certification_bundle() -> None:
    docs = (ROOT / "docs" / "cli-reference.md").read_text(encoding="utf-8")

    assert "dpone strategy certification-bundle" in docs
    assert "--replay-evidence" in docs
    assert "--matrix-artifact" in docs
