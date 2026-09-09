from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_mkdocs_config_renders_mermaid_fences_as_diagrams() -> None:
    config = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")

    assert "pymdownx.superfences:" in config
    assert "custom_fences:" in config
    assert "name: mermaid" in config
    assert "class: mermaid" in config
    assert "!!python/name" not in config


def test_docs_contain_mermaid_diagrams_that_pages_must_render() -> None:
    mermaid_docs = [path for path in (ROOT / "docs").rglob("*.md") if "```mermaid" in path.read_text(encoding="utf-8")]

    assert mermaid_docs
