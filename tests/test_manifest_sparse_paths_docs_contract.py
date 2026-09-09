from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_manifest_sparse_paths_user_and_developer_docs_are_self_service() -> None:
    user_doc = (DOCS / "manifest-sparse-paths.md").read_text(encoding="utf-8")
    developer_doc = (DOCS / "developer-manifest-sparse-paths.md").read_text(encoding="utf-8")

    for text in (user_doc, developer_doc):
        assert "dpone manifest sparse-paths" in text
        assert "sparse checkout" in text
        assert "Runbook" in text
        assert "GitOps" in text

    assert "--include-global-overrides" in user_doc
    assert "--include-env-overrides" in user_doc
    assert "--support-path" in user_doc
    assert "SparsePathPolicy" in developer_doc
    assert "ManifestSparsePathDiscovery" in developer_doc
    assert "Do not add Airflow-specific logic" in developer_doc


def test_manifest_sparse_paths_docs_are_linked_from_nav_architecture_and_cli_reference() -> None:
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cli_reference = (DOCS / "cli-reference.md").read_text(encoding="utf-8")

    assert "Manifest sparse paths: manifest-sparse-paths.md" in mkdocs
    assert "Developer manifest sparse paths: developer-manifest-sparse-paths.md" in mkdocs
    assert "manifest-sparse-paths.md" in index
    assert "Manifest sparse paths" in architecture
    assert "dpone manifest sparse-paths" in cli_reference
