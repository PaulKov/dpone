from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_gitops_user_and_developer_docs_are_self_service() -> None:
    user_doc = (DOCS / "gitops-control-plane.md").read_text(encoding="utf-8")
    developer_doc = (DOCS / "developer-gitops-control-plane.md").read_text(encoding="utf-8")

    for text in (user_doc, developer_doc):
        assert "dpone gitops plan" in text
        assert "dpone gitops verify" in text
        assert "dpone gitops affected" in text
        assert "dpone gitops bundle" in text
        assert "--verify-lock" in text
        assert "--changed-files-file" in text
        assert "--from-ref" in text
        assert "--fail-on-empty-impact" in text
        assert "--fail-on-warnings" in text
        assert "--require-lock" in text
        assert "--policy-profile" in text
        assert "--attest" in text
        assert "dpone gitops bundle verify" in text
        assert "--require-attestation" in text
        assert "attestation" in text
        assert "bundle_digest" in text
        assert "JSON Schema" in text
        assert "Runbook" in text
        assert "GitOps" in text

    assert "KubernetesPodOperator" in user_doc
    assert "sparse checkout" in user_doc
    assert "lock" in user_doc
    assert "provenance" in user_doc
    assert "GitOpsPlanBuilder" in developer_doc
    assert "GitOpsPlanVerifier" in developer_doc
    assert "GitOpsImpactAnalyzer" in developer_doc
    assert "Do not import Airflow" in developer_doc


def test_gitops_docs_are_linked_from_nav_architecture_and_cli_reference() -> None:
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cli_reference = (DOCS / "cli-reference.md").read_text(encoding="utf-8")

    assert "GitOps control plane: gitops-control-plane.md" in mkdocs
    assert "Developer GitOps control plane: developer-gitops-control-plane.md" in mkdocs
    assert "gitops-control-plane.md" in index
    assert "GitOps control-plane" in architecture
    assert "dpone gitops plan" in cli_reference
    assert "dpone gitops verify" in cli_reference
    assert "dpone gitops affected" in cli_reference
    assert "dpone gitops bundle" in cli_reference
    assert "dpone gitops bundle verify" in cli_reference
    assert "--verify-lock" in cli_reference
    assert "--changed-files-file" in cli_reference
    assert "--fail-on-empty-impact" in cli_reference
    assert "--policy-profile" in cli_reference
    assert "--attest" in cli_reference
    assert "--require-attestation" in cli_reference

    for schema_name in ("affected", "plan", "verify", "bundle", "attestation"):
        assert (DOCS / "schemas" / "gitops" / f"{schema_name}.schema.json").exists()
