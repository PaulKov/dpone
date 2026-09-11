"""Prove pinned-import auditing rejects foreign edits in real Git histories."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

AUDIT_PATH = Path(__file__).resolve().parents[1] / "test_artifacts/delivery-acceleration/dda-06/audit_ownership.py"
SPEC = importlib.util.spec_from_file_location("dda_ownership", AUDIT_PATH)
assert SPEC is not None and SPEC.loader is not None
audit = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(audit)
TRANSFER_SPEC = importlib.util.spec_from_file_location("dda_transfer", AUDIT_PATH.with_name("record_tree_transfer.py"))
assert TRANSFER_SPEC is not None and TRANSFER_SPEC.loader is not None
transfer = importlib.util.module_from_spec(TRANSFER_SPEC)
TRANSFER_SPEC.loader.exec_module(transfer)


@pytest.fixture
def history(tmp_path, monkeypatch):
    """Independent upstream and integration parents with one approved resolution."""
    monkeypatch.chdir(tmp_path)
    audit.git("init", "-q")
    audit.git("config", "user.email", "fixture@example.invalid")
    audit.git("config", "user.name", "Provenance fixture")
    Path("foreign.py").write_text("original = True\n")
    Path("CHANGELOG.md").write_text("Base\n")
    audit.git("add", ".")
    audit.git("commit", "-qm", "base")
    base = audit.git("rev-parse", "HEAD")
    Path("foreign.py").write_text("upstream = True\n")
    Path("CHANGELOG.md").write_text("Upstream\nBase\n")
    audit.git("commit", "-qam", "upstream")
    upstream = audit.git("rev-parse", "HEAD")
    audit.git("checkout", "-q", "--detach", base)
    Path("CHANGELOG.md").write_text("Integration\nBase\n")
    audit.git("commit", "-qam", "integration")
    integration = audit.git("rev-parse", "HEAD")
    Path("foreign.py").write_text("upstream = True\n")
    Path("CHANGELOG.md").write_text("Integration and Upstream\nBase\n")
    audit.git("commit", "-qam", f"import\n\n(cherry picked from commit {upstream})")
    imported = audit.git("rev-parse", "HEAD")
    approved = {"CHANGELOG.md": audit.git("ls-tree", imported, "--", "CHANGELOG.md")}
    return base, upstream, integration, imported, approved


def test_reviewed_import_accepts_only_exact_patch_blob_or_pinned_resolution(history):
    _, upstream, _, imported, approved = history
    assert audit.reviewed_upstream_import(imported, upstream, approved)
    assert not audit.reviewed_upstream_import(imported, upstream, {})


def test_merge_cannot_bypass_import_delta_verification(history):
    _, upstream, integration, imported, approved = history
    tree = audit.git("rev-parse", f"{imported}^{{tree}}")
    merge = audit.git("commit-tree", tree, "-p", integration, "-p", upstream, "-m", "invalid merge import")
    assert not audit.reviewed_upstream_import(merge, upstream, approved)


@pytest.mark.parametrize("mutation", ["foreign", "unexpected", "resolution"])
def test_import_rejects_altered_foreign_blob_unexpected_path_and_unapproved_resolution(history, mutation):
    _, upstream, integration, imported, approved = history
    audit.git("checkout", "-q", "--detach", integration)
    audit.git("checkout", imported, "--", "foreign.py", "CHANGELOG.md")
    target = {"foreign": "foreign.py", "unexpected": "extra.py", "resolution": "CHANGELOG.md"}[mutation]
    Path(target).write_text("Unreviewed change\n")
    audit.git("add", ".")
    audit.git("commit", "-qm", "altered import")
    assert not audit.reviewed_upstream_import(audit.git("rev-parse", "HEAD"), upstream, approved)


def test_final_preservation_checks_upstream_only_mode_and_blob(history):
    base, upstream, integration, imported, _ = history
    assert audit.upstream_preservation(base, upstream, integration, imported) == {
        "paths": ["foreign.py"],
        "violations": [],
    }
    Path("foreign.py").write_text("Later foreign regression\n")
    audit.git("commit", "-qam", "foreign regression")
    result = audit.upstream_preservation(base, upstream, integration, "HEAD")
    assert result["paths"] == ["foreign.py"]
    assert result["violations"] == ["foreign.py"]


def test_final_preservation_rejects_mode_only_change(history):
    base, upstream, integration, _, _ = history
    audit.git("update-index", "--chmod=+x", "foreign.py")
    audit.git("commit", "-qm", "unapproved executable mode")
    assert audit.upstream_preservation(base, upstream, integration, "HEAD")["violations"] == ["foreign.py"]


@pytest.mark.parametrize("mutation", [None, "tree", "parent", "provenance"])
def test_transfer_proves_whole_tree_parent_and_source_provenance(history, monkeypatch, mutation):
    _, upstream, integration, source, _ = history
    monkeypatch.setattr(transfer, "BASE", upstream)
    parent = integration if mutation == "parent" else upstream
    audit.git("checkout", "-qb", transfer.BRANCH, parent)
    audit.git("read-tree", "--reset", "-u", source)
    if mutation == "tree":
        Path("foreign.py").write_text("Unreviewed transfer\n")
        audit.git("add", "foreign.py")
    expected_staged = "FAIL" if mutation in {"tree", "parent"} else "PASS"
    assert transfer.observe_transfer(source, "staged")["status"] == expected_staged
    message = "Missing provenance" if mutation == "provenance" else f"Reviewed-source: {source}"
    audit.git("commit", "-qm", message)
    assert transfer.observe_transfer(source, "committed")["status"] == ("FAIL" if mutation else "PASS")
