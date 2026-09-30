from pathlib import Path

import pytest

from tests.integration.mssql.mssql_certification_identity import baked_source_commit


def test_baked_source_identity_requires_exact_requested_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    identity = tmp_path / "source-commit"
    tree = tmp_path / "source-tree"
    identity.write_text("a" * 40 + "\n", encoding="ascii")
    tree.write_text("c" * 40 + "\n", encoding="ascii")
    monkeypatch.setenv("DPONE_CERTIFICATION_COMMIT_SHA", "a" * 40)
    monkeypatch.setenv("DPONE_CERTIFICATION_TREE_OID", "c" * 40)
    monkeypatch.setenv("DPONE_CERTIFICATION_IMAGE_SHA256", "d" * 64)
    assert baked_source_commit(identity_file=identity, tree_file=tree) == "a" * 40

    monkeypatch.setenv("DPONE_CERTIFICATION_COMMIT_SHA", "b" * 40)
    with pytest.raises(pytest.fail.Exception, match="does not match"):
        baked_source_commit(identity_file=identity, tree_file=tree)


def test_baked_source_identity_ignores_path_override_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DPONE_CERTIFICATION_IDENTITY_FILE", "/tmp/untrusted-commit")
    monkeypatch.setenv("DPONE_CERTIFICATION_TREE_FILE", "/tmp/untrusted-tree")
    monkeypatch.setenv("DPONE_CERTIFICATION_COMMIT_SHA", "a" * 40)
    monkeypatch.setenv("DPONE_CERTIFICATION_TREE_OID", "b" * 40)
    monkeypatch.setenv("DPONE_CERTIFICATION_IMAGE_SHA256", "c" * 64)

    with pytest.raises(pytest.fail.Exception, match="identity unavailable"):
        baked_source_commit()
