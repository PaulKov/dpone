from __future__ import annotations

from pathlib import Path

from tests.agent_policy._release_identity_gate_helpers import (
    codes,
    git,
    publish_origin_master,
    release_identity,
    repository,
)


def test_release_metadata_is_read_from_the_frozen_commit_not_the_worktree(
    tmp_path: Path,
) -> None:
    root = repository(tmp_path)
    git(root, "init", "-q")
    git(root, "config", "user.name", "dpone test")
    git(root, "config", "user.email", "dpone-test@example.invalid")
    git(root, "add", ".")
    git(root, "commit", "-qm", "release source")
    commit_sha = git(root, "rev-parse", "HEAD")
    git(root, "tag", "-a", "v9.9.9", "-m", "incorrect release tag", commit_sha)

    for path in (*release_identity.PACKAGE_FILES.values(), Path("CHANGELOG.md")):
        source = root / path
        source.write_text(source.read_text(encoding="utf-8").replace("1.2.3", "9.9.9"), encoding="utf-8")

    publish_origin_master(root, commit_sha)
    report = release_identity.evaluate_release_identity(
        root=root,
        tag="v9.9.9",
        commit_sha=commit_sha,
    )

    assert report.status == "FAIL"
    assert set(report.package_versions.values()) == {"1.2.3"}
    assert "RELEASE_PACKAGE_VERSION_MISMATCH" in codes(report)
    assert "RELEASE_CHANGELOG_SECTION_MISSING" in codes(report)


def test_invalid_utf8_in_dirty_worktree_cannot_change_valid_commit_identity(
    tmp_path: Path,
) -> None:
    root = repository(tmp_path)
    git(root, "init", "-q")
    git(root, "config", "user.name", "dpone test")
    git(root, "config", "user.email", "dpone-test@example.invalid")
    git(root, "add", ".")
    git(root, "commit", "-qm", "release source")
    commit_sha = git(root, "rev-parse", "HEAD")
    git(root, "tag", "-a", "v1.2.3", "-m", "release", commit_sha)
    (root / "pyproject.toml").write_bytes(b"\xff")
    publish_origin_master(root, commit_sha)

    report = release_identity.evaluate_release_identity(
        root=root,
        tag="v1.2.3",
        commit_sha=commit_sha,
    )

    assert report.status == "PASS"
    assert report.package_versions["dpone"] == "1.2.3"
    assert report.remote_ref == "origin/master"
    assert report.protected_base_sha == commit_sha.lower()


def test_invalid_utf8_in_committed_package_is_a_structured_blocker(tmp_path: Path) -> None:
    root = repository(tmp_path)
    (root / "pyproject.toml").write_bytes(b"\xff")
    git(root, "init", "-q")
    git(root, "config", "user.name", "dpone test")
    git(root, "config", "user.email", "dpone-test@example.invalid")
    git(root, "add", ".")
    git(root, "commit", "-qm", "invalid release source")
    commit_sha = git(root, "rev-parse", "HEAD")
    git(root, "tag", "-a", "v1.2.3", "-m", "release", commit_sha)

    publish_origin_master(root, commit_sha)
    report = release_identity.evaluate_release_identity(
        root=root,
        tag="v1.2.3",
        commit_sha=commit_sha,
    )

    assert report.status == "FAIL"
    assert "RELEASE_PACKAGE_METADATA_INVALID" in codes(report)


def test_invalid_utf8_in_committed_changelog_is_not_reported_as_missing_section(tmp_path: Path) -> None:
    root = repository(tmp_path)
    (root / "CHANGELOG.md").write_bytes(b"\xff")
    git(root, "init", "-q")
    git(root, "config", "user.name", "dpone test")
    git(root, "config", "user.email", "dpone-test@example.invalid")
    git(root, "add", ".")
    git(root, "commit", "-qm", "invalid changelog")
    commit_sha = git(root, "rev-parse", "HEAD")
    git(root, "tag", "-a", "v1.2.3", "-m", "release", commit_sha)
    publish_origin_master(root, commit_sha)

    report = release_identity.evaluate_release_identity(
        root=root,
        tag="v1.2.3",
        commit_sha=commit_sha,
    )

    assert report.status == "FAIL"
    assert "RELEASE_CHANGELOG_UNAVAILABLE" in codes(report)
    assert "RELEASE_CHANGELOG_SECTION_MISSING" not in codes(report)
