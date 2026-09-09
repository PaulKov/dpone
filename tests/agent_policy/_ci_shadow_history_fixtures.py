"""Synthetic Git histories for portable CI contracts, never hosted receipt evidence."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


def git(root: Path, *args: str) -> str:
    """Run Git only in the explicitly supplied fixture repository."""

    return subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "GIT_NO_REPLACE_OBJECTS": "1"},
    ).stdout.strip()


def initialize_repository(root: Path) -> str:
    """Create an isolated repository and return its synthetic root commit."""

    root.mkdir()
    git(root, "init", "--quiet")
    git(root, "config", "user.name", "CI Contract Fixture")
    git(root, "config", "user.email", "ci-contract@example.invalid")
    git(root, "config", "commit.gpgsign", "false")
    git(root, "config", "core.hooksPath", os.devnull)
    return commit_files(root, "synthetic root", {"fixture-root.txt": "Not historical or hosted evidence.\n"})


def commit_files(root: Path, message: str, files: Mapping[str, str]) -> str:
    """Commit fixture bytes and return their actual locally computed identity."""

    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    git(root, "add", "--all")
    git(root, "commit", "--quiet", "-m", message)
    return git(root, "rev-parse", "HEAD")


@dataclass(frozen=True)
class SyntheticIntegration:
    """Locally created identities; none of these values certify a past PR."""

    parent: str
    base: str
    base_tree: str
    reviewed_head: str
    integration_commit: str
    integration_tree: str
    method: str
    frozen_blobs: Mapping[str, str]


def integration_repository(
    root: Path,
    *,
    frozen_files: Mapping[str, str],
    changed_files: Mapping[str, str],
    method: str,
) -> SyntheticIntegration:
    """Create an approved base, reviewed change and real merge or squash."""

    if method not in {"merge", "squash"} or frozen_files.keys() & changed_files.keys():
        raise ValueError("fixture needs a supported integration and disjoint frozen/changed paths")
    parent = initialize_repository(root)
    base = commit_files(root, "synthetic approved base", frozen_files)
    base_tree = git(root, "rev-parse", f"{base}^{{tree}}")
    blobs = {path: git(root, "rev-parse", f"{base}:{path}") for path in frozen_files}
    git(root, "switch", "--quiet", "-c", "reviewed")
    reviewed = commit_files(root, "synthetic reviewed change", changed_files)
    tree = git(root, "rev-parse", f"{reviewed}^{{tree}}")
    git(root, "switch", "--quiet", "--detach", base)
    git(root, "switch", "--quiet", "-c", "integration")
    if method == "merge":
        git(root, "merge", "--no-ff", "--no-edit", reviewed)
    else:
        git(root, "merge", "--squash", reviewed)
        git(root, "commit", "--quiet", "-m", "synthetic squash integration")
    return SyntheticIntegration(parent, base, base_tree, reviewed, git(root, "rev-parse", "HEAD"), tree, method, blobs)
