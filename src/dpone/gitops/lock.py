from __future__ import annotations

from hashlib import sha256
from pathlib import Path

from dpone.gitops.models import GitOpsLockEntry, GitOpsLockReport
from dpone.manifest.sparse_paths_models import SparsePathEntry


class GitOpsLockBuilder:
    """Builds hash lock entries for sparse checkout paths."""

    def build(self, *, repo_root: Path, entries: tuple[SparsePathEntry, ...]) -> GitOpsLockReport:
        return GitOpsLockReport(entries=tuple(_lock_entry(repo_root=repo_root, entry=entry) for entry in entries))


def _lock_entry(*, repo_root: Path, entry: SparsePathEntry) -> GitOpsLockEntry:
    path = entry.path
    candidate = (repo_root / path.rstrip("/")).resolve(strict=False)
    digest = None
    if entry.exists and not entry.is_dir and candidate.is_file():
        digest = sha256(candidate.read_bytes()).hexdigest()
    return GitOpsLockEntry(path=path, exists=entry.exists, is_dir=entry.is_dir, sha256=digest)


__all__ = ["GitOpsLockBuilder"]
