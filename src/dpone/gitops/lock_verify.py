from __future__ import annotations

from collections.abc import Mapping, Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any

from dpone.gitops.models import GitOpsIssue, GitOpsLockCheck
from dpone.gitops.path_policy import GitOpsPathValidationError, safe_relative_path


class GitOpsLockVerifier:
    """Verifies file digests recorded in a gitops.plan lock block."""

    def verify(
        self,
        *,
        plan: Mapping[str, Any],
        worktree_root: Path,
    ) -> tuple[tuple[GitOpsLockCheck, ...], tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
        warnings: list[GitOpsIssue] = []
        blockers: list[GitOpsIssue] = []
        checks: list[GitOpsLockCheck] = []
        for entry in _iter_lock_entries(plan):
            check, warning, blocker = _check_lock_entry(entry, worktree_root=worktree_root)
            if check is not None:
                checks.append(check)
            if warning is not None:
                warnings.append(warning)
            if blocker is not None:
                blockers.append(blocker)
        return tuple(checks), tuple(warnings), tuple(blockers)


def _iter_lock_entries(plan: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw_lock = plan.get("lock")
    if not isinstance(raw_lock, Mapping):
        return ()
    raw_entries = raw_lock.get("entries")
    if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, str | bytes):
        return ()
    return tuple(entry for entry in raw_entries if isinstance(entry, Mapping))


def _check_lock_entry(
    entry: Mapping[str, Any],
    *,
    worktree_root: Path,
) -> tuple[GitOpsLockCheck | None, GitOpsIssue | None, GitOpsIssue | None]:
    raw_path = entry.get("path")
    try:
        rel_path = safe_relative_path(raw_path, source="lock.path")
    except GitOpsPathValidationError as exc:
        return (
            None,
            None,
            GitOpsIssue(
                code="invalid_lock_path",
                message=str(exc),
                path=str(raw_path or ""),
                source="lock",
            ),
        )
    label = rel_path.as_posix() + ("/" if str(raw_path).endswith("/") and not rel_path.as_posix().endswith("/") else "")
    expected_sha = entry.get("sha256") if isinstance(entry.get("sha256"), str) else None
    expected_exists = bool(entry.get("exists"))
    expected_is_dir = bool(entry.get("is_dir")) or str(raw_path).endswith("/")
    candidate = (worktree_root / rel_path).resolve(strict=False)
    exists = candidate.is_dir() if expected_is_dir else candidate.is_file()
    actual_sha = _sha256_file(candidate) if exists and not expected_is_dir else None
    passed = expected_sha is None or (exists and actual_sha == expected_sha)
    check = GitOpsLockCheck(
        path=label,
        exists=exists,
        is_dir=expected_is_dir,
        expected_sha256=expected_sha,
        actual_sha256=actual_sha,
        passed=passed,
    )
    if expected_sha is None:
        warning = None
        if expected_exists:
            warning = GitOpsIssue(
                code="lock_digest_unavailable",
                message="Lock entry has no file digest to verify",
                path=label,
                source="lock",
            )
        return check, warning, None
    if not exists:
        return (
            check,
            None,
            GitOpsIssue(
                code="lock_path_missing",
                message="Locked file is missing from the worktree",
                path=label,
                source="lock",
            ),
        )
    if actual_sha != expected_sha:
        return (
            check,
            None,
            GitOpsIssue(
                code="lock_digest_mismatch",
                message="Locked file digest differs from the GitOps plan lock",
                path=label,
                source="lock",
            ),
        )
    return check, None, None


def _sha256_file(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


__all__ = ["GitOpsLockVerifier"]
