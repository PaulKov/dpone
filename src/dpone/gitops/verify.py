from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.gitops.models import GitOpsIssue, GitOpsPathCheck, GitOpsVerifyReport
from dpone.gitops.path_policy import GitOpsPathValidationError, safe_relative_path


class GitOpsPlanVerifier:
    """Verifies that a sparse worktree satisfies a GitOps plan."""

    def verify(
        self,
        *,
        plan: Mapping[str, Any],
        plan_path: str,
        repo_root: Path,
        worktree: object,
    ) -> GitOpsVerifyReport:
        worktree_label, worktree_root, worktree_blocker = _resolve_repo_relative_root(
            raw=worktree,
            repo_root=repo_root,
            source="--worktree",
        )
        if worktree_blocker is not None:
            return GitOpsVerifyReport(
                plan=plan_path,
                worktree=worktree_label,
                manifest=str(plan.get("manifest") or ""),
                checked_paths=(),
                lock_checks=(),
                blockers=(worktree_blocker,),
            )

        warnings: list[GitOpsIssue] = []
        blockers: list[GitOpsIssue] = []
        checks: list[GitOpsPathCheck] = []
        for entry in _iter_sparse_entries(plan):
            check, issue = _check_entry(entry, worktree_root=worktree_root)
            if check is not None:
                checks.append(check)
            if issue is None:
                continue
            if issue.code == "missing_required_path":
                blockers.append(issue)
            else:
                warnings.append(issue)

        return GitOpsVerifyReport(
            plan=plan_path,
            worktree=worktree_label,
            manifest=str(plan.get("manifest") or ""),
            checked_paths=tuple(checks),
            warnings=tuple(warnings),
            blockers=tuple(blockers),
        )


def _iter_sparse_entries(plan: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw_entries = plan.get("sparse_paths")
    if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, str | bytes):
        return ()
    return tuple(entry for entry in raw_entries if isinstance(entry, Mapping))


def _check_entry(entry: Mapping[str, Any], *, worktree_root: Path) -> tuple[GitOpsPathCheck | None, GitOpsIssue | None]:
    raw_path = entry.get("path")
    try:
        rel_path = safe_relative_path(raw_path, source="sparse_paths.path")
    except GitOpsPathValidationError as exc:
        return None, GitOpsIssue(code="invalid_plan_path", message=str(exc), path=str(raw_path or ""), source="plan")

    declared_dir = bool(entry.get("is_dir")) or str(raw_path).endswith("/")
    candidate = (worktree_root / rel_path).resolve(strict=False)
    exists = candidate.is_dir() if declared_dir else candidate.exists()
    required = bool(entry.get("required"))
    check = GitOpsPathCheck(
        path=rel_path.as_posix() + ("/" if declared_dir and not rel_path.as_posix().endswith("/") else ""),
        required=required,
        exists=exists,
        is_dir=declared_dir,
    )
    if exists:
        return check, None
    code = "missing_required_path" if required else "missing_optional_path"
    return check, GitOpsIssue(
        code=code, message="Sparse checkout path is missing from the worktree", path=check.path, source="verify"
    )


def resolve_repo_relative_root(
    *,
    raw: object,
    repo_root: Path,
    source: str,
) -> tuple[str, Path, GitOpsIssue | None]:
    try:
        rel_path = safe_relative_path(raw or ".", source=source)
    except GitOpsPathValidationError as exc:
        return (
            str(raw or ""),
            repo_root,
            GitOpsIssue(code="invalid_path", message=str(exc), path=str(raw or ""), source=source),
        )
    label = "." if rel_path.as_posix() == "." else rel_path.as_posix()
    return label, (repo_root / rel_path).resolve(strict=False), None


_resolve_repo_relative_root = resolve_repo_relative_root


__all__ = ["GitOpsPlanVerifier", "resolve_repo_relative_root"]
