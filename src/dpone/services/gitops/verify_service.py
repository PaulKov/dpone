from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from dpone.gitops.lock_verify import GitOpsLockVerifier
from dpone.gitops.models import GitOpsIssue, GitOpsVerifyReport
from dpone.gitops.paths import safe_relative_path
from dpone.gitops.verify import GitOpsPlanVerifier, resolve_repo_relative_root
from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsVerifyContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


class GitOpsVerifyService:
    """Verifies that a worktree satisfies a GitOps plan artifact."""

    def __init__(self, *, ctx: GitOpsVerifyContext) -> None:
        self._ctx = ctx

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        raw_plan = str(getattr(args, "plan", "") or "")
        plan_label, plan_payload, blockers = self._read_plan(raw_plan=raw_plan, repo_root=repo_root)
        if blockers:
            report = GitOpsVerifyReport(
                plan=plan_label,
                worktree=str(getattr(args, "worktree", ".") or "."),
                manifest="",
                checked_paths=(),
                lock_checks=(),
                blockers=tuple(blockers),
            )
        else:
            report = GitOpsPlanVerifier().verify(
                plan=plan_payload,
                plan_path=plan_label,
                repo_root=repo_root,
                worktree=getattr(args, "worktree", "."),
            )
            if bool(getattr(args, "verify_lock", False)) and report.passed:
                report = _verify_lock(
                    plan=plan_payload,
                    report=report,
                    repo_root=repo_root,
                    worktree=getattr(args, "worktree", "."),
                )
        return GitOpsView(
            meta=build_gitops_meta(
                "gitops.verify",
                path=plan_label,
                options={
                    "format": getattr(args, "format", "json"),
                    "worktree": getattr(args, "worktree", "."),
                    "verify_lock": bool(getattr(args, "verify_lock", False)),
                    "repo_root": repo_root.relative_to(repo_root).as_posix(),
                },
            ),
            report=report,
        )

    def _read_plan(self, *, raw_plan: str, repo_root: Path) -> tuple[str, dict[str, Any], list[GitOpsIssue]]:
        try:
            rel_path = safe_relative_path(raw_plan, source="plan")
        except ValueError as exc:
            return raw_plan, {}, [GitOpsIssue(code="invalid_path", message=str(exc), path=raw_plan, source="plan")]
        plan_label = "." if rel_path.as_posix() == "." else rel_path.as_posix()
        try:
            payload = json.loads(self._ctx.fs.read_text(repo_root / rel_path, encoding="utf-8"))
        except Exception as exc:
            return (
                plan_label,
                {},
                [
                    GitOpsIssue(
                        code="plan_read_failed",
                        message=f"Could not read or parse GitOps plan: {exc}",
                        path=plan_label,
                        source="plan",
                    )
                ],
            )
        if not isinstance(payload, dict):
            return (
                plan_label,
                {},
                [
                    GitOpsIssue(
                        code="invalid_plan",
                        message="GitOps plan JSON root must be an object",
                        path=plan_label,
                        source="plan",
                    )
                ],
            )
        return plan_label, payload, []


def _verify_lock(
    *,
    plan: dict[str, Any],
    report: GitOpsVerifyReport,
    repo_root: Path,
    worktree: object,
) -> GitOpsVerifyReport:
    _label, worktree_root, worktree_blocker = resolve_repo_relative_root(
        raw=worktree,
        repo_root=repo_root,
        source="--worktree",
    )
    if worktree_blocker is not None:
        return report
    lock_checks, lock_warnings, lock_blockers = GitOpsLockVerifier().verify(
        plan=plan,
        worktree_root=worktree_root,
    )
    return GitOpsVerifyReport(
        plan=report.plan,
        worktree=report.worktree,
        manifest=report.manifest,
        checked_paths=report.checked_paths,
        lock_checks=lock_checks,
        warnings=(*report.warnings, *lock_warnings),
        blockers=(*report.blockers, *lock_blockers),
    )


__all__ = ["GitOpsVerifyContext", "GitOpsVerifyService"]
