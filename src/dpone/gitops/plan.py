from __future__ import annotations

from dpone.gitops.models import (
    GitOpsCommandContract,
    GitOpsIssue,
    GitOpsLockReport,
    GitOpsPlanReport,
    GitOpsRunnerContract,
    GitOpsSparsePath,
)
from dpone.manifest.sparse_paths_models import SparsePathEntry, SparsePathIssue, SparsePathReport

_REQUIRED_KINDS = frozenset(
    {"manifest", "manifest_dependency", "convention", "registry", "registry_dir", "support_path"}
)


class GitOpsPlanBuilder:
    """Builds a scheduler-neutral GitOps runner contract from sparse-path evidence."""

    def build(
        self,
        *,
        sparse_report: SparsePathReport,
        runner: str,
        run_command: str | None,
        lock: GitOpsLockReport | None = None,
    ) -> GitOpsPlanReport:
        return GitOpsPlanReport(
            manifest=sparse_report.manifest,
            workload_root=sparse_report.workload_root,
            runner=GitOpsRunnerContract(kind=runner),
            command=GitOpsCommandContract(text=run_command or f"dpone run {sparse_report.manifest}"),
            sparse_paths=tuple(_sparse_path(entry) for entry in sparse_report.entries),
            warnings=tuple(_issue(issue) for issue in sparse_report.warnings),
            blockers=tuple(_issue(issue) for issue in sparse_report.blockers),
            lock=lock or GitOpsLockReport(entries=()),
        )


def _sparse_path(entry: SparsePathEntry) -> GitOpsSparsePath:
    return GitOpsSparsePath(
        path=entry.path,
        kind=entry.kind,
        source=entry.source,
        required=entry.required or entry.kind in _REQUIRED_KINDS,
        exists=entry.exists,
        is_dir=entry.is_dir,
        reason=entry.reason,
    )


def _issue(issue: SparsePathIssue) -> GitOpsIssue:
    return GitOpsIssue(
        code=issue.code,
        message=issue.message,
        path=issue.path,
        source=issue.source,
    )


__all__ = ["GitOpsPlanBuilder"]
