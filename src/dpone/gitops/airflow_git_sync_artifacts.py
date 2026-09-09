from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.gitops.airflow_git_sync_models import GitOpsAirflowGitSyncSparsePath
from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.ports.filesystem import FileSystem


class GitOpsAirflowGitSyncArtifactCollector:
    """Aggregate sparse checkout paths from emitted GitOps bundle artifacts."""

    def __init__(self, *, fs: FileSystem) -> None:
        self._fs = fs

    def collect(
        self,
        *,
        repo_root: Path,
        bundle: Mapping[str, Any],
        control_paths: Sequence[str],
    ) -> tuple[tuple[GitOpsAirflowGitSyncSparsePath, ...], tuple[GitOpsIssue, ...]]:
        entries: list[GitOpsAirflowGitSyncSparsePath] = []
        blockers: list[GitOpsIssue] = []
        seen: set[str] = set()

        def add(entry: GitOpsAirflowGitSyncSparsePath) -> None:
            if entry.path in seen:
                return
            seen.add(entry.path)
            entries.append(entry)

        for path in control_paths:
            entry, issue = self._control_entry(repo_root=repo_root, raw_path=path, source="airflow.git_sync.control")
            if issue is not None:
                blockers.append(issue)
            elif entry is not None:
                add(entry)

        for bundle_entry in _bundle_entries(bundle):
            for attr in ("plan_path", "verify_path"):
                entry, issue = self._control_entry(
                    repo_root=repo_root,
                    raw_path=bundle_entry.get(attr),
                    source=f"bundle.entries.{attr}",
                )
                if issue is not None:
                    blockers.append(issue)
                elif entry is not None:
                    add(entry)
            plan_path = str(bundle_entry.get("plan_path") or "").strip()
            plan, issue = self._load_plan(repo_root=repo_root, raw_path=plan_path)
            if issue is not None:
                blockers.append(issue)
                continue
            for sparse_entry in _plan_sparse_paths(plan):
                add(sparse_entry)

        return tuple(entries), tuple(blockers)

    def _control_entry(
        self, *, repo_root: Path, raw_path: object, source: str
    ) -> tuple[GitOpsAirflowGitSyncSparsePath | None, GitOpsIssue | None]:
        text = str(raw_path or "").strip()
        if not text:
            return None, None
        try:
            rel_path = safe_relative_path(text, source=source)
        except GitOpsPathValidationError as exc:
            return None, _issue("git_sync_invalid_path", str(exc), text, source)
        full_path = repo_root / rel_path
        return (
            GitOpsAirflowGitSyncSparsePath(
                path=rel_path.as_posix(),
                kind="control_artifact",
                source=source,
                required=True,
                exists=self._fs.exists(full_path),
                is_dir=False,
                reason="GitOps runtime control artifact required by run-spec execution",
            ),
            None,
        )

    def _load_plan(self, *, repo_root: Path, raw_path: str) -> tuple[Mapping[str, Any], GitOpsIssue | None]:
        if not raw_path:
            return {}, None
        try:
            rel_path = safe_relative_path(raw_path, source="bundle.entries.plan_path")
            payload = json.loads(self._fs.read_text(repo_root / rel_path, encoding="utf-8"))
        except Exception as exc:
            return {}, _issue("git_sync_plan_read_failed", f"Could not read GitOps plan: {exc}", raw_path, "plan_path")
        if not isinstance(payload, Mapping):
            return {}, _issue("git_sync_plan_invalid", "GitOps plan JSON root must be an object", raw_path, "plan_path")
        return payload, None


def _bundle_entries(bundle: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw_entries = bundle.get("entries")
    if not isinstance(raw_entries, list):
        return ()
    return tuple(entry for entry in raw_entries if isinstance(entry, Mapping))


def _plan_sparse_paths(plan: Mapping[str, Any]) -> tuple[GitOpsAirflowGitSyncSparsePath, ...]:
    raw_entries = plan.get("sparse_paths")
    if not isinstance(raw_entries, Sequence) or isinstance(raw_entries, str | bytes):
        return ()
    return tuple(_sparse_path(entry) for entry in raw_entries if isinstance(entry, Mapping))


def _sparse_path(entry: Mapping[str, Any]) -> GitOpsAirflowGitSyncSparsePath:
    return GitOpsAirflowGitSyncSparsePath(
        path=str(entry.get("path") or ""),
        kind=str(entry.get("kind") or "sparse_path"),
        source=str(entry.get("source") or "gitops.plan"),
        required=bool(entry.get("required", True)),
        exists=bool(entry.get("exists", False)),
        is_dir=bool(entry.get("is_dir", False)),
        reason=str(entry.get("reason") or "GitOps plan sparse checkout path"),
    )


def _issue(code: str, message: str, path: str, source: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=source)


__all__ = ["GitOpsAirflowGitSyncArtifactCollector"]
