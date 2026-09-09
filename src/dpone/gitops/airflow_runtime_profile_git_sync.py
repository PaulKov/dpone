from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from dpone.gitops.airflow_git_sync_capabilities import ALLOWED_GIT_SYNC_FILTERS
from dpone.gitops.airflow_git_sync_models import (
    GitOpsAirflowGitSyncAuth,
    GitOpsAirflowGitSyncCloneOptions,
    GitOpsAirflowGitSyncContract,
    GitOpsAirflowGitSyncSparsePath,
)
from dpone.gitops.models import GitOpsIssue


class GitOpsAirflowGitSyncPathCollector(Protocol):
    def collect(
        self,
        *,
        repo_root: Path,
        bundle: Mapping[str, Any],
        control_paths: Sequence[str],
    ) -> tuple[tuple[GitOpsAirflowGitSyncSparsePath, ...], tuple[GitOpsIssue, ...]]: ...


class GitOpsAirflowRuntimeProfileGitSyncBuilder:
    """Build optional git-sync runtime contracts from CLI options and bundle artifacts."""

    def __init__(self, *, collector: GitOpsAirflowGitSyncPathCollector) -> None:
        self._collector = collector

    def build(
        self,
        *,
        args: object,
        repo_root: Path,
        bundle: Mapping[str, Any],
        control_paths: Sequence[str],
        enabled: bool,
    ) -> tuple[GitOpsAirflowGitSyncContract | None, tuple[GitOpsIssue, ...]]:
        repo = _optional_text(getattr(args, "git_sync_repo", None))
        image = _optional_text(getattr(args, "git_sync_image", None))
        if not repo and not image:
            return None, ()

        blockers: list[GitOpsIssue] = []
        if not repo:
            blockers.append(_issue("git_sync_repo_required", "--git-sync-repo is required", "--git-sync-repo"))
        if not image:
            blockers.append(_issue("git_sync_image_required", "--git-sync-image is required", "--git-sync-image"))

        auth, auth_blockers = _git_sync_auth(args)
        clone, clone_blockers = _git_sync_clone(args)
        blockers.extend(auth_blockers)
        blockers.extend(clone_blockers)
        sparse_paths: tuple[GitOpsAirflowGitSyncSparsePath, ...] = ()
        if enabled and not blockers:
            sparse_paths, collect_blockers = self._collector.collect(
                repo_root=repo_root,
                bundle=bundle,
                control_paths=control_paths,
            )
            blockers.extend(collect_blockers)

        if not repo or not image:
            return None, tuple(blockers)
        return (
            GitOpsAirflowGitSyncContract(
                repo=repo,
                ref=_optional_text(getattr(args, "git_sync_ref", None)) or "HEAD",
                image=image,
                auth=auth,
                clone=clone,
                sparse_paths=sparse_paths,
            ),
            tuple(blockers),
        )


def _git_sync_auth(args: object) -> tuple[GitOpsAirflowGitSyncAuth, tuple[GitOpsIssue, ...]]:
    mode = _text(getattr(args, "git_sync_auth_mode", None)) or "image"
    auth = GitOpsAirflowGitSyncAuth(
        mode=mode,
        ssh_secret_name=_optional_text(getattr(args, "git_sync_ssh_secret", None)),
        ssh_key=_text(getattr(args, "git_sync_ssh_key", None)) or "ssh",
        ssh_known_hosts_key=_text(getattr(args, "git_sync_ssh_known_hosts_key", None)) or "known_hosts",
        https_secret_name=_optional_text(getattr(args, "git_sync_https_secret", None)),
        https_username_key=_text(getattr(args, "git_sync_https_username_key", None)) or "username",
        https_password_key=_text(getattr(args, "git_sync_https_password_key", None)) or "password",
    )
    blockers: list[GitOpsIssue] = []
    if mode not in {"image", "ssh_secret", "https_secret"}:
        blockers.append(_issue("git_sync_auth_invalid", "Unknown git-sync auth mode", mode))
    if mode == "ssh_secret" and not auth.ssh_secret_name:
        blockers.append(_issue("git_sync_auth_invalid", "--git-sync-ssh-secret is required", "--git-sync-ssh-secret"))
    if mode == "https_secret" and not auth.https_secret_name:
        blockers.append(
            _issue("git_sync_auth_invalid", "--git-sync-https-secret is required", "--git-sync-https-secret")
        )
    return auth, tuple(blockers)


def _git_sync_clone(args: object) -> tuple[GitOpsAirflowGitSyncCloneOptions, tuple[GitOpsIssue, ...]]:
    raw_depth = getattr(args, "git_sync_depth", 1)
    depth = _int(raw_depth)
    filter_value = _optional_text(getattr(args, "git_sync_filter", None))
    blockers: list[GitOpsIssue] = []
    if depth is None or depth < 0:
        blockers.append(
            _issue("git_sync_depth_invalid", "--git-sync-depth must be greater than or equal to 0", str(raw_depth))
        )
        depth = 1
    if filter_value is not None and filter_value not in ALLOWED_GIT_SYNC_FILTERS:
        blockers.append(
            _issue("git_sync_filter_invalid", "--git-sync-filter must be blob:none or tree:0", filter_value)
        )
    return GitOpsAirflowGitSyncCloneOptions(depth=depth, filter=filter_value), tuple(blockers)


def _issue(code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source="dpone gitops airflow runtime-profile")


def _int(value: object) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value: object) -> str:
    return str(value or "").strip()


def _optional_text(value: object) -> str | None:
    text = _text(value)
    return text or None


__all__ = ["GitOpsAirflowRuntimeProfileGitSyncBuilder"]
