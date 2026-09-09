from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class GitOpsAirflowGitSyncAuth:
    mode: str = "image"
    ssh_secret_name: str | None = None
    ssh_key: str = "ssh"
    ssh_known_hosts_key: str = "known_hosts"
    https_secret_name: str | None = None
    https_username_key: str = "username"
    https_password_key: str = "password"

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"mode": self.mode}
        if self.mode == "ssh_secret":
            payload["ssh_secret"] = {
                "name": self.ssh_secret_name,
                "ssh_key": self.ssh_key,
                "known_hosts_key": self.ssh_known_hosts_key,
            }
        if self.mode == "https_secret":
            payload["https_secret"] = {
                "name": self.https_secret_name,
                "username_key": self.https_username_key,
                "password_key": self.https_password_key,
            }
        return payload


@dataclass(frozen=True, slots=True)
class GitOpsAirflowGitSyncSparsePath:
    path: str
    kind: str
    source: str
    required: bool
    exists: bool
    is_dir: bool
    reason: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind,
            "source": self.source,
            "required": self.required,
            "exists": self.exists,
            "is_dir": self.is_dir,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowGitSyncCloneOptions:
    depth: int = 1
    filter: str | None = None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "depth": self.depth,
            "filter": self.filter,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowGitSyncContract:
    repo: str
    ref: str
    image: str
    auth: GitOpsAirflowGitSyncAuth
    sparse_paths: tuple[GitOpsAirflowGitSyncSparsePath, ...]
    root: str = "/workspace/.git-sync"
    link: str = "/workspace/repo"
    worktree_path: str = "/workspace/repo"
    sparse_checkout_file: str = "/workspace/.dpone/git-sync/sparse-checkout"
    clone: GitOpsAirflowGitSyncCloneOptions = GitOpsAirflowGitSyncCloneOptions()
    enabled: bool = True

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "repo": self.repo,
            "ref": self.ref,
            "image": self.image,
            "root": self.root,
            "link": self.link,
            "worktree_path": self.worktree_path,
            "sparse_checkout_file": self.sparse_checkout_file,
            "clone": self.clone.to_jsonable(),
            "auth": self.auth.to_jsonable(),
            "sparse_paths": [entry.to_jsonable() for entry in self.sparse_paths],
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowGitSyncPodPatch:
    volumes: tuple[dict[str, Any], ...]
    init_containers: tuple[dict[str, Any], ...]
    base_volume_mounts: tuple[dict[str, Any], ...]
    working_dir: str


def git_sync_contract_from_mapping(value: object) -> GitOpsAirflowGitSyncContract | None:
    if not isinstance(value, Mapping) or not value.get("enabled"):
        return None
    auth = _auth_from_mapping(value.get("auth"))
    clone = _clone_from_mapping(value.get("clone"))
    return GitOpsAirflowGitSyncContract(
        repo=str(value.get("repo") or ""),
        ref=str(value.get("ref") or "HEAD"),
        image=str(value.get("image") or ""),
        root=str(value.get("root") or "/workspace/.git-sync"),
        link=str(value.get("link") or "/workspace/repo"),
        worktree_path=str(value.get("worktree_path") or "/workspace/repo"),
        sparse_checkout_file=str(value.get("sparse_checkout_file") or "/workspace/.dpone/git-sync/sparse-checkout"),
        auth=auth,
        clone=clone,
        sparse_paths=tuple(_sparse_path(item) for item in value.get("sparse_paths", ()) if isinstance(item, dict)),
    )


def _auth_from_mapping(value: object) -> GitOpsAirflowGitSyncAuth:
    raw = value if isinstance(value, Mapping) else {}
    mode = str(raw.get("mode") or "image")
    ssh = raw.get("ssh_secret") if isinstance(raw.get("ssh_secret"), Mapping) else {}
    https = raw.get("https_secret") if isinstance(raw.get("https_secret"), Mapping) else {}
    return GitOpsAirflowGitSyncAuth(
        mode=mode,
        ssh_secret_name=_optional(ssh.get("name")),
        ssh_key=str(ssh.get("ssh_key") or "ssh"),
        ssh_known_hosts_key=str(ssh.get("known_hosts_key") or "known_hosts"),
        https_secret_name=_optional(https.get("name")),
        https_username_key=str(https.get("username_key") or "username"),
        https_password_key=str(https.get("password_key") or "password"),
    )


def _sparse_path(value: dict[str, Any]) -> GitOpsAirflowGitSyncSparsePath:
    return GitOpsAirflowGitSyncSparsePath(
        path=str(value.get("path") or ""),
        kind=str(value.get("kind") or "unknown"),
        source=str(value.get("source") or "unknown"),
        required=bool(value.get("required", True)),
        exists=bool(value.get("exists", False)),
        is_dir=bool(value.get("is_dir", False)),
        reason=str(value.get("reason") or ""),
    )


def _clone_from_mapping(value: object) -> GitOpsAirflowGitSyncCloneOptions:
    raw = value if isinstance(value, Mapping) else {}
    return GitOpsAirflowGitSyncCloneOptions(
        depth=_int(raw.get("depth"), default=1),
        filter=_optional(raw.get("filter")),
    )


def _int(value: object, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _optional(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


__all__ = [
    "GitOpsAirflowGitSyncAuth",
    "GitOpsAirflowGitSyncCloneOptions",
    "GitOpsAirflowGitSyncContract",
    "GitOpsAirflowGitSyncPodPatch",
    "GitOpsAirflowGitSyncSparsePath",
    "git_sync_contract_from_mapping",
]
