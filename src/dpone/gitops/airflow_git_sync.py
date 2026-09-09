from __future__ import annotations

import shlex
from pathlib import PurePosixPath

from dpone.gitops.airflow_git_sync_capabilities import should_render_git_sync_filter
from dpone.gitops.airflow_git_sync_models import (
    GitOpsAirflowGitSyncAuth,
    GitOpsAirflowGitSyncContract,
    GitOpsAirflowGitSyncPodPatch,
)
from dpone.gitops.airflow_git_sync_names import (
    GIT_SYNC_INIT_CONTAINER_NAME,
    HTTPS_SECRET_MOUNT_PATH,
    HTTPS_SECRET_VOLUME_NAME,
    SPARSE_INIT_CONTAINER_NAME,
    SSH_SECRET_MOUNT_PATH,
    SSH_SECRET_VOLUME_NAME,
    WORKSPACE_MOUNT_PATH,
    WORKTREE_VOLUME_NAME,
)


class GitOpsAirflowGitSyncPodPatchBuilder:
    """Build Kubernetes PodSpec fragments for one-time sparse git-sync."""

    def build(self, *, contract: GitOpsAirflowGitSyncContract, runtime_image: str) -> GitOpsAirflowGitSyncPodPatch:
        volumes = [_worktree_volume()]
        init_containers = [
            _sparse_checkout_container(contract=contract, runtime_image=runtime_image),
            _git_sync_container(contract=contract),
        ]
        auth_volume = _auth_volume(contract.auth)
        if auth_volume is not None:
            volumes.append(auth_volume)
        return GitOpsAirflowGitSyncPodPatch(
            volumes=tuple(volumes),
            init_containers=tuple(init_containers),
            base_volume_mounts=({"name": WORKTREE_VOLUME_NAME, "mountPath": WORKSPACE_MOUNT_PATH, "readOnly": False},),
            working_dir=contract.worktree_path,
        )


def sparse_checkout_content(contract: GitOpsAirflowGitSyncContract) -> str:
    return "".join(f"{entry.path}\n" for entry in contract.sparse_paths if entry.path)


def reserved_git_sync_volume_names() -> frozenset[str]:
    return frozenset({WORKTREE_VOLUME_NAME, SSH_SECRET_VOLUME_NAME, HTTPS_SECRET_VOLUME_NAME})


def _worktree_volume() -> dict[str, object]:
    return {"name": WORKTREE_VOLUME_NAME, "emptyDir": {}}


def _sparse_checkout_container(*, contract: GitOpsAirflowGitSyncContract, runtime_image: str) -> dict[str, object]:
    sparse_dir = str(PurePosixPath(contract.sparse_checkout_file).parent)
    script = (
        "set -eu\n"
        f"mkdir -p {shlex.quote(sparse_dir)}\n"
        f"cat > {shlex.quote(contract.sparse_checkout_file)} <<'EOF'\n"
        f"{sparse_checkout_content(contract)}"
        "EOF\n"
    )
    return {
        "name": SPARSE_INIT_CONTAINER_NAME,
        "image": runtime_image,
        "imagePullPolicy": "IfNotPresent",
        "command": ["/bin/sh", "-ec"],
        "args": [script],
        "volumeMounts": [{"name": WORKTREE_VOLUME_NAME, "mountPath": WORKSPACE_MOUNT_PATH, "readOnly": False}],
    }


def _git_sync_container(*, contract: GitOpsAirflowGitSyncContract) -> dict[str, object]:
    container: dict[str, object] = {
        "name": GIT_SYNC_INIT_CONTAINER_NAME,
        "image": contract.image,
        "imagePullPolicy": "IfNotPresent",
        "args": _git_sync_args(contract),
        "volumeMounts": [
            {"name": WORKTREE_VOLUME_NAME, "mountPath": WORKSPACE_MOUNT_PATH, "readOnly": False},
            *_auth_volume_mounts(contract.auth),
        ],
    }
    env = _auth_env(contract.auth)
    if env:
        container["env"] = env
    return container


def _git_sync_args(contract: GitOpsAirflowGitSyncContract) -> list[str]:
    args = [
        f"--repo={contract.repo}",
        f"--ref={contract.ref}",
        f"--root={contract.root}",
        f"--link={contract.link}",
        "--one-time",
        f"--depth={contract.clone.depth}",
        "--submodules=off",
        f"--sparse-checkout-file={contract.sparse_checkout_file}",
    ]
    if should_render_git_sync_filter(image=contract.image, filter_value=contract.clone.filter):
        args.append(f"--filter={contract.clone.filter}")
    if contract.auth.mode == "ssh_secret":
        args.extend(
            [
                f"--ssh-key-file={SSH_SECRET_MOUNT_PATH}/{contract.auth.ssh_key}",
                "--ssh-known-hosts=true",
                f"--ssh-known-hosts-file={SSH_SECRET_MOUNT_PATH}/{contract.auth.ssh_known_hosts_key}",
            ]
        )
    return args


def _auth_volume(auth: GitOpsAirflowGitSyncAuth) -> dict[str, object] | None:
    if auth.mode == "ssh_secret" and auth.ssh_secret_name:
        return {"name": SSH_SECRET_VOLUME_NAME, "secret": {"secretName": auth.ssh_secret_name}}
    if auth.mode == "https_secret" and auth.https_secret_name:
        return {"name": HTTPS_SECRET_VOLUME_NAME, "secret": {"secretName": auth.https_secret_name}}
    return None


def _auth_volume_mounts(auth: GitOpsAirflowGitSyncAuth) -> list[dict[str, object]]:
    if auth.mode == "ssh_secret" and auth.ssh_secret_name:
        return [{"name": SSH_SECRET_VOLUME_NAME, "mountPath": SSH_SECRET_MOUNT_PATH, "readOnly": True}]
    if auth.mode == "https_secret" and auth.https_secret_name:
        return [{"name": HTTPS_SECRET_VOLUME_NAME, "mountPath": HTTPS_SECRET_MOUNT_PATH, "readOnly": True}]
    return []


def _auth_env(auth: GitOpsAirflowGitSyncAuth) -> list[dict[str, object]]:
    if auth.mode != "https_secret" or not auth.https_secret_name:
        return []
    return [
        {
            "name": "GITSYNC_USERNAME",
            "valueFrom": {"secretKeyRef": {"name": auth.https_secret_name, "key": auth.https_username_key}},
        },
        {
            "name": "GITSYNC_PASSWORD",
            "valueFrom": {"secretKeyRef": {"name": auth.https_secret_name, "key": auth.https_password_key}},
        },
    ]


__all__ = [
    "GIT_SYNC_INIT_CONTAINER_NAME",
    "GitOpsAirflowGitSyncPodPatchBuilder",
    "SPARSE_INIT_CONTAINER_NAME",
    "WORKTREE_VOLUME_NAME",
    "reserved_git_sync_volume_names",
    "sparse_checkout_content",
]
