from __future__ import annotations

WORKTREE_VOLUME_NAME = "dpone-worktree"
SPARSE_INIT_CONTAINER_NAME = "dpone-sparse-checkout"
GIT_SYNC_INIT_CONTAINER_NAME = "dpone-git-sync"
SSH_SECRET_VOLUME_NAME = "dpone-git-sync-ssh"
HTTPS_SECRET_VOLUME_NAME = "dpone-git-sync-https"
WORKSPACE_MOUNT_PATH = "/workspace"
SSH_SECRET_MOUNT_PATH = "/etc/git-secret"
HTTPS_SECRET_MOUNT_PATH = "/etc/git-secret"

__all__ = [
    "GIT_SYNC_INIT_CONTAINER_NAME",
    "HTTPS_SECRET_MOUNT_PATH",
    "HTTPS_SECRET_VOLUME_NAME",
    "SPARSE_INIT_CONTAINER_NAME",
    "SSH_SECRET_MOUNT_PATH",
    "SSH_SECRET_VOLUME_NAME",
    "WORKSPACE_MOUNT_PATH",
    "WORKTREE_VOLUME_NAME",
]
