"""Inode-bound detach primitives for destructive deployment retention."""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from dpone.runtime.deployment_cache_common import fsync_directory
from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError


@dataclass(frozen=True, slots=True)
class DirectoryIdentity:
    device: int
    inode: int


@dataclass(frozen=True, slots=True)
class DetachedDeployment:
    original_path: Path
    detached_path: Path
    identity: DirectoryIdentity


@dataclass(frozen=True, slots=True)
class _RestoreOutcome:
    original_present: bool
    detached_present: bool
    durable: bool


def directory_identity(path: Path) -> DirectoryIdentity:
    """Capture one no-follow directory identity used by the delete fence."""

    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode):
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_PATH_CHANGED",
            "deployment cache GC candidate is no longer a directory",
            path=path.as_posix(),
        )
    return DirectoryIdentity(device=metadata.st_dev, inode=metadata.st_ino)


def detach_validated_directory(
    path: Path,
    *,
    expected_identity: DirectoryIdentity,
    trash_root: Path,
    detached_path: Path | None = None,
) -> DetachedDeployment:
    """Atomically detach exactly the inode that passed validation."""

    _ensure_private_trash_root(trash_root)
    detached_path = detached_path or trash_root / f"{path.name}.{uuid4().hex}"
    if detached_path.parent != trash_root or os.path.lexists(detached_path):
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_DETACH_FAILED",
            "deployment cache GC detached path reservation is unsafe",
            path=detached_path.as_posix(),
        )
    try:
        os.replace(path, detached_path)
    except OSError as exc:
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_PATH_CHANGED",
            "deployment cache GC candidate changed before detach",
            path=path.as_posix(),
            details={"state_may_have_changed": False, "restored": True},
        ) from exc
    try:
        fsync_directory(path.parent)
        fsync_directory(trash_root)
    except OSError as exc:
        outcome = _restore_if_absent(detached_path, path)
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_PATH_CHANGED",
            "deployment cache GC detach could not be committed durably",
            path=path.as_posix(),
            details=_restore_details(outcome, detached_path=detached_path),
        ) from exc
    try:
        observed_identity = directory_identity(detached_path)
    except (OSError, DeploymentCacheRetentionApplyError) as exc:
        outcome = _restore_if_absent(detached_path, path)
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_PATH_CHANGED",
            "deployment cache GC could not verify the detached directory",
            path=path.as_posix(),
            details=_restore_details(outcome, detached_path=detached_path),
        ) from exc
    if observed_identity != expected_identity:
        outcome = _restore_if_absent(detached_path, path)
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_PATH_CHANGED",
            "deployment cache GC detached an inode that was not reviewed",
            path=path.as_posix(),
            details=_restore_details(outcome, detached_path=detached_path),
        )
    return DetachedDeployment(
        original_path=path,
        detached_path=detached_path,
        identity=observed_identity,
    )


def restore_detached_directory(detached: DetachedDeployment) -> bool:
    """Restore a verified detach only when its original path is still absent."""

    try:
        if directory_identity(detached.detached_path) != detached.identity:
            return False
    except (OSError, DeploymentCacheRetentionApplyError):
        return False
    return _restore_if_absent(detached.detached_path, detached.original_path).durable


def _restore_if_absent(detached_path: Path, original_path: Path) -> _RestoreOutcome:
    if os.path.lexists(original_path):
        return _restore_outcome(detached_path, original_path, durable=False)
    try:
        os.replace(detached_path, original_path)
    except OSError:
        return _restore_outcome(detached_path, original_path, durable=False)
    try:
        fsync_directory(detached_path.parent)
        fsync_directory(original_path.parent)
    except OSError:
        return _restore_outcome(detached_path, original_path, durable=False)
    return _restore_outcome(detached_path, original_path, durable=True)


def _restore_outcome(detached_path: Path, original_path: Path, *, durable: bool) -> _RestoreOutcome:
    return _RestoreOutcome(
        original_present=os.path.lexists(original_path),
        detached_present=os.path.lexists(detached_path),
        durable=durable,
    )


def _restore_details(outcome: _RestoreOutcome, *, detached_path: Path) -> dict[str, object]:
    return {
        "state_may_have_changed": True,
        "restored": outcome.durable,
        "original_present": outcome.original_present,
        "restore_durability_uncertain": outcome.original_present and not outcome.durable,
        "quarantined_path": detached_path.as_posix() if outcome.detached_present else None,
    }


def _ensure_private_trash_root(path: Path) -> None:
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        metadata = path.lstat()
    except OSError as exc:
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_DETACH_FAILED",
            "deployment cache GC detach root is unavailable",
            path=path.as_posix(),
        ) from exc
    unsafe_mode = stat.S_IMODE(metadata.st_mode) & 0o777 != 0o700
    wrong_owner = hasattr(os, "geteuid") and metadata.st_uid != os.geteuid()
    if not stat.S_ISDIR(metadata.st_mode) or path.is_symlink() or unsafe_mode or wrong_owner:
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_DETACH_FAILED",
            "deployment cache GC detach root must be a private 0700 directory owned by the current process UID",
            path=path.as_posix(),
        )


__all__ = [
    "DetachedDeployment",
    "DirectoryIdentity",
    "detach_validated_directory",
    "directory_identity",
    "restore_detached_directory",
]
