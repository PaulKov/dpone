"""Descriptor-anchored project-root identity shared by build-plane entry points."""

from __future__ import annotations

import errno
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from dpone.manifest.project_discovery_models import ProjectDiscoveryProjectionError

_DIRECTORY_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)


class ProjectRootError(OSError):
    """A project root could not be identified without following its final entry."""


@dataclass(frozen=True, slots=True)
class ProjectRootIdentity:
    """Canonical project path and stable directory identity."""

    path: Path
    device: int
    inode: int

    def matches(self, metadata: os.stat_result) -> bool:
        return stat.S_ISDIR(metadata.st_mode) and metadata.st_dev == self.device and metadata.st_ino == self.inode


def project_root_candidate(root: str | Path) -> Path:
    """Canonicalize parent aliases while preserving the final root entry."""

    requested = Path(root).expanduser().absolute()
    if requested == requested.parent:
        return requested.resolve(strict=True)
    try:
        parent = requested.parent.resolve(strict=True)
    except OSError as exc:
        raise ProjectRootError("Project root parent could not be resolved safely.") from exc
    return parent / requested.name


def inspect_project_root(
    root: str | Path,
    *,
    allow_missing: bool = False,
) -> ProjectRootIdentity | None:
    """Inspect one final directory entry through its canonical parent descriptor."""

    candidate = project_root_candidate(root)
    if candidate == candidate.parent:
        return _identity_from_descriptor(candidate)
    parent_fd: int | None = None
    child_fd: int | None = None
    try:
        parent_fd = os.open(candidate.parent, _DIRECTORY_FLAGS)
        try:
            observed = os.stat(candidate.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            if allow_missing:
                return None
            raise ProjectRootError("Project root does not exist.")
        if stat.S_ISLNK(observed.st_mode):
            raise ProjectRootError("Project root must not be a symbolic link.")
        if not stat.S_ISDIR(observed.st_mode):
            raise ProjectRootError("Project root must be a real directory.")
        child_fd = os.open(candidate.name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
        opened = os.fstat(child_fd)
        if opened.st_dev != observed.st_dev or opened.st_ino != observed.st_ino or not stat.S_ISDIR(opened.st_mode):
            raise ProjectRootError("Project root changed while it was inspected.")
        return ProjectRootIdentity(candidate, opened.st_dev, opened.st_ino)
    except ProjectRootError:
        raise
    except OSError as exc:
        raise ProjectRootError("Project root could not be inspected safely.") from exc
    finally:
        if child_fd is not None:
            os.close(child_fd)
        if parent_fd is not None:
            os.close(parent_fd)


def ensure_project_root(root: str | Path) -> ProjectRootIdentity:
    """Create one missing final root directory and return its stable identity."""

    candidate = project_root_candidate(root)
    existing = inspect_project_root(candidate, allow_missing=True)
    if existing is not None:
        return existing
    parent_fd: int | None = None
    try:
        parent_fd = os.open(candidate.parent, _DIRECTORY_FLAGS)
        try:
            os.mkdir(candidate.name, mode=0o777, dir_fd=parent_fd)
        except FileExistsError:
            pass
        os.fsync(parent_fd)
    except OSError as exc:
        if exc.errno not in {errno.EINVAL, errno.ENOTSUP}:
            raise ProjectRootError("Project root could not be created safely.") from exc
    finally:
        if parent_fd is not None:
            os.close(parent_fd)
    identity = inspect_project_root(candidate)
    assert identity is not None
    return identity


def verify_project_root(identity: ProjectRootIdentity) -> None:
    """Fail when the path no longer names the captured project directory."""

    current = inspect_project_root(identity.path)
    if current != identity:
        raise ProjectRootError("Project root changed during the operation.")


def validate_project_root(root: str | Path) -> Path:
    """Return one real project directory without accepting a symlink alias."""

    try:
        identity = inspect_project_root(root)
    except ProjectRootError as exc:
        raise ProjectDiscoveryProjectionError(str(exc)) from exc
    assert identity is not None
    return identity.path


def _identity_from_descriptor(path: Path) -> ProjectRootIdentity:
    descriptor: int | None = None
    try:
        descriptor = os.open(path, _DIRECTORY_FLAGS)
        metadata = os.fstat(descriptor)
        if not stat.S_ISDIR(metadata.st_mode):
            raise ProjectRootError("Project root must be a real directory.")
        return ProjectRootIdentity(path, metadata.st_dev, metadata.st_ino)
    except ProjectRootError:
        raise
    except OSError as exc:
        raise ProjectRootError("Project root could not be opened safely.") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


__all__ = [
    "ProjectRootError",
    "ProjectRootIdentity",
    "ensure_project_root",
    "inspect_project_root",
    "project_root_candidate",
    "validate_project_root",
    "verify_project_root",
]
