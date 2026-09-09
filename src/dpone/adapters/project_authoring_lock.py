"""POSIX adapter for project-wide authoring serialization."""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from errno import EACCES, EAGAIN
from importlib import import_module
from pathlib import Path
from threading import Lock, RLock
from time import monotonic, sleep
from typing import Protocol, cast

from dpone.ports.project_authoring_lock import ProjectAuthoringLockError

_LOCKS_GUARD = Lock()
_THREAD_LOCKS: dict[str, RLock] = {}
_LOCK_TIMEOUT_SECONDS = 10.0
_LOCK_POLL_SECONDS = 0.05


class _FileLockModule(Protocol):
    LOCK_EX: int
    LOCK_NB: int
    LOCK_UN: int

    def flock(self, descriptor: int, operation: int) -> None: ...


@contextmanager
def project_authoring_lock(root: Path) -> Iterator[None]:
    """Serialize discovery-to-commit without creating repository files."""

    canonical_root = _project_root_candidate(root)
    _reject_unsafe_root_entry(canonical_root)
    key = canonical_root.as_posix()
    with _LOCKS_GUARD:
        thread_lock = _THREAD_LOCKS.setdefault(key, RLock())
    if not thread_lock.acquire(timeout=_LOCK_TIMEOUT_SECONDS):
        raise ProjectAuthoringLockError("Timed out waiting for the project authoring lock.")
    try:
        descriptor = _open_lock_file(canonical_root)
        file_lock = None
        locked = False
        try:
            try:
                file_lock = cast(_FileLockModule, import_module("fcntl"))
                _acquire_file_lock(file_lock, descriptor)
            except (ImportError, OSError) as exc:
                raise ProjectAuthoringLockError("Project authoring requires a supported advisory file lock.") from exc
            locked = True
            yield
        finally:
            try:
                if locked and file_lock is not None:
                    file_lock.flock(descriptor, file_lock.LOCK_UN)
            finally:
                os.close(descriptor)
    finally:
        thread_lock.release()


def _open_lock_file(root: Path) -> int:
    lock_root = Path(tempfile.gettempdir()) / f"dpone-authoring-locks-{os.getuid()}"
    try:
        lock_root.mkdir(mode=0o700, exist_ok=True)
    except OSError as exc:
        raise ProjectAuthoringLockError("Project lock directory could not be created safely.") from exc
    _validate_lock_root(lock_root)
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory_descriptor: int | None = None
    descriptor: int | None = None
    try:
        directory_descriptor = os.open(lock_root, directory_flags)
        name = f"{hashlib.sha256(root.as_posix().encode('utf-8')).hexdigest()}.lock"
        file_flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(name, file_flags, 0o600, dir_fd=directory_descriptor)
        _validate_lock_file(descriptor)
        return descriptor
    except ProjectAuthoringLockError:
        if descriptor is not None:
            os.close(descriptor)
        raise
    except OSError as exc:
        if descriptor is not None:
            os.close(descriptor)
        raise ProjectAuthoringLockError("Project lock could not be opened safely for authoring.") from exc
    finally:
        if directory_descriptor is not None:
            os.close(directory_descriptor)


def _project_root_candidate(root: Path) -> Path:
    requested = root.expanduser().absolute()
    if requested == requested.parent:
        return requested.resolve(strict=True)
    try:
        parent = requested.parent.resolve(strict=True)
    except OSError as exc:
        raise ProjectAuthoringLockError("Project root parent could not be resolved safely.") from exc
    return parent / requested.name


def _reject_unsafe_root_entry(root: Path) -> None:
    try:
        metadata = root.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise ProjectAuthoringLockError("Project root could not be inspected safely for authoring.") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ProjectAuthoringLockError("Project root could not be opened safely for authoring.")


def _validate_lock_root(lock_root: Path) -> None:
    try:
        metadata = lock_root.lstat()
    except OSError as exc:
        raise ProjectAuthoringLockError("Project lock directory could not be inspected safely.") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise ProjectAuthoringLockError("Project lock directory is not private to the current user.")


def _validate_lock_file(descriptor: int) -> None:
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o177
    ):
        raise ProjectAuthoringLockError("Project lock file is not a private regular file.")


def _acquire_file_lock(file_lock: _FileLockModule, descriptor: int) -> None:
    deadline = monotonic() + _LOCK_TIMEOUT_SECONDS
    while True:
        try:
            file_lock.flock(descriptor, file_lock.LOCK_EX | file_lock.LOCK_NB)
            return
        except OSError as exc:
            if exc.errno not in {EACCES, EAGAIN}:
                raise
            if monotonic() >= deadline:
                raise ProjectAuthoringLockError("Timed out waiting for the project authoring lock.") from exc
            sleep(_LOCK_POLL_SECONDS)


__all__ = ["ProjectAuthoringLockError", "project_authoring_lock"]
