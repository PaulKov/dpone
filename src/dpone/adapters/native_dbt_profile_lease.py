"""Retain an owned profile directory while keeping credential bytes short-lived."""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from types import TracebackType
from uuid import uuid4

from dpone.contracts.dbt_publishing import DbtPublishingError

_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


class NativeDbtProfileLease:
    """Preallocate one profile directory for an outer bootstrap lifetime.

    Enter before constructing the native evidence writer. Pass this same object
    as the bound build's profile store. The materialization context removes only
    its own credential file; the outer context retains directory identity through
    evidence authentication and then removes its owned empty directory.
    """

    def __init__(self, root: Path, *, max_bytes: int) -> None:
        if not isinstance(root, Path) or not root.is_absolute() or ".." in root.parts:
            raise _error("Native profile root must be an absolute path without traversal")
        if type(max_bytes) is not int or max_bytes <= 0:
            raise _error("Native profile byte bound must be an exact positive integer")
        self._root, self._maximum = root, max_bytes
        self._lock = RLock()
        self._entered = False
        self._closed = False
        self._used = False
        self._root_fd = self._child_fd = -1
        self._name = ""
        self._child_identity: tuple[int, int] | None = None
        self._file_identity: tuple[int, int] | None = None

    def __enter__(self) -> NativeDbtProfileLease:
        with self._lock:
            if self._entered or self._closed:
                raise _error("Native profile lease cannot be entered again")
            self._entered = True
            try:
                self._root_fd = _open_directory(self._root)
                self._name = "dpone-native-profile-" + uuid4().hex
                os.mkdir(self._name, 0o700, dir_fd=self._root_fd)
                created = os.stat(self._name, dir_fd=self._root_fd, follow_symlinks=False)
                self._child_identity = _identity(created)
                self._child_fd = os.open(self._name, _DIRECTORY_FLAGS, dir_fd=self._root_fd)
                if _identity(os.fstat(self._child_fd)) != self._child_identity:
                    raise _error("Native profile directory changed during creation")
                self._require_paths()
                return self
            except BaseException as exc:
                try:
                    self._close()
                except BaseException as cleanup:
                    raise cleanup from exc
                if isinstance(exc, OSError):
                    raise _error("Native profile directory could not be owned safely") from exc
                raise

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        with self._lock:
            try:
                self._close()
            except BaseException as cleanup:
                if exc is not None:
                    raise cleanup from exc
                raise

    @property
    def profile_path(self) -> Path:
        """The preallocated path; access does not grant credential authority."""
        with self._lock:
            self._require_open()
            self._require_paths()
            return self._root / self._name / "profiles.yml"

    @contextmanager
    def materialize(self, content: bytes) -> Iterator[Path]:
        """Write once, then unlink only the same opened credential file."""
        with self._lock:
            self._require_open()
            if self._used:
                raise _error("Native profile materialization is single-use")
            self._used = True
            if type(content) is not bytes or not content or len(content) > self._maximum:
                raise _error("Native profile content must be nonempty bytes within its bound")
            try:
                self._require_paths()
                self._write(content)
                self._require_paths()
                yield self._root / self._name / "profiles.yml"
            except BaseException as exc:
                try:
                    self._remove_profile()
                except BaseException as cleanup:
                    raise cleanup from exc
                if isinstance(exc, OSError):
                    raise _error("Native profile materialization failed") from exc
                raise
            else:
                self._remove_profile()

    def _write(self, content: bytes) -> None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
        descriptor = os.open("profiles.yml", flags, 0o600, dir_fd=self._child_fd)
        try:
            self._file_identity = _identity(os.fstat(descriptor))
            pending = memoryview(content)
            while pending:
                written = os.write(descriptor, pending)
                if written <= 0:
                    raise OSError("profile write made no progress")
                pending = pending[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _remove_profile(self) -> None:
        if self._file_identity is None:
            return
        try:
            observed = os.stat("profiles.yml", dir_fd=self._child_fd, follow_symlinks=False)
            if not stat.S_ISREG(observed.st_mode) or _identity(observed) != self._file_identity:
                raise _error("Native profile file changed; refusing to remove a substituted file")
            os.unlink("profiles.yml", dir_fd=self._child_fd)
            self._file_identity = None
        except OSError as exc:
            raise _error("Native profile credential cleanup failed") from exc

    def _require_open(self) -> None:
        if not self._entered or self._closed or self._child_fd < 0:
            raise _error("Native profile lease is not open")

    def _require_paths(self) -> None:
        try:
            for path, held in ((self._root, self._root_fd), (self._root / self._name, self._child_fd)):
                observed = _open_directory(path)
                try:
                    if _identity(os.fstat(observed)) != _identity(os.fstat(held)):
                        raise _error("Native profile path differs from its held directory")
                finally:
                    os.close(observed)
        except OSError as exc:
            raise _error("Native profile path could not be revalidated") from exc

    def _close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._remove_profile()
            if self._child_identity is not None:
                observed = os.stat(self._name, dir_fd=self._root_fd, follow_symlinks=False)
                if not stat.S_ISDIR(observed.st_mode) or _identity(observed) != self._child_identity:
                    raise _error("Native profile directory changed; refusing substituted cleanup")
                os.rmdir(self._name, dir_fd=self._root_fd)
        except OSError as exc:
            raise _error("Native profile directory cleanup failed") from exc
        finally:
            self._close_descriptors()

    def _close_descriptors(self) -> None:
        descriptors = self._child_fd, self._root_fd
        self._child_fd = self._root_fd = -1
        failure: BaseException | None = None
        for descriptor in descriptors:
            if descriptor < 0:
                continue
            try:
                os.close(descriptor)
            except BaseException as exc:
                # A failed close may already have freed the numeric descriptor.
                # Never retry it, but still attempt every other owned close.
                if failure is None:
                    failure = exc
        if isinstance(failure, OSError):
            raise _error("Native profile descriptor cleanup failed") from failure
        if failure is not None:
            raise failure


def _open_directory(path: Path) -> int:
    """Walk every component with held parent descriptors and no symlink following."""
    descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
    try:
        for part in path.parts[1:]:
            child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _identity(value: os.stat_result) -> tuple[int, int]:
    return value.st_dev, value.st_ino


def _error(message: str) -> DbtPublishingError:
    return DbtPublishingError("DPONE_DBT_PROFILE_INVALID", message)
