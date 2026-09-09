"""Descriptor-anchored filesystem primitives for the local object-store adapter."""

from __future__ import annotations

import errno
import hashlib
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import BinaryIO
from uuid import uuid4

_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_REGULAR_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)


@dataclass(frozen=True, slots=True)
class LocalFileFacts:
    """Content metadata captured from one descriptor-pinned regular file."""

    size_bytes: int
    sha256: str
    modified_at: float


@dataclass(frozen=True, slots=True)
class LocalTreeFile:
    """One regular file below an anchored object-store root."""

    parts: tuple[str, ...]
    facts: LocalFileFacts


class AnchoredLocalFilesystem:
    """Perform object-store operations relative to a no-follow root descriptor."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).absolute()
        self._identity_lock = Lock()
        self._root_identity: tuple[int, int] | None = None

    def write_file(self, source: str | Path, parts: tuple[str, ...], *, create_only: bool) -> LocalFileFacts:
        normalized = _validated_parts(parts)
        with self._open_root(create=True) as root_fd:
            parent_fd, leaf = _open_parent(root_fd, normalized, create=True)
            try:
                return _install_source(Path(source), parent_fd, leaf, create_only=create_only)
            finally:
                os.close(parent_fd)

    @contextmanager
    def open_reader(self, parts: tuple[str, ...]) -> Iterator[BinaryIO]:
        normalized = _validated_parts(parts)
        with self._open_root(create=False) as root_fd:
            parent_fd, leaf = _open_parent(root_fd, normalized, create=False)
            try:
                descriptor = _open_regular(parent_fd, leaf)
            finally:
                os.close(parent_fd)
            with os.fdopen(descriptor, "rb") as handle:
                yield handle

    def facts(self, parts: tuple[str, ...]) -> LocalFileFacts:
        normalized = _validated_parts(parts)
        with self._open_root(create=False) as root_fd:
            parent_fd, leaf = _open_parent(root_fd, normalized, create=False)
            try:
                descriptor = _open_regular(parent_fd, leaf)
            finally:
                os.close(parent_fd)
            try:
                return _descriptor_facts(descriptor)
            finally:
                os.close(descriptor)

    def exists(self, parts: tuple[str, ...]) -> bool:
        normalized = _validated_parts(parts)
        try:
            with self._open_root(create=False) as root_fd:
                parent_fd, leaf = _open_parent(root_fd, normalized, create=False)
                try:
                    mode = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False).st_mode
                finally:
                    os.close(parent_fd)
        except FileNotFoundError:
            return False
        if stat.S_ISLNK(mode):
            raise ValueError("Object storage path must not contain symlink components")
        return stat.S_ISREG(mode) or stat.S_ISDIR(mode)

    def list_files(self, parts: tuple[str, ...]) -> tuple[LocalTreeFile, ...]:
        normalized = _validated_parts(parts)
        try:
            with self._open_root(create=False) as root_fd:
                parent_fd, leaf = _open_parent(root_fd, normalized, create=False)
                try:
                    mode = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False).st_mode
                    if stat.S_ISREG(mode):
                        descriptor = _open_regular(parent_fd, leaf)
                        try:
                            return (LocalTreeFile(normalized, _descriptor_facts(descriptor)),)
                        finally:
                            os.close(descriptor)
                    if not stat.S_ISDIR(mode):
                        raise ValueError("Object storage path must contain only regular files and directories")
                    directory_fd = _open_directory(parent_fd, leaf, create=False)
                finally:
                    os.close(parent_fd)
                try:
                    return tuple(_walk_files(directory_fd, normalized))
                finally:
                    os.close(directory_fd)
        except FileNotFoundError:
            return ()

    def delete_tree(self, parts: tuple[str, ...]) -> int:
        normalized = _validated_parts(parts)
        try:
            with self._open_root(create=False) as root_fd:
                parent_fd, leaf = _open_parent(root_fd, normalized, create=False)
                try:
                    mode = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False).st_mode
                    if stat.S_ISREG(mode):
                        os.unlink(leaf, dir_fd=parent_fd)
                        _fsync(parent_fd)
                        return 1
                    if not stat.S_ISDIR(mode):
                        raise ValueError("Object storage path must contain only regular files and directories")
                    directory_fd = _open_directory(parent_fd, leaf, create=False)
                    try:
                        deleted = _delete_contents(directory_fd)
                        _assert_same_directory(parent_fd, leaf, directory_fd)
                    finally:
                        os.close(directory_fd)
                    os.rmdir(leaf, dir_fd=parent_fd)
                    _fsync(parent_fd)
                    return deleted
                finally:
                    os.close(parent_fd)
        except FileNotFoundError:
            return 0

    @contextmanager
    def _open_root(self, *, create: bool) -> Iterator[int]:
        try:
            descriptor = _open_absolute_directory(self._root, create=create)
        except OSError as exc:
            raise _safe_path_error(exc) from exc
        try:
            try:
                current = os.stat(self._root, follow_symlinks=False)
            except OSError as exc:
                raise ValueError("Object storage root changed while it was being opened") from exc
            opened = os.fstat(descriptor)
            if not stat.S_ISDIR(current.st_mode) or (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
                raise ValueError("Object storage root changed while it was being opened")
            self._require_stable_root(opened)
            yield descriptor
        finally:
            os.close(descriptor)

    def _require_stable_root(self, opened: os.stat_result) -> None:
        identity = (opened.st_dev, opened.st_ino)
        with self._identity_lock:
            if self._root_identity is None:
                self._root_identity = identity
            elif self._root_identity != identity:
                raise ValueError("Object storage root changed between operations")


def _validated_parts(parts: tuple[str, ...]) -> tuple[str, ...]:
    if not parts:
        raise ValueError("Object storage path must not be empty")
    if any(not part or part in {".", ".."} or "/" in part or "\\" in part or "\x00" in part for part in parts):
        raise ValueError("Object storage path must be a normalized relative POSIX path")
    return parts


def _open_absolute_directory(path: Path, *, create: bool) -> int:
    if not path.is_absolute() or not path.anchor:
        raise ValueError("Object storage root must be an absolute path")
    parts = path.parts[1:]
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("Object storage root must be a normalized directory below the filesystem root")
    descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
    try:
        for part in parts:
            next_descriptor = _open_directory(descriptor, part, create=create)
            os.close(descriptor)
            descriptor = next_descriptor
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_parent(root_fd: int, parts: tuple[str, ...], *, create: bool) -> tuple[int, str]:
    current = os.dup(root_fd)
    try:
        for part in parts[:-1]:
            next_fd = _open_directory(current, part, create=create)
            os.close(current)
            current = next_fd
        return current, parts[-1]
    except BaseException:
        os.close(current)
        raise


def _open_directory(parent_fd: int, name: str, *, create: bool) -> int:
    try:
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            raise
        created = False
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_fd)
            created = True
        except FileExistsError:
            pass
        if created:
            _fsync(parent_fd)
        try:
            return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
        except OSError as exc:
            raise _safe_path_error(exc) from exc
    except OSError as exc:
        raise _safe_path_error(exc) from exc


def _open_regular(parent_fd: int, name: str) -> int:
    try:
        descriptor = os.open(name, _REGULAR_READ_FLAGS, dir_fd=parent_fd)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            raise FileNotFoundError(name) from exc
        raise _safe_path_error(exc) from exc
    if stat.S_ISREG(os.fstat(descriptor).st_mode):
        return descriptor
    os.close(descriptor)
    raise ValueError("Object storage object must be a regular file")


def _install_source(source: Path, parent_fd: int, leaf: str, *, create_only: bool) -> LocalFileFacts:
    source_fd = os.open(source, _REGULAR_READ_FLAGS)
    staging = f".{leaf}.tmp.{uuid4().hex}"
    staging_fd = -1
    try:
        if not stat.S_ISREG(os.fstat(source_fd).st_mode):
            raise ValueError("Object storage upload source must be a regular file")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        staging_fd = os.open(staging, flags, 0o600, dir_fd=parent_fd)
        digest = hashlib.sha256()
        size_bytes = 0
        while chunk := os.read(source_fd, 1024 * 1024):
            digest.update(chunk)
            size_bytes += len(chunk)
            _write_all(staging_fd, chunk)
        os.fsync(staging_fd)
        staged = os.fstat(staging_fd)
        if create_only:
            os.link(
                staging,
                leaf,
                src_dir_fd=parent_fd,
                dst_dir_fd=parent_fd,
                follow_symlinks=False,
            )
        else:
            os.replace(staging, leaf, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        _fsync(parent_fd)
        return LocalFileFacts(size_bytes, digest.hexdigest(), staged.st_mtime)
    finally:
        os.close(source_fd)
        if staging_fd >= 0:
            os.close(staging_fd)
        try:
            os.unlink(staging, dir_fd=parent_fd)
        except FileNotFoundError:
            pass


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("Object storage staging write made no progress")
        view = view[written:]


def _descriptor_facts(descriptor: int) -> LocalFileFacts:
    before = os.fstat(descriptor)
    if not stat.S_ISREG(before.st_mode):
        raise ValueError("Object storage object must be a regular file")
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    size_bytes = 0
    while chunk := os.read(descriptor, 1024 * 1024):
        digest.update(chunk)
        size_bytes += len(chunk)
    after = os.fstat(descriptor)
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after or size_bytes != after.st_size:
        raise OSError("Object storage object changed while it was being read")
    return LocalFileFacts(size_bytes, digest.hexdigest(), after.st_mtime)


def _walk_files(directory_fd: int, prefix: tuple[str, ...]) -> Iterator[LocalTreeFile]:
    for name in sorted(os.listdir(directory_fd)):
        mode = os.stat(name, dir_fd=directory_fd, follow_symlinks=False).st_mode
        child_parts = (*prefix, name)
        if stat.S_ISREG(mode):
            descriptor = _open_regular(directory_fd, name)
            try:
                yield LocalTreeFile(child_parts, _descriptor_facts(descriptor))
            finally:
                os.close(descriptor)
        elif stat.S_ISDIR(mode):
            child_fd = _open_directory(directory_fd, name, create=False)
            try:
                yield from _walk_files(child_fd, child_parts)
            finally:
                os.close(child_fd)
        else:
            raise ValueError("Object storage tree must not contain symlinks or special files")


def _delete_contents(directory_fd: int) -> int:
    deleted = 0
    for name in sorted(os.listdir(directory_fd)):
        mode = os.stat(name, dir_fd=directory_fd, follow_symlinks=False).st_mode
        if stat.S_ISREG(mode):
            os.unlink(name, dir_fd=directory_fd)
            deleted += 1
        elif stat.S_ISDIR(mode):
            child_fd = _open_directory(directory_fd, name, create=False)
            try:
                deleted += _delete_contents(child_fd)
                _assert_same_directory(directory_fd, name, child_fd)
            finally:
                os.close(child_fd)
            os.rmdir(name, dir_fd=directory_fd)
        else:
            raise ValueError("Object storage tree must not contain symlinks or special files")
    _fsync(directory_fd)
    return deleted


def _assert_same_directory(parent_fd: int, name: str, opened_fd: int) -> None:
    current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    opened = os.fstat(opened_fd)
    if not stat.S_ISDIR(current.st_mode) or (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
        raise ValueError("Object storage directory changed during deletion")


def _fsync(descriptor: int) -> None:
    os.fsync(descriptor)


def _safe_path_error(exc: OSError) -> ValueError | OSError:
    if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
        return ValueError("Object storage path must not contain symlink components")
    return exc


__all__ = ["AnchoredLocalFilesystem", "LocalFileFacts", "LocalTreeFile"]
