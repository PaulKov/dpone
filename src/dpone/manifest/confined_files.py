"""Descriptor-anchored reads for project-confined build-plane inputs."""

from __future__ import annotations

import errno
import hashlib
import os
import stat
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol


class ConfinedRootIdentity(Protocol):
    """Stable root identity accepted by descriptor-confined reads."""

    @property
    def path(self) -> Path: ...

    def matches(self, metadata: os.stat_result) -> bool: ...


class ConfinedFileError(OSError):
    """A project file could not be opened through the no-follow fd walk."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class ConfinedFileIdentity:
    """Stable descriptor identity used to reject torn in-place snapshots."""

    device: int
    inode: int
    mode: int
    size: int
    modified_ns: int
    changed_ns: int

    @classmethod
    def from_stat(cls, metadata: os.stat_result) -> ConfinedFileIdentity:
        return cls(
            device=metadata.st_dev,
            inode=metadata.st_ino,
            mode=metadata.st_mode,
            size=metadata.st_size,
            modified_ns=metadata.st_mtime_ns,
            changed_ns=metadata.st_ctime_ns,
        )


@dataclass(frozen=True, slots=True)
class ConfinedFileSnapshot:
    """Bounded bytes and identity captured from one stable regular-file descriptor."""

    identity: ConfinedFileIdentity
    content: bytes
    sha256: str


def read_confined_file(
    root: Path,
    relative_path: str,
    *,
    max_bytes: int,
    follow_in_root_symlinks: bool = False,
    root_identity: ConfinedRootIdentity | None = None,
) -> bytes:
    """Read one regular file from a stable root fd with a hard byte limit."""

    return read_confined_file_snapshot(
        root,
        relative_path,
        max_bytes=max_bytes,
        follow_in_root_symlinks=follow_in_root_symlinks,
        root_identity=root_identity,
    ).content


def read_confined_file_snapshot(
    root: Path,
    relative_path: str,
    *,
    max_bytes: int,
    follow_in_root_symlinks: bool = False,
    root_identity: ConfinedRootIdentity | None = None,
) -> ConfinedFileSnapshot:
    """Read bounded bytes and their digest from the same stable descriptor."""

    with _open_confined_file(
        root,
        relative_path,
        follow_in_root_symlinks=follow_in_root_symlinks,
        root_identity=root_identity,
    ) as descriptor:
        return read_stable_descriptor(descriptor, max_bytes=max_bytes)


def sha256_confined_file(
    root: Path,
    relative_path: str,
    *,
    follow_in_root_symlinks: bool = False,
    max_bytes: int | None = None,
    root_identity: ConfinedRootIdentity | None = None,
) -> str:
    """Hash one stable regular-file descriptor without loading it into memory."""

    digest = hashlib.sha256()
    with _open_confined_file(
        root,
        relative_path,
        follow_in_root_symlinks=follow_in_root_symlinks,
        root_identity=root_identity,
    ) as descriptor:
        _consume_stable_descriptor(descriptor, max_bytes=max_bytes, consumer=digest.update)
    return "sha256:" + digest.hexdigest()


def read_confined_leaf(
    parent_fd: int,
    name: str,
    *,
    max_bytes: int,
) -> ConfinedFileSnapshot:
    """Read one no-follow sibling leaf relative to an already confined directory."""

    file_flags = os.O_RDONLY | _flag("O_CLOEXEC") | _flag("O_NOFOLLOW") | _flag("O_NONBLOCK")
    descriptor: int | None = None
    try:
        descriptor = os.open(name, file_flags, dir_fd=parent_fd)
        return read_stable_descriptor(descriptor, max_bytes=max_bytes)
    except ConfinedFileError:
        raise
    except OSError as exc:
        raise _open_error(exc) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def read_stable_descriptor(descriptor: int, *, max_bytes: int) -> ConfinedFileSnapshot:
    """Return bytes only when one descriptor's before/after identity stayed stable."""

    chunks: list[bytes] = []
    identity = _consume_stable_descriptor(descriptor, max_bytes=max_bytes, consumer=chunks.append)
    content = b"".join(chunks)
    return ConfinedFileSnapshot(
        identity=identity,
        content=content,
        sha256="sha256:" + hashlib.sha256(content).hexdigest(),
    )


def _consume_stable_descriptor(
    descriptor: int,
    *,
    max_bytes: int | None,
    consumer: Callable[[bytes], object],
) -> ConfinedFileIdentity:
    if max_bytes is not None and max_bytes < 0:
        raise ValueError("max_bytes must be non-negative")
    before = os.fstat(descriptor)
    if not stat.S_ISREG(before.st_mode):
        raise ConfinedFileError("not_regular_file", "Project input is not a regular file.")
    if max_bytes is not None and before.st_size > max_bytes:
        raise ConfinedFileError("file_too_large", "Project file exceeds its byte limit.")
    byte_count = 0
    digest = hashlib.sha256()
    remaining = before.st_size + 1 if max_bytes is None else max_bytes + 1
    while remaining > 0:
        read_size = min(64 * 1024, remaining)
        chunk = _read_chunk(descriptor, read_size)
        if not chunk:
            break
        consumer(chunk)
        digest.update(chunk)
        byte_count += len(chunk)
        remaining -= len(chunk)
    after = os.fstat(descriptor)
    before_identity = ConfinedFileIdentity.from_stat(before)
    after_identity = ConfinedFileIdentity.from_stat(after)
    if before_identity != after_identity or byte_count != after_identity.size:
        raise ConfinedFileError("source_changed", "Project file changed while it was read.")
    if max_bytes is not None and byte_count > max_bytes:
        raise ConfinedFileError("file_too_large", "Project file exceeds its byte limit.")
    _verify_descriptor_content(descriptor, after_identity, digest.digest())
    return after_identity


def _verify_descriptor_content(descriptor: int, identity: ConfinedFileIdentity, expected_digest: bytes) -> None:
    """Compare bounded reread bytes when filesystem timestamps may be coarse.

    This checks content consistency across two passes through the held file;
    it cannot certify absence of every transient or subsequent mutation.
    """
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    byte_count = 0
    remaining = identity.size + 1
    while remaining > 0:
        chunk = _read_chunk(descriptor, min(64 * 1024, remaining))
        if not chunk:
            break
        digest.update(chunk)
        byte_count += len(chunk)
        remaining -= len(chunk)
    if (
        byte_count != identity.size
        or digest.digest() != expected_digest
        or ConfinedFileIdentity.from_stat(os.fstat(descriptor)) != identity
    ):
        raise ConfinedFileError("source_changed", "Project file changed while it was read.")


def _read_chunk(descriptor: int, size: int) -> bytes:
    return os.read(descriptor, size)


def project_relative_path(root: Path, path: Path) -> str:
    """Return a lexical project-relative POSIX path without following symlinks."""

    root_path = root.resolve(strict=True)
    raw = path.as_posix()
    parsed = PurePosixPath(raw)
    if not raw or "\\" in raw or ".." in parsed.parts or "." in parsed.parts:
        raise ConfinedFileError("path_invalid", "Project input path is not a safe relative POSIX path.")
    if path.is_absolute():
        try:
            relative = Path(os.path.abspath(path)).relative_to(root_path)
        except ValueError as exc:
            raise ConfinedFileError("path_invalid", "Project input path is outside the project root.") from exc
        parsed = PurePosixPath(relative.as_posix())
    if parsed.is_absolute() or not parsed.parts:
        raise ConfinedFileError("path_invalid", "Project input path is not a safe relative POSIX path.")
    return parsed.as_posix()


@contextmanager
def _open_confined_file(
    root: Path,
    relative_path: str,
    *,
    follow_in_root_symlinks: bool,
    root_identity: ConfinedRootIdentity | None,
) -> Iterator[int]:
    root_path = root_identity.path if root_identity is not None else root.resolve(strict=True)
    parts = _path_parts(root_path, relative_path, follow_in_root_symlinks=follow_in_root_symlinks)
    if sys.platform == "win32":
        if root_identity is not None and not root_identity.matches(os.stat(root_path, follow_symlinks=False)):
            raise ConfinedFileError("root_changed", "Project root changed during the operation.")
        with _open_confined_file_windows(root_path, parts) as descriptor:
            yield descriptor
        return
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | _flag("O_CLOEXEC") | _flag("O_NOFOLLOW")
    # A repository-controlled FIFO must not block the authoring command before
    # we can reject it as a non-regular file.
    file_flags = os.O_RDONLY | _flag("O_CLOEXEC") | _flag("O_NOFOLLOW") | _flag("O_NONBLOCK")
    root_fd = os.open(root_path, directory_flags)
    parent_fd = root_fd
    file_fd: int | None = None
    try:
        if root_identity is not None and not root_identity.matches(os.fstat(root_fd)):
            raise ConfinedFileError("root_changed", "Project root changed during the operation.")
        for part in parts[:-1]:
            next_fd = os.open(part, directory_flags, dir_fd=parent_fd)
            if parent_fd != root_fd:
                os.close(parent_fd)
            parent_fd = next_fd
        file_fd = os.open(parts[-1], file_flags, dir_fd=parent_fd)
        if not stat.S_ISREG(os.fstat(file_fd).st_mode):
            raise ConfinedFileError("not_regular_file", "Project input is not a regular file.")
        yield file_fd
    except ConfinedFileError:
        raise
    except OSError as exc:
        raise _open_error(exc) from exc
    finally:
        if file_fd is not None:
            os.close(file_fd)
        if parent_fd != root_fd:
            os.close(parent_fd)
        os.close(root_fd)


def _path_parts(root: Path, relative_path: str, *, follow_in_root_symlinks: bool) -> tuple[str, ...]:
    path = PurePosixPath(relative_path)
    if not relative_path or "\\" in relative_path or path.is_absolute() or ".." in path.parts or not path.parts:
        raise ConfinedFileError("path_invalid", "Project input path is not a safe relative POSIX path.")
    if not follow_in_root_symlinks:
        return path.parts
    try:
        resolved = root.joinpath(*path.parts).resolve(strict=True)
        return resolved.relative_to(root).parts
    except (OSError, ValueError) as exc:
        raise ConfinedFileError("file_unavailable", "Project input resolves outside the project root.") from exc


def _flag(name: str) -> int:
    return int(getattr(os, name, 0))


def _open_error(error: OSError) -> ConfinedFileError:
    if error.errno in {errno.ELOOP, errno.ENOTDIR}:
        code = "symlink_forbidden"
    elif error.errno == errno.ENOENT:
        code = "file_not_found"
    else:
        code = "file_unavailable"
    return ConfinedFileError(code, "Project input could not be opened safely.")


@contextmanager
def _open_confined_file_windows(root: Path, parts: tuple[str, ...]) -> Iterator[int]:
    """Delegate Windows handle verification without importing Win32 APIs on POSIX."""

    from dpone.manifest.confined_files_windows import open_confined_file_windows

    with open_confined_file_windows(root, parts) as descriptor:
        yield descriptor


__all__ = [
    "ConfinedFileError",
    "ConfinedFileIdentity",
    "ConfinedFileSnapshot",
    "project_relative_path",
    "read_confined_file",
    "read_confined_file_snapshot",
    "read_confined_leaf",
    "read_stable_descriptor",
    "sha256_confined_file",
]
