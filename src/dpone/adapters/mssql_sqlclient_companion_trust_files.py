"""Descriptor-aware bounded filesystem checks for companion trust admission."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tarfile
from pathlib import Path
from typing import Any

ERROR = "mssql_native.sqlclient_companion_untrusted"
RELATIVE = re.compile(r"[A-Za-z0-9_.+-]+(?:/[A-Za-z0-9_.+-]+)*")
MAX_TREE_BYTES = 2 * 1024**3
MAX_FILE_BYTES = 512 * 1024**2
MAX_FILES = 8192
MAX_DIRECTORIES = 8192
MAX_DEPTH = 16


def root_snapshot(path: Path, *, production: bool) -> tuple[tuple[Path, tuple[int, ...]], ...]:
    if not path.is_absolute():
        raise ValueError(ERROR)
    owner = 0 if production else os.geteuid()
    snapshots: list[tuple[Path, tuple[int, ...]]] = []
    current = Path("/")
    for part in path.parts[1:]:
        current /= part
        value = current.lstat()
        if stat.S_ISLNK(value.st_mode) or not stat.S_ISDIR(value.st_mode):
            raise ValueError(ERROR)
        insecure_write = bool(value.st_mode & 0o022) and not (value.st_uid == 0 and value.st_mode & stat.S_ISVTX)
        if insecure_write:
            raise ValueError(ERROR)
        snapshots.append((current, directory_identity(value)))
    if path.resolve(strict=True) != path:
        raise ValueError(ERROR)
    directories = entries = 0
    for base, names, files, descriptor in os.fwalk(path, topdown=True, follow_symlinks=False):
        directories += 1
        relative = Path(base).relative_to(path)
        if directories > MAX_DIRECTORIES or len(relative.parts) > MAX_DEPTH:
            raise ValueError(ERROR)
        for name in (*names, *files):
            entries += 1
            if entries > MAX_DIRECTORIES + MAX_FILES or not _component(name):
                raise ValueError(ERROR)
            value = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISLNK(value.st_mode) or value.st_uid != owner or value.st_mode & 0o222:
                raise ValueError(ERROR)
            if stat.S_ISREG(value.st_mode) and value.st_nlink != 1:
                raise ValueError(ERROR)
    return tuple(snapshots)


def assert_snapshot(snapshot: tuple[tuple[Path, tuple[int, ...]], ...]) -> None:
    for path, expected in snapshot:
        if directory_identity(path.lstat()) != expected:
            raise ValueError(ERROR)


def read_file(path: Path, maximum: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
            raise ValueError(ERROR)
        value = bytearray()
        while len(value) <= maximum:
            block = os.read(descriptor, min(65536, maximum + 1 - len(value)))
            if not block:
                break
            value.extend(block)
        after, named = os.fstat(descriptor), path.lstat()
        if len(value) > maximum or identity(before) != identity(after) or identity(after) != identity(named):
            raise ValueError(ERROR)
        return bytes(value)
    finally:
        os.close(descriptor)


def read_at(directory_fd: int, name: str, maximum: int) -> bytes:
    if not _component(name):
        raise ValueError(ERROR)
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > maximum:
            raise ValueError(ERROR)
        value = bytearray()
        while len(value) <= maximum:
            block = os.read(descriptor, min(65536, maximum + 1 - len(value)))
            if not block:
                break
            value.extend(block)
        after = os.fstat(descriptor)
        named = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if len(value) > maximum or identity(before) != identity(after) or identity(after) != identity(named):
            raise ValueError(ERROR)
        return bytes(value)
    finally:
        os.close(descriptor)


def read_root_file(root: Path, name: str, maximum: int) -> bytes:
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        return read_at(directory, name, maximum)
    finally:
        os.close(directory)


def inventory(root: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    total = directories = 0
    for base, names, files, descriptor in os.fwalk(root, topdown=True, follow_symlinks=False):
        directories += 1
        relative_root = Path(base).relative_to(root)
        if directories > MAX_DIRECTORIES or len(relative_root.parts) > MAX_DEPTH:
            raise ValueError(ERROR)
        for entry in (*names, *files):
            if not _component(entry):
                raise ValueError(ERROR)
            value = os.stat(entry, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISLNK(value.st_mode) or (not stat.S_ISREG(value.st_mode) and not stat.S_ISDIR(value.st_mode)):
                raise ValueError(ERROR)
            if stat.S_ISREG(value.st_mode) and value.st_nlink != 1:
                raise ValueError(ERROR)
        for entry in files:
            if len(result) >= MAX_FILES:
                raise ValueError(ERROR)
            name = (relative_root / entry).as_posix()
            if RELATIVE.fullmatch(name) is None:
                raise ValueError(ERROR)
            data = read_at(descriptor, entry, MAX_FILE_BYTES)
            total += len(data)
            if total > MAX_TREE_BYTES:
                raise ValueError(ERROR)
            result[name] = hashlib.sha256(data).hexdigest()
    return result


def inventory_digest(root: Path) -> str:
    raw = json.dumps(inventory(root), ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()


def file_digest(path: Path, *, maximum: int = MAX_TREE_BYTES) -> str:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
            raise ValueError(ERROR)
        digest, total = hashlib.sha256(), 0
        while True:
            block = os.read(descriptor, 1024**2)
            if not block:
                break
            total += len(block)
            if total > maximum:
                raise ValueError(ERROR)
            digest.update(block)
        after, named = os.fstat(descriptor), path.lstat()
        if identity(before) != identity(after) or identity(after) != identity(named):
            raise ValueError(ERROR)
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def secure_executable_bytes(path: Path, expected_sha256: str) -> bytes:
    snapshots = []
    current = Path("/")
    for part in path.parts[1:-1]:
        current /= part
        value = current.lstat()
        insecure_write = bool(value.st_mode & 0o022) and not (value.st_uid == 0 and value.st_mode & stat.S_ISVTX)
        if stat.S_ISLNK(value.st_mode) or not stat.S_ISDIR(value.st_mode) or insecure_write:
            raise ValueError(ERROR)
        snapshots.append((current, directory_identity(value)))
    data = read_file(path, 256 * 1024**2)
    details = path.lstat()
    if (
        not stat.S_ISREG(details.st_mode)
        or not details.st_mode & 0o111
        or details.st_mode & 0o022
        or details.st_nlink != 1
        or hashlib.sha256(data).hexdigest() != expected_sha256
    ):
        raise ValueError(ERROR)
    assert_snapshot(tuple(snapshots))
    return data


def validate_archive(path: Path, root: Path) -> None:
    actual, seen = inventory(root), {}
    prefix, count, total = root.name + "/", 0, 0
    try:
        with tarfile.open(path, "r:") as archive:
            for member in archive:
                count += 1
                if count > MAX_FILES * 2 or member.size > MAX_FILE_BYTES:
                    raise ValueError(ERROR)
                if member.name == root.name and member.isdir():
                    continue
                if not member.name.startswith(prefix) or member.issym() or member.islnk():
                    raise ValueError(ERROR)
                relative = member.name[len(prefix) :].rstrip("/")
                if not relative or RELATIVE.fullmatch(relative) is None:
                    raise ValueError(ERROR)
                if member.isfile():
                    stream = archive.extractfile(member)
                    if stream is None or relative in seen:
                        raise ValueError(ERROR)
                    digest, remaining = hashlib.sha256(), member.size
                    while remaining:
                        block = stream.read(min(1024**2, remaining))
                        if not block:
                            raise ValueError(ERROR)
                        remaining -= len(block)
                        total += len(block)
                        if total > MAX_TREE_BYTES:
                            raise ValueError(ERROR)
                        digest.update(block)
                    seen[relative] = digest.hexdigest()
                elif not member.isdir():
                    raise ValueError(ERROR)
                if member.uid or member.gid or member.mtime or member.uname or member.gname:
                    raise ValueError(ERROR)
    except (tarfile.TarError, OSError) as error:
        raise ValueError(ERROR) from error
    if seen != actual:
        raise ValueError(ERROR)


def validate_archive_bound(path: Path, root: Path, expected_sha256: str) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > MAX_TREE_BYTES:
            raise ValueError(ERROR)
        digest, total = hashlib.sha256(), 0
        while block := os.read(descriptor, 1024**2):
            total += len(block)
            if total > MAX_TREE_BYTES:
                raise ValueError(ERROR)
            digest.update(block)
        if digest.hexdigest() != expected_sha256:
            raise ValueError(ERROR)
        os.lseek(descriptor, 0, os.SEEK_SET)
        duplicate = os.dup(descriptor)
        try:
            _validate_archive_stream(_BoundedStream(os.fdopen(duplicate, "rb"), MAX_TREE_BYTES), root)
        except BaseException:
            os.close(duplicate) if _fd_open(duplicate) else None
            raise
        after, named = os.fstat(descriptor), path.lstat()
        if identity(before) != identity(after) or identity(after) != identity(named):
            raise ValueError(ERROR)
    finally:
        os.close(descriptor)


def _validate_archive_stream(stream: Any, root: Path) -> None:
    actual, seen = inventory(root), {}
    prefix, count, total = root.name + "/", 0, 0
    try:
        with stream, tarfile.open(fileobj=stream, mode="r:") as archive:
            for member in archive:
                count += 1
                if count > MAX_FILES * 2 or member.size > MAX_FILE_BYTES:
                    raise ValueError(ERROR)
                if member.name == root.name and member.isdir():
                    continue
                if not member.name.startswith(prefix) or member.issym() or member.islnk():
                    raise ValueError(ERROR)
                relative = member.name[len(prefix) :].rstrip("/")
                if not relative or RELATIVE.fullmatch(relative) is None:
                    raise ValueError(ERROR)
                if member.isfile():
                    extracted = archive.extractfile(member)
                    if extracted is None or relative in seen:
                        raise ValueError(ERROR)
                    digest, remaining = hashlib.sha256(), member.size
                    while remaining:
                        block = extracted.read(min(1024**2, remaining))
                        if not block:
                            raise ValueError(ERROR)
                        remaining -= len(block)
                        total += len(block)
                        if total > MAX_TREE_BYTES:
                            raise ValueError(ERROR)
                        digest.update(block)
                    seen[relative] = digest.hexdigest()
                elif not member.isdir():
                    raise ValueError(ERROR)
                if member.uid or member.gid or member.mtime or member.uname or member.gname:
                    raise ValueError(ERROR)
    except (tarfile.TarError, OSError) as error:
        raise ValueError(ERROR) from error
    if seen != actual:
        raise ValueError(ERROR)


def _fd_open(descriptor: int) -> bool:
    try:
        os.fstat(descriptor)
    except OSError:
        return False
    return True


class _BoundedStream:
    def __init__(self, stream: Any, maximum: int) -> None:
        self._stream, self._maximum, self._consumed = stream, maximum, 0

    def read(self, size: int = -1) -> bytes:
        value = self._stream.read(size)
        self._consumed += len(value)
        if self._consumed > self._maximum:
            raise ValueError(ERROR)
        return value

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        return self._stream.seek(offset, whence)

    def tell(self) -> int:
        return self._stream.tell()

    def close(self) -> None:
        self._stream.close()

    def __enter__(self) -> _BoundedStream:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def strict_object(raw: bytes) -> dict[str, Any]:
    def unique(rows: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in rows:
            if key in result:
                raise ValueError(ERROR)
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode(), object_pairs_hook=unique)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(ERROR) from error
    if type(value) is not dict:
        raise ValueError(ERROR)
    return value


def identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def directory_identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_mode, value.st_uid)


def _component(value: str) -> bool:
    return 0 < len(value.encode()) <= 255 and value not in {".", ".."} and "/" not in value and "\x00" not in value


__all__ = [
    "ERROR",
    "RELATIVE",
    "assert_snapshot",
    "file_digest",
    "inventory",
    "inventory_digest",
    "read_at",
    "read_file",
    "read_root_file",
    "root_snapshot",
    "secure_executable_bytes",
    "strict_object",
    "validate_archive",
    "validate_archive_bound",
]
