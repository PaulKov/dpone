"""Descriptor-safe durable filesystem primitives for legacy cache generations."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
from pathlib import Path
from typing import Any
from uuid import uuid4

from dpone_airflow_pack.cache_permissions import (
    SHARED_CONTROL_MODE,
    SHARED_WORK_CONTROL_MODE,
    ensure_directory_mode,
    ensure_shared_directory,
    ensure_shared_work_directory,
)

_MAX_CONTROL_BYTES = 64 * 1024
_MAX_GENERATION_FILES = 20_000


def tree_inventory(
    root: Path,
    *,
    ignored_names: frozenset[str],
    max_total_bytes: int | None = None,
) -> tuple[tuple[tuple[str, int, str], ...], int]:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("airflow_pack_generation_conflict: generation must be a regular directory")
    pending: list[tuple[Path, str, int]] = []
    total = 0
    for directory, directory_names, file_names in os.walk(root, followlinks=False, onerror=_raise_walk_error):
        parent = Path(directory)
        for name in directory_names:
            if not stat.S_ISDIR((parent / name).lstat().st_mode):
                raise ValueError("airflow_pack_generation_conflict: generation contains unsafe entries")
        for name in sorted(file_names):
            if name in ignored_names:
                continue
            path = parent / name
            metadata = path.lstat()
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("airflow_pack_generation_conflict: generation contains unsafe entries")
            total += metadata.st_size
            pending.append((path, path.relative_to(root).as_posix(), metadata.st_size))
            if len(pending) > _MAX_GENERATION_FILES:
                raise ValueError("airflow_pack_generation_too_many_files")
            if max_total_bytes is not None and total > max_total_bytes:
                raise ValueError("airflow_pack_generation_too_large")
    entries = tuple((relative, size, file_sha256(path)) for path, relative, size in pending)
    return tuple(sorted(entries)), total


def inventory_digest(entries: tuple[tuple[str, int, str], ...]) -> str:
    raw = json.dumps(entries, ensure_ascii=False, separators=(",", ":"), sort_keys=False).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def tree_metadata_digest(root: Path) -> str:
    """Fingerprint bounded inode metadata without rereading artifact payload bytes."""

    entries: list[tuple[str, str, int, int, int, int, int, int]] = []
    for directory, directory_names, file_names in os.walk(root, followlinks=False, onerror=_raise_walk_error):
        parent = Path(directory)
        for kind, names in (("d", directory_names), ("f", file_names)):
            for name in sorted(names):
                path = parent / name
                metadata = path.lstat()
                expected = stat.S_ISDIR(metadata.st_mode) if kind == "d" else stat.S_ISREG(metadata.st_mode)
                if not expected:
                    raise ValueError("airflow_pack_generation_conflict: generation contains unsafe entries")
                entries.append(
                    (
                        path.relative_to(root).as_posix(),
                        kind,
                        metadata.st_dev,
                        metadata.st_ino,
                        metadata.st_size,
                        metadata.st_mtime_ns,
                        metadata.st_ctime_ns,
                        stat.S_IMODE(metadata.st_mode),
                    )
                )
                if len(entries) > _MAX_GENERATION_FILES:
                    raise ValueError("airflow_pack_generation_too_many_files")
    raw = json.dumps(sorted(entries), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def seal_tree(path: Path, *, seal_root: bool = True, group_managed: bool = False) -> None:
    """Publish durable bytes with either public-read or fsGroup-managed modes."""

    file_mode = 0o440 if group_managed else 0o444
    directory_mode = 0o2770 if group_managed else 0o555

    for root, directories, files in os.walk(
        path,
        topdown=False,
        followlinks=False,
        onerror=_raise_walk_error,
    ):
        root_path = Path(root)
        for name in files:
            child = root_path / name
            if child.is_symlink():
                raise ValueError("airflow_pack_generation_conflict: generation contains unsafe entries")
            descriptor = os.open(child, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                os.fchmod(descriptor, file_mode)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        for name in directories:
            child = root_path / name
            if child.is_symlink():
                raise ValueError("airflow_pack_generation_conflict: generation contains unsafe entries")
            ensure_directory_mode(child, directory_mode)
            fsync_directory(child)
        if seal_root or root_path != path:
            ensure_directory_mode(root_path, directory_mode)
        fsync_directory(root_path)


def remove_tree(path: Path) -> None:
    """Delete one detached private or fsGroup-managed tree without following symlinks."""

    if not os.path.lexists(path):
        return
    if path.is_symlink() or not path.is_dir():
        path.unlink()
        fsync_directory(path.parent)
        return
    for root, directories, files in os.walk(
        path,
        topdown=False,
        followlinks=False,
        onerror=_raise_walk_error,
    ):
        root_path = Path(root)
        _make_owner_writable_when_needed(root_path)
        for name in files:
            child = root_path / name
            if child.is_symlink():
                child.unlink()
        for name in directories:
            child = root_path / name
            if child.is_symlink():
                child.unlink()
            else:
                _make_owner_writable_when_needed(child)
    shutil.rmtree(path)
    fsync_directory(path.parent)


def write_text_durable(path: Path, value: str) -> None:
    write_bytes_durable(path, value.encode("utf-8"), mode=SHARED_CONTROL_MODE)


def _raise_walk_error(exc: OSError) -> None:
    raise exc


def write_json_durable(path: Path, payload: dict[str, Any], *, mode: int) -> None:
    raw = (json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
    write_bytes_durable(path, raw, mode=mode)


def write_bytes_durable(path: Path, payload: bytes, *, mode: int) -> None:
    if mode == SHARED_CONTROL_MODE:
        ensure_shared_directory(path.parent)
    elif mode == SHARED_WORK_CONTROL_MODE:
        ensure_shared_work_directory(path.parent)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        mode,
    )
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        if os.path.lexists(temporary):
            temporary.unlink()


def read_control_json(path: Path) -> dict[str, Any]:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _MAX_CONTROL_BYTES:
            raise ValueError(f"invalid cache control file: {path}")
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            raw = handle.read(_MAX_CONTROL_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(raw) > _MAX_CONTROL_BYTES:
        raise ValueError(f"invalid cache control file: {path}")
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"cache control file must be an object: {path}")
    return payload


def unlink_durable(path: Path) -> None:
    """Remove one control file and commit the directory entry change."""

    try:
        path.unlink()
    except FileNotFoundError:
        return
    fsync_directory(path.parent)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
    )
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("cache artifact must be a regular file")
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        after = os.fstat(descriptor)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise ValueError("cache artifact changed while it was read")
    finally:
        os.close(descriptor)
    return "sha256:" + digest.hexdigest()


def pack_file_sha256(path: Path) -> str:
    """Return the historical unprefixed checksum used by legacy pack indexes."""

    return file_sha256(path).removeprefix("sha256:")


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _make_owner_writable_when_needed(path: Path) -> None:
    metadata = path.stat(follow_symlinks=False)
    mode = stat.S_IMODE(metadata.st_mode)
    if mode & (stat.S_IWUSR | stat.S_IWGRP):
        return
    if metadata.st_uid == os.geteuid():
        ensure_directory_mode(path, mode | stat.S_IWUSR)


__all__ = [
    "file_sha256",
    "fsync_directory",
    "inventory_digest",
    "pack_file_sha256",
    "read_control_json",
    "remove_tree",
    "seal_tree",
    "tree_inventory",
    "tree_metadata_digest",
    "unlink_durable",
    "write_json_durable",
    "write_text_durable",
]
