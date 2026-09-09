"""Descriptor-safe local reads used by cache status diagnostics."""

from __future__ import annotations

import os
import stat
from pathlib import Path


def read_bounded_regular_file(path: Path, *, max_bytes: int) -> bytes:
    """Read one regular file without following links or blocking on special files."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("cache status input must be a regular file")
        if metadata.st_size > max_bytes:
            raise CacheStatusFileTooLarge
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > max_bytes:
            raise CacheStatusFileTooLarge
        return payload
    finally:
        os.close(descriptor)


def read_cache_status_current(root: Path, *, max_bytes: int = 4096) -> str | None:
    """Read either exact-cache pointer shape or one bounded legacy text pointer."""

    current = root / "current"
    try:
        metadata = current.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(metadata.st_mode):
        target = Path(os.readlink(current))
        return target.name or None
    if stat.S_ISDIR(metadata.st_mode):
        return current.name
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("cache current pointer must be a directory, symlink, or small regular file")
    return read_bounded_regular_file(current, max_bytes=max_bytes).decode("utf-8").strip() or None


class CacheStatusFileTooLarge(ValueError):
    """A cache status input exceeded its explicit byte boundary."""


__all__ = ["CacheStatusFileTooLarge", "read_bounded_regular_file", "read_cache_status_current"]
