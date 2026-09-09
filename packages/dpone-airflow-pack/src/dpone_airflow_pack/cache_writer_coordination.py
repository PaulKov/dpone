"""Non-parser locks serializing cache commit evidence publication."""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path

from dpone_airflow_pack.cache_permissions import (
    SHARED_CONTROL_MODE,
    ensure_control_file_mode,
    ensure_shared_directory,
)
from dpone_airflow_pack.diagnostic_warnings import emit_nonfatal_runtime_warning

_FLAGS = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)


@contextmanager
def cache_evidence_lease(cache_root: Path) -> Iterator[None]:
    """Serialize commit and mutable diagnostic publication without blocking parsers."""

    ensure_shared_directory(cache_root)
    path = cache_root / ".evidence.lock"
    descriptor = os.open(path, _FLAGS, SHARED_CONTROL_MODE)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise ValueError("cache evidence lock must be one unlinked regular inode")
        ensure_control_file_mode(descriptor, SHARED_CONTROL_MODE)
        file_lock = import_module("fcntl")
        file_lock.flock(descriptor, file_lock.LOCK_EX)
    except BaseException:
        os.close(descriptor)
        raise
    try:
        yield
    finally:
        failures: list[str] = []
        try:
            file_lock.flock(descriptor, file_lock.LOCK_UN)
        except OSError as exc:
            failures.append(f"unlock:{exc.__class__.__name__}")
        try:
            os.close(descriptor)
        except OSError as exc:
            failures.append(f"close:{exc.__class__.__name__}")
        if failures:
            emit_nonfatal_runtime_warning(
                f"DPONE_CACHE_EVIDENCE_LEASE_RELEASE_WARNING:{','.join(failures)}:{path}",
                stacklevel=2,
            )


__all__ = ["cache_evidence_lease"]
