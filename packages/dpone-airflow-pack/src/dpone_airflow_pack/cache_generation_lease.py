"""Attempt-scoped leases protecting slow legacy-cache downloads."""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from importlib import import_module
from pathlib import Path

from dpone_airflow_pack.cache_generation_files import read_control_json
from dpone_airflow_pack.cache_permissions import (
    SHARED_CONTROL_MODE,
    ensure_control_file_mode,
    ensure_shared_directory,
)
from dpone_airflow_pack.diagnostic_warnings import emit_nonfatal_runtime_warning

_OPEN_FLAGS = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
_CREATE_FLAGS = _OPEN_FLAGS | os.O_CREAT


class StageLeaseStatus(Enum):
    """Fail-closed state of a process-independent stage attempt lease."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    UNCERTAIN = "uncertain"


@dataclass(slots=True)
class StageDetachLease:
    """Exclusive stage lease whose lock is retired only after a detach."""

    acquired: bool
    lock_path: Path
    detached: bool = False

    def mark_detached(self) -> None:
        """Record that the protected stage path no longer exists."""

        if not self.acquired:
            raise RuntimeError("airflow_pack_cache_stage_detach_without_lease")
        self.detached = True


def ensure_stage_lock(stage: Path, marker_name: str) -> Path:
    """Create the shared lock inode associated with one private stage attempt."""

    path = _stage_lock_path(stage, marker_name=marker_name)
    ensure_shared_directory(path.parent)
    descriptor = os.open(path, _CREATE_FLAGS, SHARED_CONTROL_MODE)
    try:
        _require_regular_lock(descriptor)
        ensure_control_file_mode(descriptor, SHARED_CONTROL_MODE)
    finally:
        os.close(descriptor)
    return path


@contextmanager
def generation_stage_lease(stage: Path, *, marker_name: str) -> Iterator[None]:
    """Hold a shared lease for the complete remote-read/write attempt."""

    path = ensure_stage_lock(stage, marker_name)
    descriptor = _open_regular_lock(path)
    file_lock = import_module("fcntl")
    try:
        file_lock.flock(descriptor, file_lock.LOCK_SH)
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
                f"DPONE_CACHE_STAGE_LEASE_RELEASE_WARNING:{','.join(failures)}:{path}",
                stacklevel=2,
            )


@contextmanager
def exclusive_stage_detach_lease(stage: Path, *, marker_name: str) -> Iterator[StageDetachLease]:
    """Try to exclude producers while a stale stage is revalidated and detached."""

    path = ensure_stage_lock(stage, marker_name)
    descriptor = _open_regular_lock(path)
    file_lock = import_module("fcntl")
    lease = StageDetachLease(acquired=False, lock_path=path)
    try:
        try:
            file_lock.flock(descriptor, file_lock.LOCK_EX | file_lock.LOCK_NB)
        except BlockingIOError:
            yield lease
            return
        lease.acquired = True
        yield lease
    finally:
        if lease.acquired:
            if lease.detached:
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    emit_nonfatal_runtime_warning(
                        f"DPONE_CACHE_STAGE_LOCK_CLEANUP_WARNING:{exc.__class__.__name__}:{path}",
                        stacklevel=2,
                    )
            try:
                file_lock.flock(descriptor, file_lock.LOCK_UN)
            except OSError as exc:
                emit_nonfatal_runtime_warning(
                    f"DPONE_CACHE_STAGE_LEASE_RELEASE_WARNING:unlock:{exc.__class__.__name__}:{path}",
                    stacklevel=2,
                )
        try:
            os.close(descriptor)
        except OSError as exc:
            emit_nonfatal_runtime_warning(
                f"DPONE_CACHE_STAGE_LEASE_RELEASE_WARNING:close:{exc.__class__.__name__}:{path}",
                stacklevel=2,
            )


def stage_has_active_lease(stage: Path, *, marker_name: str) -> bool:
    """Return true when a producer owns the stage or its lease is uncertain."""

    return stage_lease_status(stage, marker_name=marker_name) is not StageLeaseStatus.INACTIVE


def stage_lease_status(stage: Path, *, marker_name: str) -> StageLeaseStatus:
    """Inspect one attempt lease without treating unreadable evidence as inactive."""

    try:
        return _read_stage_lease_status(stage, marker_name=marker_name)
    except (ImportError, OSError, TypeError, ValueError):
        return StageLeaseStatus.UNCERTAIN


def _read_stage_lease_status(stage: Path, *, marker_name: str) -> StageLeaseStatus:
    """Read a lease state and raise when its evidence cannot be validated."""

    path = _stage_lock_path(stage, marker_name=marker_name)
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return StageLeaseStatus.INACTIVE
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("airflow_pack_cache_stage_lease_invalid: lock must be a regular file")
    try:
        descriptor = _open_regular_lock(path)
    except FileNotFoundError:
        return StageLeaseStatus.INACTIVE
    file_lock = import_module("fcntl")
    try:
        try:
            file_lock.flock(descriptor, file_lock.LOCK_EX | file_lock.LOCK_NB)
        except BlockingIOError:
            return StageLeaseStatus.ACTIVE
        file_lock.flock(descriptor, file_lock.LOCK_UN)
        return StageLeaseStatus.INACTIVE
    finally:
        os.close(descriptor)


def _stage_lock_path(stage: Path, *, marker_name: str) -> Path:
    marker = read_control_json(stage / marker_name)
    attempt_id = marker.get("attempt_id")
    if not isinstance(attempt_id, str) or not attempt_id or not attempt_id.isalnum():
        raise ValueError("airflow_pack_cache_stage_invalid: attempt_id")
    return stage.parent.parent / ".stage-locks" / f"{attempt_id}.lock"


def _require_regular_lock(descriptor: int) -> None:
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        raise ValueError("airflow_pack_cache_stage_lease_invalid: lock must be a regular file")


def _open_regular_lock(path: Path) -> int:
    descriptor = os.open(path, _OPEN_FLAGS)
    try:
        _require_regular_lock(descriptor)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


__all__ = [
    "StageDetachLease",
    "StageLeaseStatus",
    "ensure_stage_lock",
    "exclusive_stage_detach_lease",
    "generation_stage_lease",
    "stage_has_active_lease",
    "stage_lease_status",
]
