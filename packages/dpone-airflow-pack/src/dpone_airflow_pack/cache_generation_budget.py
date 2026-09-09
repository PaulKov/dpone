"""Concurrent capacity reservations for bounded legacy cache downloads."""

from __future__ import annotations

import math
import os
import socket
import stat
import time
from collections.abc import Iterator
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import TypeGuard
from uuid import uuid4

from dpone_airflow_pack.cache_activation_contract import cache_write_lease
from dpone_airflow_pack.cache_generation_files import read_control_json, unlink_durable, write_json_durable
from dpone_airflow_pack.cache_permissions import (
    SHARED_CONTROL_MODE,
    ensure_control_file_mode,
    ensure_shared_directory,
)
from dpone_airflow_pack.diagnostic_warnings import emit_nonfatal_runtime_warning

_CONTROL_RESERVE_BYTES = 128 * 1024
_RESERVATION_TTL_SECONDS = 30 * 60
_RESERVATION_SCHEMA = "dpone.airflow-pack-cache-reservation.v1"
_OPEN_FLAGS = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)


@contextmanager
def cache_capacity_reservation(
    cache_root: Path,
    *,
    requested_payload_bytes: int,
    max_total_bytes: int | None,
) -> Iterator[None]:
    """Reserve worst-case artifact bytes before any generation payload is written."""

    if max_total_bytes is None:
        yield
        return
    reservation_id = uuid4().hex
    reservation_path = cache_root / ".reservations" / f"{reservation_id}.json"
    reserved = requested_payload_bytes + _CONTROL_RESERVE_BYTES
    descriptor: int | None = None
    with cache_write_lease(cache_root):
        ensure_shared_directory(reservation_path.parent)
        projected = _accounted_cache_bytes(cache_root) + _reserved_bytes(cache_root) + reserved
        if projected > max_total_bytes:
            raise ValueError(
                f"airflow_pack_cache_capacity_unavailable: projected {projected} > max_total_bytes {max_total_bytes}"
            )
        created_at = time.time()
        write_json_durable(
            reservation_path,
            {
                "schema": _RESERVATION_SCHEMA,
                "reservation_id": reservation_id,
                "reserved_bytes": reserved,
                "hostname": socket.gethostname(),
                "pid": os.getpid(),
                "created_at_epoch": created_at,
                "expires_at_epoch": created_at + _RESERVATION_TTL_SECONDS,
            },
            mode=SHARED_CONTROL_MODE,
        )
        try:
            descriptor = _acquire_reservation_lease(reservation_path)
        except BaseException:
            unlink_durable(reservation_path)
            raise
    assert descriptor is not None
    body_failed = False
    try:
        yield
    except BaseException:
        body_failed = True
        raise
    finally:
        cleanup_failures: list[Exception] = []
        try:
            with cache_write_lease(cache_root):
                unlink_durable(reservation_path)
        except Exception as exc:  # noqa: BLE001 - preserve the body failure below.
            cleanup_failures.append(exc)
        try:
            _release_reservation_lease(descriptor)
        except Exception as exc:  # noqa: BLE001 - preserve the body failure below.
            cleanup_failures.append(exc)
        if cleanup_failures:
            if not body_failed:
                raise cleanup_failures[0]
            emit_nonfatal_runtime_warning(
                "DPONE_CACHE_CAPACITY_RESERVATION_CLEANUP_WARNING:"
                + ",".join(exc.__class__.__name__ for exc in cleanup_failures),
                stacklevel=2,
            )


def _accounted_cache_bytes(cache_root: Path) -> int:
    excluded = {".stage-locks", ".reservations"}
    total = 0
    for directory, directory_names, file_names in os.walk(
        cache_root,
        followlinks=False,
        onerror=_raise_walk_error,
    ):
        parent = Path(directory)
        if parent == cache_root:
            directory_names[:] = [name for name in directory_names if name not in excluded]
        directory_names[:] = [name for name in directory_names if not (parent / name).is_symlink()]
        for name in file_names:
            path = parent / name
            if not path.is_symlink():
                total += path.stat().st_size
    return total


def _reserved_bytes(cache_root: Path) -> int:
    root = cache_root / ".reservations"
    if not root.exists() or root.is_symlink():
        return 0
    total = 0
    for path in sorted(root.glob("*.json")):
        try:
            reserved, expires_at = _read_reservation(path)
        except FileNotFoundError:
            continue
        except (OSError, TypeError, ValueError) as exc:
            raise ValueError(f"airflow_pack_cache_capacity_reservation_invalid: {path.name}") from exc
        if expires_at is not None and expires_at <= time.time() and not _reservation_has_active_lease(path):
            unlink_durable(path)
            continue
        total += reserved
    return total


def _read_reservation(path: Path) -> tuple[int, float | None]:
    payload = read_control_json(path)
    reserved = payload.get("reserved_bytes")
    if not isinstance(reserved, int) or isinstance(reserved, bool) or reserved <= 0:
        raise ValueError("reserved_bytes")
    schema = payload.get("schema")
    if schema is None:
        return reserved, path.lstat().st_mtime + _RESERVATION_TTL_SECONDS
    if schema != _RESERVATION_SCHEMA or payload.get("reservation_id") != path.stem:
        raise ValueError("schema or reservation_id")
    hostname = payload.get("hostname")
    pid = payload.get("pid")
    created_at = payload.get("created_at_epoch")
    expires_at = payload.get("expires_at_epoch")
    if not isinstance(hostname, str) or not hostname:
        raise ValueError("hostname")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise ValueError("pid")
    if not _is_number(created_at) or not _is_number(expires_at) or float(expires_at) <= float(created_at):
        raise ValueError("reservation timestamps")
    return reserved, float(expires_at)


def _reservation_has_active_lease(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("reservation lease must be a regular file")
    file_lock = import_module("fcntl")
    descriptor = os.open(path, _OPEN_FLAGS)
    try:
        try:
            file_lock.flock(descriptor, file_lock.LOCK_EX | file_lock.LOCK_NB)
        except BlockingIOError:
            return True
        file_lock.flock(descriptor, file_lock.LOCK_UN)
        return False
    finally:
        os.close(descriptor)


def _acquire_reservation_lease(path: Path) -> int:
    descriptor = os.open(path, _OPEN_FLAGS)
    try:
        ensure_control_file_mode(descriptor, SHARED_CONTROL_MODE)
        file_lock = import_module("fcntl")
        file_lock.flock(descriptor, file_lock.LOCK_SH)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _release_reservation_lease(descriptor: int) -> None:
    file_lock = import_module("fcntl")
    try:
        file_lock.flock(descriptor, file_lock.LOCK_UN)
    finally:
        os.close(descriptor)


def _is_number(value: object) -> TypeGuard[int | float]:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _raise_walk_error(error: OSError) -> None:
    raise error


__all__ = ["cache_capacity_reservation"]
