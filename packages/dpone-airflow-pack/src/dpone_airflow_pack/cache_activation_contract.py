"""Active-cache identity and read-lease contract for Airflow parsing."""

from __future__ import annotations

import json
import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from importlib import import_module
from pathlib import Path, PurePosixPath

from dpone_airflow_pack.cache_permissions import (
    SHARED_CONTROL_MODE,
    ensure_control_file_mode,
    ensure_shared_directory,
)
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError
from dpone_airflow_pack.diagnostic_warnings import emit_nonfatal_runtime_warning

_DEPLOYMENT_DIRECTORY_PREFIX = "sha256-"
_DEPLOYMENT_HEX_LENGTH = 64
_MAX_POINTER_BYTES = 64 * 1024
_LOCK_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
_WRITE_LOCK_FLAGS = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
_DIRECTORY_LOCK_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0)
_LOCK_MODE = SHARED_CONTROL_MODE
_UUID_V4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


@dataclass(frozen=True, slots=True)
class CacheActivationIdentity:
    """Immutable identity captured while resolving the active cache symlink."""

    environment: str
    deployment_id: str
    release_id: str | None
    activation_id: str | None
    workspace_authority_connection_ref: str | None = None


@dataclass(frozen=True, slots=True)
class CacheReadLease:
    """Frozen cache-root availability observed before any protected read."""

    root_available: bool


@contextmanager
def cache_read_lease(cache_root: str | Path) -> Iterator[CacheReadLease]:
    """Prevent promotion or retention from mutating cache bytes during parse."""

    root = Path(cache_root).resolve(strict=False)
    if not root.exists():
        yield CacheReadLease(root_available=False)
        return
    with _cache_lease(root, write=False):
        yield CacheReadLease(root_available=True)


@contextmanager
def cache_write_lease(cache_root: str | Path) -> Iterator[None]:
    """Serialize lightweight cache sync with read-only Airflow consumers."""

    root = Path(cache_root).resolve(strict=False)
    try:
        ensure_shared_directory(root)
    except OSError as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_WRITE_LEASE_FAILED",
            "cache root could not be created for the write lease",
            path=root.as_posix(),
        ) from exc
    with _cache_lease(root, write=True):
        yield


@contextmanager
def _cache_lease(root: Path, *, write: bool) -> Iterator[None]:
    lock_path = root / ".promotion.lock"
    code = "DPONE_CACHE_WRITE_LEASE_FAILED" if write else "DPONE_CACHE_READ_LEASE_FAILED"
    lease_kind = "write" if write else "read"
    root_descriptor: int | None = None
    lock_descriptor: int | None = None
    file_lock: object | None = None
    try:
        file_lock = import_module("fcntl")
        root_descriptor = os.open(root, _DIRECTORY_LOCK_FLAGS)
        file_lock.flock(root_descriptor, file_lock.LOCK_EX if write else file_lock.LOCK_SH)
        _require_same_open_path(root, root_descriptor, directory=True)
        lock_descriptor = os.open(lock_path, _WRITE_LOCK_FLAGS if write else _LOCK_FLAGS, _LOCK_MODE)
        if write:
            ensure_control_file_mode(lock_descriptor, _LOCK_MODE)
        if not stat.S_ISREG(os.fstat(lock_descriptor).st_mode):
            raise OSError("cache lock is not a regular file")
        file_lock.flock(lock_descriptor, file_lock.LOCK_EX if write else file_lock.LOCK_SH)
        _require_same_open_path(lock_path, lock_descriptor, directory=False)
    except (ImportError, OSError) as exc:
        if file_lock is not None and lock_descriptor is not None:
            _release_lease(file_lock, lock_descriptor, lock_path=lock_path, lease_kind=lease_kind)
        elif lock_descriptor is not None:
            os.close(lock_descriptor)
        if file_lock is not None and root_descriptor is not None:
            _release_lease(file_lock, root_descriptor, lock_path=root, lease_kind=f"{lease_kind}-root")
        elif root_descriptor is not None:
            os.close(root_descriptor)
        raise AirflowDeploymentIndexError(
            code,
            f"cache {lease_kind} lease could not be acquired safely",
            path=lock_path.as_posix(),
        ) from exc
    assert file_lock is not None and lock_descriptor is not None and root_descriptor is not None
    try:
        yield
    finally:
        _release_lease(file_lock, lock_descriptor, lock_path=lock_path, lease_kind=lease_kind)
        _release_lease(file_lock, root_descriptor, lock_path=root, lease_kind=f"{lease_kind}-root")


def _require_same_open_path(path: Path, descriptor: int, *, directory: bool) -> None:
    opened = os.fstat(descriptor)
    current = os.stat(path, follow_symlinks=False)
    expected_type = stat.S_ISDIR if directory else stat.S_ISREG
    if (
        opened.st_nlink <= 0
        or not expected_type(opened.st_mode)
        or not expected_type(current.st_mode)
        or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
    ):
        raise OSError("cache lease path changed while acquiring its lock")


def _release_lease(file_lock: object, descriptor: int, *, lock_path: Path, lease_kind: str) -> None:
    """Release best-effort without masking a body error or reporting false rollback."""

    failures: list[str] = []
    try:
        file_lock.flock(descriptor, file_lock.LOCK_UN)  # type: ignore[attr-defined]
    except OSError as exc:
        failures.append(f"unlock:{exc.__class__.__name__}")
    try:
        os.close(descriptor)
    except OSError as exc:
        failures.append(f"close:{exc.__class__.__name__}")
    if failures:
        emit_nonfatal_runtime_warning(
            f"DPONE_CACHE_LEASE_RELEASE_WARNING:{lease_kind}:{','.join(failures)}:{lock_path}",
            stacklevel=2,
        )


def pinned_cache_parts(
    root_descriptor: int,
    parts: tuple[str, ...],
    *,
    allow_current_pointer: bool,
    path: Path,
) -> tuple[tuple[str, ...], CacheActivationIdentity | None]:
    """Resolve one canonical current symlink and capture its occurrence token."""

    if parts[0] != "current":
        return parts, None
    if not allow_current_pointer:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_UNPINNED_REFERENCE",
            "cache artifacts must not resolve through the mutable current pointer",
            path=path.as_posix(),
        )
    before = _current_metadata(root_descriptor, path=path)
    if not stat.S_ISLNK(before.st_mode):
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_REF_UNSAFE",
            "cache current pointer must be a relative symlink",
            path=path.as_posix(),
        )
    try:
        target_text = os.readlink("current", dir_fd=root_descriptor)
        target = PurePosixPath(target_text)
        after = os.stat("current", dir_fd=root_descriptor, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_READ_FAILED",
            "cache current pointer changed while it was read",
            path=path.as_posix(),
        ) from exc
    except OSError as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_READ_FAILED",
            "cache current pointer could not be read safely",
            path=path.as_posix(),
        ) from exc
    if _file_identity(before) != _file_identity(after):
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_READ_FAILED",
            "cache current pointer changed while it was read",
            path=path.as_posix(),
        )
    if not _canonical_target(target, target_text=target_text):
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_REF_UNSAFE",
            "cache current pointer must target activations/<environment>/sha256-<64 lowercase hex>",
            path=path.as_posix(),
        )
    environment = target.parts[1]
    deployment_id = "sha256:" + target.parts[2].removeprefix(_DEPLOYMENT_DIRECTORY_PREFIX)
    identity = _read_activation_identity(
        root_descriptor,
        environment=environment,
        deployment_id=deployment_id,
        path=path,
    )
    return (*target.parts, *parts[1:]), identity


def _current_metadata(root_descriptor: int, *, path: Path) -> os.stat_result:
    try:
        return os.stat("current", dir_fd=root_descriptor, follow_symlinks=False)
    except FileNotFoundError as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_MISSING",
            "cache current pointer is missing",
            path=path.as_posix(),
        ) from exc
    except OSError as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_READ_FAILED",
            "cache current pointer could not be inspected safely",
            path=path.as_posix(),
        ) from exc


def _canonical_target(target: PurePosixPath, *, target_text: str) -> bool:
    if (
        target.is_absolute()
        or len(target.parts) != 3
        or any(part in {"", ".", ".."} for part in target.parts)
        or target.parts[0] != "activations"
        or not target.parts[1]
        or target_text != target.as_posix()
    ):
        return False
    value = target.parts[2]
    hexadecimal = value.removeprefix(_DEPLOYMENT_DIRECTORY_PREFIX)
    return (
        value.startswith(_DEPLOYMENT_DIRECTORY_PREFIX)
        and len(hexadecimal) == _DEPLOYMENT_HEX_LENGTH
        and all(character in "0123456789abcdef" for character in hexadecimal)
    )


def _file_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
    )


def _read_activation_identity(
    root_descriptor: int,
    *,
    environment: str,
    deployment_id: str,
    path: Path,
) -> CacheActivationIdentity:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open("current-pointer.json", flags, dir_fd=root_descriptor)
        with os.fdopen(descriptor, "rb") as handle:
            payload_bytes = handle.read(_MAX_POINTER_BYTES + 1)
    except FileNotFoundError:
        # Legacy v1 caches predate current-pointer.json. Preserve their
        # symlink-derived deployment identity; strict v2 validation rejects
        # the missing occurrence token at the deployment-index boundary.
        return CacheActivationIdentity(
            environment=environment,
            deployment_id=deployment_id,
            release_id=None,
            activation_id=None,
            workspace_authority_connection_ref=None,
        )
    except OSError as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_CURRENT_POINTER_INVALID",
            "cache current pointer metadata could not be read safely",
            path=path.as_posix(),
        ) from exc
    if len(payload_bytes) > _MAX_POINTER_BYTES:
        raise AirflowDeploymentIndexError(
            "DPONE_CURRENT_POINTER_INVALID",
            "cache current pointer metadata exceeds 64 KiB",
            path=path.as_posix(),
        )
    try:
        payload = json.loads(payload_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_CURRENT_POINTER_INVALID",
            "cache current pointer metadata is invalid JSON",
            path=path.as_posix(),
        ) from exc
    if not isinstance(payload, dict):
        raise AirflowDeploymentIndexError(
            "DPONE_CURRENT_POINTER_INVALID",
            "cache current pointer metadata does not match the active deployment",
            path=path.as_posix(),
        )
    release_id = payload.get("release_id")
    activation_id = payload.get("activation_id")
    workspace_ref = payload.get("workspace_authority_connection_ref")
    if (
        payload.get("schema") != "dpone.current-pointer.v1"
        or payload.get("environment") != environment
        or payload.get("deployment_id") != deployment_id
        or not _sha256(release_id)
        or not _nonempty_text(payload.get("promoted_by"))
        or not _aware_datetime(payload.get("promoted_at"))
        or (
            activation_id is not None
            and (not isinstance(activation_id, str) or _UUID_V4.fullmatch(activation_id) is None)
        )
        or (
            workspace_ref is not None
            and (not isinstance(workspace_ref, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,127}", workspace_ref) is None)
        )
    ):
        raise AirflowDeploymentIndexError(
            "DPONE_CURRENT_POINTER_INVALID",
            "cache current pointer metadata does not satisfy the canonical contract",
            path=path.as_posix(),
        )
    return CacheActivationIdentity(
        environment=environment,
        deployment_id=deployment_id,
        release_id=str(release_id),
        activation_id=activation_id,
        workspace_authority_connection_ref=workspace_ref,
    )


def _sha256(value: object) -> bool:
    return bool(isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value))


def _nonempty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value)


def _aware_datetime(value: object) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


__all__ = [
    "CacheActivationIdentity",
    "CacheReadLease",
    "cache_read_lease",
    "cache_write_lease",
    "pinned_cache_parts",
]
