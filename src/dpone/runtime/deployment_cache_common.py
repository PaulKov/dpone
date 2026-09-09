"""Shared helpers for local Airflow deployment cache files."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from threading import local
from typing import Any

from dpone.adapters.airflow_cache_layout import (
    AirflowCacheLayoutFailure,
    assert_exact_cache_layout,
    ensure_exact_cache_layout,
    normalize_shared_control_file,
    shared_control_mode,
)
from dpone.adapters.deployment_cache_files import (
    DeploymentCacheError,
    durable_fsync_directory,
    open_regular_file,
    read_regular_json_object,
    require_path_without_symlinks,
    resolve_mount_projected_regular_file,
    resolve_relative_current_symlink,
)
from dpone.adapters.deployment_cache_files import (
    atomic_write_json as _atomic_write_json,
)

_PROMOTION_LOCK_STATE = local()
_PROMOTION_LOCK_MODE = shared_control_mode()
_SHA256_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


def is_sha256_digest(value: object) -> bool:
    """Return whether a cache coordinate is one canonical SHA-256 digest."""

    return isinstance(value, str) and _SHA256_DIGEST.fullmatch(value) is not None


@dataclass(frozen=True, slots=True)
class RegularFileIdentity:
    """Immutable byte identity captured from one no-follow descriptor."""

    size_bytes: int
    sha256: str


def regular_file_identity(
    path: Path,
    *,
    root: Path,
    missing_code: str,
    invalid_code: str,
    label: str,
) -> RegularFileIdentity:
    """Hash one confined regular file without reopening it by mutable path."""

    descriptor = open_regular_file(
        path,
        missing_code=missing_code,
        invalid_code=invalid_code,
        label=label,
        root=root,
    )
    try:
        size_bytes = os.fstat(descriptor).st_size
        digest = hashlib.sha256()
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    finally:
        os.close(descriptor)
    return RegularFileIdentity(
        size_bytes=size_bytes,
        sha256="sha256:" + digest.hexdigest(),
    )


def read_json_object(path: Path, *, code: str) -> dict[str, Any]:
    if not path.exists():
        raise DeploymentCacheError(code, "required deployment cache file is missing", path=path.as_posix())
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise DeploymentCacheError("DPONE_DEPLOYMENT_CACHE_INVALID", "deployment cache file must be a JSON object")
    return payload


@contextmanager
def promotion_lock(cache_root: Path) -> Iterator[None]:
    """Serialize cache promotion/recovery across processes under one root."""

    with _exclusive_cache_control_lock(
        cache_root,
        lock_name=".promotion.lock",
        operation="promotion",
        error_prefix="DPONE_CACHE_PROMOTION_LOCK",
    ):
        yield


@contextmanager
def reconcile_lock(cache_root: Path) -> Iterator[None]:
    """Serialize remote desired-state cycles without blocking DAG parse leases."""

    with _exclusive_cache_control_lock(
        cache_root,
        lock_name=".reconcile.lock",
        operation="desired-state reconcile",
        error_prefix="DPONE_CACHE_RECONCILE_LOCK",
    ):
        yield


@contextmanager
def _exclusive_cache_control_lock(
    cache_root: Path,
    *,
    lock_name: str,
    operation: str,
    error_prefix: str,
) -> Iterator[None]:
    """Acquire one reentrant process lock in the exact-cache control directory."""

    try:
        assert_exact_cache_layout(cache_root)
    except AirflowCacheLayoutFailure as exc:
        raise DeploymentCacheError(exc.code, exc.message, path=exc.path) from exc
    cache_root = cache_root.resolve(strict=False)
    lock_key = f"{lock_name}:{cache_root.as_posix()}"
    held_roots = _held_promotion_lock_roots()
    if lock_key in held_roots:
        yield
        return
    lock_path = cache_root / lock_name
    try:
        cache_root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise DeploymentCacheError(
            f"{error_prefix}_FAILED",
            f"deployment cache root could not be created for {operation}",
            path=cache_root.as_posix(),
        ) from exc
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, _PROMOTION_LOCK_MODE)
    except OSError as exc:
        raise DeploymentCacheError(
            f"{error_prefix}_FAILED",
            f"deployment cache {operation} lock could not be opened safely",
            path=lock_path.as_posix(),
        ) from exc
    try:
        normalize_shared_control_file(descriptor)
    except OSError as exc:
        os.close(descriptor)
        raise DeploymentCacheError(
            f"{error_prefix}_FAILED",
            f"deployment cache {operation} lock permissions could not be normalized",
            path=lock_path.as_posix(),
        ) from exc
    locked = False
    root_descriptor: int | None = None
    root_locked = False
    try:
        file_lock = import_module("fcntl")
    except ImportError as exc:
        os.close(descriptor)
        raise DeploymentCacheError(
            f"{error_prefix}_UNSUPPORTED",
            f"deployment cache {operation} requires a POSIX file-lock implementation",
            path=lock_path.as_posix(),
        ) from exc
    try:
        if lock_name == ".promotion.lock":
            root_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0)
            try:
                root_descriptor = os.open(cache_root, root_flags)
                file_lock.flock(root_descriptor, file_lock.LOCK_EX)
                root_locked = True
                _require_locked_path_identity(
                    cache_root,
                    root_descriptor,
                    directory=True,
                    error_prefix=error_prefix,
                    operation=operation,
                )
            except OSError as exc:
                raise DeploymentCacheError(
                    f"{error_prefix}_FAILED",
                    f"deployment cache root lock could not be acquired for {operation}",
                    path=cache_root.as_posix(),
                ) from exc
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise DeploymentCacheError(
                f"{error_prefix}_FAILED",
                f"deployment cache {operation} lock must be a regular file",
                path=lock_path.as_posix(),
            )
        try:
            file_lock.flock(descriptor, file_lock.LOCK_EX)
        except OSError as exc:
            raise DeploymentCacheError(
                f"{error_prefix}_FAILED",
                f"deployment cache {operation} lock could not be acquired",
                path=lock_path.as_posix(),
            ) from exc
        locked = True
        _require_locked_path_identity(
            lock_path,
            descriptor,
            directory=False,
            error_prefix=error_prefix,
            operation=operation,
        )
        try:
            ensure_exact_cache_layout(cache_root)
        except AirflowCacheLayoutFailure as exc:
            raise DeploymentCacheError(exc.code, exc.message, path=exc.path) from exc
        held_roots.add(lock_key)
        yield
    finally:
        held_roots.discard(lock_key)
        _release_promotion_lock(
            file_lock,
            descriptor,
            locked=locked,
            lock_path=lock_path,
            warning_prefix=error_prefix,
        )
        if root_descriptor is not None:
            _release_promotion_lock(
                file_lock,
                root_descriptor,
                locked=root_locked,
                lock_path=cache_root,
                warning_prefix=error_prefix,
            )


def _require_locked_path_identity(
    path: Path,
    descriptor: int,
    *,
    directory: bool,
    error_prefix: str,
    operation: str,
) -> None:
    opened = os.fstat(descriptor)
    current = os.stat(path, follow_symlinks=False)
    expected_type = stat.S_ISDIR if directory else stat.S_ISREG
    if (
        opened.st_nlink <= 0
        or not expected_type(opened.st_mode)
        or not expected_type(current.st_mode)
        or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
    ):
        raise DeploymentCacheError(
            f"{error_prefix}_FAILED",
            f"deployment cache {operation} lock path changed while acquiring authority",
            path=path.as_posix(),
        )


def _release_promotion_lock(
    file_lock: Any,
    descriptor: int,
    *,
    locked: bool,
    lock_path: Path,
    warning_prefix: str = "DPONE_CACHE_PROMOTION_LOCK",
) -> None:
    """Release best-effort without hiding a committed mutation or primary exception."""

    failures: list[str] = []
    if locked:
        try:
            file_lock.flock(descriptor, file_lock.LOCK_UN)
        except OSError as exc:
            failures.append(f"unlock:{exc.__class__.__name__}")
    try:
        os.close(descriptor)
    except OSError as exc:
        failures.append(f"close:{exc.__class__.__name__}")
    if failures:
        warnings.warn(
            f"{warning_prefix}_RELEASE_WARNING:{','.join(failures)}:{lock_path}",
            RuntimeWarning,
            stacklevel=2,
        )


def _held_promotion_lock_roots() -> set[str]:
    roots = getattr(_PROMOTION_LOCK_STATE, "roots", None)
    if roots is None:
        roots = set()
        _PROMOTION_LOCK_STATE.roots = roots
    return roots


def fsync_directory(path: Path) -> None:
    """Expose directory durability for the materializer commit point."""

    _fsync_directory(path)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write through the shared adapter while preserving the runtime durability seam."""

    _atomic_write_json(path, payload, fsync_directory=_fsync_directory)


def remove_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


def remove_sealed_directory(path: Path) -> None:
    """Make one sealed managed tree owner-writable and remove it without following links."""

    if path.is_symlink():
        path.unlink()
        return
    if not path.exists():
        return
    for root, directories, files in os.walk(path, topdown=False, followlinks=False):
        root_path = Path(root)
        for name in files:
            (root_path / name).chmod(0o600, follow_symlinks=False)
        for name in directories:
            (root_path / name).chmod(0o700, follow_symlinks=False)
        root_path.chmod(0o700, follow_symlinks=False)
    remove_path(path)


def deployment_id_from_dir(path: Path) -> str:
    return path.name.replace("sha256-", "sha256:", 1)


def cache_error_after_mutation(exc: DeploymentCacheError) -> DeploymentCacheError:
    """Preserve one cache error while marking its operation as mutation-uncertain."""

    details = dict(exc.details)
    details["state_may_have_changed"] = True
    details.setdefault("recovery_required", True)
    return DeploymentCacheError(
        exc.code,
        str(exc),
        path=exc.path,
        details=details,
    )


def _fsync_directory(path: Path) -> None:
    durable_fsync_directory(path)


__all__ = [
    "DeploymentCacheError",
    "RegularFileIdentity",
    "atomic_write_json",
    "cache_error_after_mutation",
    "deployment_id_from_dir",
    "fsync_directory",
    "open_regular_file",
    "promotion_lock",
    "reconcile_lock",
    "regular_file_identity",
    "read_json_object",
    "read_regular_json_object",
    "resolve_mount_projected_regular_file",
    "resolve_relative_current_symlink",
    "require_path_without_symlinks",
    "remove_path",
    "remove_sealed_directory",
]
