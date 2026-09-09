"""Sealed activation snapshots for verified Airflow deployment projections."""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    cache_error_after_mutation,
    fsync_directory,
    open_regular_file,
    remove_sealed_directory,
    require_path_without_symlinks,
)
from dpone.runtime.deployment_cache_models import ValidatedDeploymentProjection
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator

DEFAULT_MAX_ACTIVATION_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class DeploymentCacheActivation:
    path: Path
    projection: ValidatedDeploymentProjection


class DeploymentCacheActivationSnapshotter:
    """Copy a candidate into a sealed tree and validate the copied bytes."""

    def __init__(
        self,
        cache_root: Path,
        *,
        validator: DeploymentCacheProjectionValidator,
        max_snapshot_bytes: int = DEFAULT_MAX_ACTIVATION_BYTES,
    ) -> None:
        self._cache_root = cache_root
        self._validator = validator
        self._max_snapshot_bytes = max_snapshot_bytes

    def prepare(self, candidate: Path, *, environment: str) -> DeploymentCacheActivation:
        source = self._validator.validate_details(candidate, environment=environment)
        source_fingerprint = _regular_tree_fingerprint(
            candidate,
            cache_root=self._cache_root,
            max_bytes=self._max_snapshot_bytes,
        )
        activation_parent = self._cache_root / "activations" / environment
        activation_path = activation_parent / _digest_dir(source.deployment_id)
        release_path = self._cache_root / "releases" / _digest_dir(source.release_id)
        activation_existed = False
        activation_published = False
        staging = activation_parent / f".{_digest_dir(source.deployment_id)}.tmp.{uuid4().hex}"
        mutation_started = False
        try:
            mutation_started = True
            activation_parent.mkdir(parents=True, exist_ok=True)
            require_path_without_symlinks(
                activation_parent,
                root=self._cache_root,
                error_path=activation_parent,
            )
            activation_existed = activation_path.exists() or activation_path.is_symlink()
            staging.mkdir(mode=0o700)
            _copy_regular_tree(
                candidate,
                staging,
                cache_root=self._cache_root,
                max_bytes=self._max_snapshot_bytes,
            )
            _seal_tree_read_only(release_path, cache_root=self._cache_root)
            _seal_tree_read_only(staging, cache_root=self._cache_root, seal_root=False)
            staged_projection = self._validator.validate_staged_activation_details(
                staging,
                environment=environment,
                expected_deployment_id=source.deployment_id,
            )
            staged_fingerprint = _regular_tree_fingerprint(
                staging,
                cache_root=self._cache_root,
                max_bytes=self._max_snapshot_bytes,
            )
            final_source_fingerprint = _regular_tree_fingerprint(
                candidate,
                cache_root=self._cache_root,
                max_bytes=self._max_snapshot_bytes,
            )
            if source_fingerprint != staged_fingerprint or source_fingerprint != final_source_fingerprint:
                raise _snapshot_conflict("activation source changed while the snapshot was being created", candidate)
            if activation_existed:
                _seal_tree_read_only(activation_path, cache_root=self._cache_root)
                projection = self._validator.validate_activation_details(
                    activation_path,
                    environment=environment,
                )
                activation_fingerprint = _regular_tree_fingerprint(
                    activation_path,
                    cache_root=self._cache_root,
                    max_bytes=self._max_snapshot_bytes,
                )
                if activation_fingerprint != staged_fingerprint:
                    raise _snapshot_conflict(
                        "existing activation bytes differ from the newly validated candidate",
                        activation_path,
                    )
                remove_sealed_activation(staging)
                return DeploymentCacheActivation(path=activation_path, projection=projection)
            os.rename(staging, activation_path)
            activation_published = True
            activation_path.chmod(0o555, follow_symlinks=False)
            fsync_directory(activation_path)
            fsync_directory(activation_parent)
            projection = self._validator.validate_activation_details(
                activation_path,
                environment=environment,
            )
            if projection != staged_projection:
                raise _snapshot_conflict(
                    "published activation differs from its validated staging tree", activation_path
                )
        except BaseException as exc:
            cleanup_failed_paths = _cleanup_failed_snapshot(
                staging=staging,
                activation_path=activation_path,
                remove_published=activation_published and not activation_existed,
            )
            if isinstance(exc, DeploymentCacheError):
                error = cache_error_after_mutation(exc) if mutation_started else exc
            elif isinstance(exc, OSError):
                error = cache_error_after_mutation(
                    _snapshot_error("activation snapshot could not be published durably", activation_path)
                )
            else:
                raise
            if cleanup_failed_paths:
                error = _with_cleanup_failures(error, cleanup_failed_paths)
            raise error from exc
        return DeploymentCacheActivation(path=activation_path, projection=projection)


def remove_sealed_activation(path: Path) -> None:
    """Make one non-current activation removable, then delete it."""

    remove_sealed_directory(path)


def _cleanup_failed_snapshot(
    *,
    staging: Path,
    activation_path: Path,
    remove_published: bool,
) -> list[str]:
    failures: list[str] = []
    cleanup_targets = [("staging", staging)]
    if remove_published:
        cleanup_targets.append(("activation", activation_path))
    for label, path in cleanup_targets:
        if not (path.exists() or path.is_symlink()):
            continue
        try:
            remove_sealed_activation(path)
        except OSError:
            failures.append(label)
    return failures


def _with_cleanup_failures(error: DeploymentCacheError, failed_paths: list[str]) -> DeploymentCacheError:
    details = dict(error.details)
    details.update(
        cleanup_failed_paths=failed_paths,
        state_may_have_changed=True,
        recovery_required=True,
    )
    return DeploymentCacheError(error.code, str(error), path=error.path, details=details)


def _copy_regular_tree(source: Path, target: Path, *, cache_root: Path, max_bytes: int) -> None:
    total_bytes = 0
    for root, directories, files in os.walk(
        source,
        topdown=True,
        onerror=_raise_walk_error,
        followlinks=False,
    ):
        root_path = Path(root)
        require_path_without_symlinks(root_path, root=cache_root, error_path=root_path)
        directories.sort()
        files.sort()
        relative_root = root_path.relative_to(source)
        for name in directories:
            source_dir = root_path / name
            if stat.S_ISLNK(source_dir.lstat().st_mode):
                raise _snapshot_error("activation source contains a symlink", source_dir)
            (target / relative_root / name).mkdir(mode=0o700, exist_ok=False)
        for name in files:
            source_file = root_path / name
            payload = _read_regular_bytes(
                source_file,
                root=cache_root,
                max_bytes=max_bytes - total_bytes,
            )
            total_bytes += len(payload)
            if total_bytes > max_bytes:
                raise _snapshot_error("activation snapshot exceeds the configured size limit", source_file)
            _write_new_file(target / relative_root / name, payload)


def _read_regular_bytes(path: Path, *, root: Path, max_bytes: int) -> bytes:
    try:
        descriptor = open_regular_file(
            path,
            missing_code="DPONE_DEPLOYMENT_ACTIVATION_FAILED",
            invalid_code="DPONE_DEPLOYMENT_ACTIVATION_FAILED",
            label="activation source file",
            root=root,
        )
    except DeploymentCacheError as exc:
        raise _snapshot_error("activation source file could not be opened safely", path) from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise _snapshot_error("activation source entry must be a regular file", path)
        if max_bytes < 0 or metadata.st_size > max_bytes:
            raise _snapshot_error("activation snapshot exceeds the configured size limit", path)
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            payload = handle.read(max_bytes + 1)
            if len(payload) > max_bytes:
                raise _snapshot_error("activation snapshot exceeds the configured size limit", path)
            return payload
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _regular_tree_fingerprint(path: Path, *, cache_root: Path, max_bytes: int) -> str:
    digest = hashlib.sha256()
    total_bytes = 0
    for root, directories, files in os.walk(
        path,
        topdown=True,
        onerror=_raise_walk_error,
        followlinks=False,
    ):
        root_path = Path(root)
        require_path_without_symlinks(root_path, root=cache_root, error_path=root_path)
        directories.sort()
        files.sort()
        relative_root = root_path.relative_to(path)
        for name in directories:
            child = root_path / name
            if stat.S_ISLNK(child.lstat().st_mode):
                raise _snapshot_error("activation tree contains a symlink", child)
            _update_tree_entry(digest, b"D", (relative_root / name).as_posix(), b"")
        for name in files:
            child = root_path / name
            payload = _read_regular_bytes(
                child,
                root=cache_root,
                max_bytes=max_bytes - total_bytes,
            )
            total_bytes += len(payload)
            if total_bytes > max_bytes:
                raise _snapshot_error("activation snapshot exceeds the configured size limit", child)
            _update_tree_entry(digest, b"F", (relative_root / name).as_posix(), payload)
    return "sha256:" + digest.hexdigest()


def _update_tree_entry(digest: Any, kind: bytes, relative_path: str, payload: bytes) -> None:
    encoded_path = relative_path.encode("utf-8")
    digest.update(kind)
    digest.update(len(encoded_path).to_bytes(8, "big"))
    digest.update(encoded_path)
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)


def _raise_walk_error(exc: OSError) -> None:
    path = Path(exc.filename) if exc.filename else Path("<activation-tree>")
    raise _snapshot_error("activation tree could not be read completely", path) from exc


def _write_new_file(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _seal_tree_read_only(path: Path, *, cache_root: Path, seal_root: bool = True) -> None:
    require_path_without_symlinks(path, root=cache_root, error_path=path)
    if not path.is_dir():
        raise _snapshot_error("immutable cache tree is missing", path)
    for root, directories, files in os.walk(path, topdown=False, followlinks=False):
        root_path = Path(root)
        for name in files:
            child = root_path / name
            if stat.S_ISLNK(child.lstat().st_mode):
                raise _snapshot_error("immutable cache tree contains a symlink", child)
            descriptor = os.open(child, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                os.fchmod(descriptor, 0o444)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        for name in directories:
            child = root_path / name
            if stat.S_ISLNK(child.lstat().st_mode):
                raise _snapshot_error("immutable cache tree contains a symlink", child)
            child.chmod(0o555, follow_symlinks=False)
            fsync_directory(child)
        if seal_root or root_path != path:
            root_path.chmod(0o555, follow_symlinks=False)
        fsync_directory(root_path)


def copy_regular_activation_tree(source: Path, target: Path, *, cache_root: Path, max_bytes: int) -> None:
    """Copy a confined activation tree for a separately validated restore."""

    _copy_regular_tree(source, target, cache_root=cache_root, max_bytes=max_bytes)


def activation_tree_fingerprint(path: Path, *, cache_root: Path, max_bytes: int) -> str:
    """Hash one complete activation tree with path and entry-type framing."""

    return _regular_tree_fingerprint(path, cache_root=cache_root, max_bytes=max_bytes)


def seal_activation_tree(path: Path, *, cache_root: Path) -> None:
    """Seal every restore-staging entry before its atomic publication."""

    _seal_tree_read_only(path, cache_root=cache_root)


def _snapshot_error(message: str, path: Path) -> DeploymentCacheError:
    return DeploymentCacheError("DPONE_DEPLOYMENT_ACTIVATION_FAILED", message, path=path.as_posix())


def _snapshot_conflict(message: str, path: Path) -> DeploymentCacheError:
    return DeploymentCacheError("DPONE_DEPLOYMENT_ACTIVATION_CONFLICT", message, path=path.as_posix())


def _digest_dir(value: str) -> str:
    return value.replace(":", "-", 1)


__all__ = [
    "DEFAULT_MAX_ACTIVATION_BYTES",
    "DeploymentCacheActivation",
    "DeploymentCacheActivationSnapshotter",
    "activation_tree_fingerprint",
    "copy_regular_activation_tree",
    "remove_sealed_activation",
    "seal_activation_tree",
]
