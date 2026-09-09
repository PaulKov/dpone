"""Crash-aware, descriptor-confined Airflow deployment projection I/O."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.readiness.airflow_deployment_projection_errors import (
        ProjectionPublicationState,
        ProjectionWriteOperation,
    )


import os
import re
import secrets
from collections.abc import Mapping
from pathlib import Path

from dpone.manifest.confined_files import ConfinedFileError, read_confined_leaf
from dpone.readiness.airflow_deployment_projection_errors import AirflowDeploymentProjectionError
from dpone.readiness.confined_projection_directory import (
    close_descriptor,
    open_child_directory,
    open_or_create_confined_directory,
    validate_child_name,
)

_PROJECTION_FILE_ORDER = (
    "deployment.json",
    "airflow-index.json",
    "binding-set.json",
    "connection-registry.ref",
    "credential-runtime.ref",
    "_SUCCESS",
)
_SEMANTIC_REFRESH_PROJECTION_NAME = re.compile(r"^semantic-refresh-[0-9a-f]{64}\.dag-projection\.json$")
_MAX_SEMANTIC_REFRESH_PROJECTIONS = 64
_CREATE_ATTEMPTS = 32


def publish_immutable_projection(
    *,
    root: Path,
    parent: Path,
    final_name: str,
    expected_files: Mapping[str, bytes],
) -> Path:
    """Create or compare one complete projection below a stable project root.

    All directory traversal, staging writes, comparison, cleanup and rename
    operations are anchored to no-follow directory descriptors. A mutable path
    component therefore cannot redirect publication outside the configured
    cache root after the service has been constructed.
    """

    files = _ordered_projection_files(expected_files)
    final_dir = parent / final_name
    try:
        validate_child_name(final_name)
        with open_or_create_confined_directory(root, parent) as parent_fd:
            return _publish_in_parent(
                parent_fd,
                final_dir=final_dir,
                final_name=final_name,
                files=files,
            )
    except AirflowDeploymentProjectionError:
        raise
    except (OSError, ValueError):
        raise _write_error(
            final_dir,
            operation="create_parent",
            message="deployment projection parent could not be prepared safely",
        ) from None


def _publish_in_parent(
    parent_fd: int,
    *,
    final_dir: Path,
    final_name: str,
    files: tuple[tuple[str, bytes], ...],
) -> Path:
    if _entry_exists(parent_fd, final_name, final_dir=final_dir):
        return _existing_projection(parent_fd, final_dir, final_name, files)

    stage_name = _create_stage_directory(parent_fd, final_name, final_dir=final_dir)
    stage_fd = open_child_directory(parent_fd, stage_name)
    result: Path | None = None
    primary: AirflowDeploymentProjectionError | None = None
    try:
        _write_stage(stage_fd, final_dir=final_dir, files=files)
        try:
            _rename_stage(parent_fd, stage_name, final_name)
        except OSError:
            if _entry_exists(parent_fd, final_name, final_dir=final_dir):
                result = _existing_projection(parent_fd, final_dir, final_name, files)
            else:
                raise _write_error(
                    final_dir,
                    operation="rename",
                    publication_state="unknown",
                    message="deployment projection could not be published atomically",
                ) from None
        else:
            _fsync_directory(
                parent_fd,
                final_dir=final_dir,
                publication_state="published",
            )
            result = final_dir
    except AirflowDeploymentProjectionError as exc:
        primary = exc
    finally:
        close_descriptor(stage_fd)

    cleanup_failed = _cleanup_stage(parent_fd, stage_name, files)
    if cleanup_failed:
        if primary is None:
            primary = AirflowDeploymentProjectionError(
                "DPONE_DEPLOYMENT_CLEANUP_FAILED",
                "temporary deployment projection cleanup is required",
                path=final_dir.as_posix(),
                operation="cleanup",
                publication_state="not_published" if result is None else "published",
            )
        primary.record_temporary_cleanup_failure()
    if primary is not None:
        raise primary from None
    if result is None:  # pragma: no cover - defensive state invariant
        raise _write_error(
            final_dir,
            operation="rename",
            publication_state="unknown",
            message="deployment projection publication outcome is unknown",
        )
    return result


def _ordered_projection_files(expected_files: Mapping[str, bytes]) -> tuple[tuple[str, bytes], ...]:
    expected_names = set(expected_files)
    base_names = set(_PROJECTION_FILE_ORDER)
    sidecar_names = expected_names - base_names
    if (
        not base_names.issubset(expected_names)
        or len(sidecar_names) > _MAX_SEMANTIC_REFRESH_PROJECTIONS
        or any(_SEMANTIC_REFRESH_PROJECTION_NAME.fullmatch(name) is None for name in sidecar_names)
    ):
        raise ValueError("projection file set is incomplete")
    order = (
        *_PROJECTION_FILE_ORDER[:-1],
        *sorted(sidecar_names),
        _PROJECTION_FILE_ORDER[-1],
    )
    return tuple((name, bytes(expected_files[name])) for name in order)


def _existing_projection(
    parent_fd: int,
    final_dir: Path,
    final_name: str,
    files: tuple[tuple[str, bytes], ...],
) -> Path:
    if _projection_matches(parent_fd, final_name, files):
        return final_dir
    raise AirflowDeploymentProjectionError(
        "DPONE_DEPLOYMENT_ALREADY_EXISTS",
        "deployment projection already exists with different content",
        path=final_dir.as_posix(),
    )


def _projection_matches(
    parent_fd: int,
    directory_name: str,
    files: tuple[tuple[str, bytes], ...],
) -> bool:
    try:
        directory_fd = open_child_directory(parent_fd, directory_name)
    except OSError:
        return False
    try:
        if set(os.listdir(directory_fd)) != {name for name, _ in files}:
            return False
        for name, expected in files:
            try:
                observed = read_confined_leaf(directory_fd, name, max_bytes=len(expected))
            except (ConfinedFileError, OSError, ValueError):
                return False
            if observed.content != expected:
                return False
        return True
    except OSError:
        return False
    finally:
        close_descriptor(directory_fd)


def _write_stage(
    stage_fd: int,
    *,
    final_dir: Path,
    files: tuple[tuple[str, bytes], ...],
) -> None:
    for name, payload in files:
        _write_durable_file(stage_fd, name, payload, final_dir=final_dir)
    _fsync_directory(
        stage_fd,
        final_dir=final_dir,
        publication_state="not_published",
    )


def _write_durable_file(
    stage_fd: int,
    name: str,
    payload: bytes,
    *,
    final_dir: Path,
) -> None:
    descriptor: int | None = None
    primary: AirflowDeploymentProjectionError | None = None
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _flag("O_CLOEXEC") | _flag("O_NOFOLLOW")
    try:
        descriptor = os.open(name, flags, 0o600, dir_fd=stage_fd)
        _write_all(descriptor, payload)
    except OSError:
        primary = _write_error(
            final_dir,
            operation="write",
            message="deployment projection bytes could not be staged",
        )
    if primary is None and descriptor is not None:
        try:
            os.fsync(descriptor)
        except OSError:
            primary = _write_error(
                final_dir,
                operation="fsync",
                message="deployment projection bytes could not be synchronized",
            )
    if descriptor is not None:
        try:
            os.close(descriptor)
        except OSError:
            if primary is None:
                primary = _write_error(
                    final_dir,
                    operation="write",
                    message="deployment projection bytes could not be closed safely",
                )
    if primary is not None:
        raise primary from None


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    written = 0
    while written < len(view):
        count = os.write(descriptor, view[written:])
        if count <= 0:
            raise OSError("short deployment projection write")
        written += count


def _fsync_directory(
    descriptor: int,
    *,
    final_dir: Path,
    publication_state: ProjectionPublicationState,
) -> None:
    try:
        os.fsync(descriptor)
    except OSError:
        raise _write_error(
            final_dir,
            operation="fsync",
            publication_state=publication_state,
            message="deployment projection directory could not be synchronized",
        ) from None


def _create_stage_directory(parent_fd: int, final_name: str, *, final_dir: Path) -> str:
    for _ in range(_CREATE_ATTEMPTS):
        stage_name = f".{final_name}.tmp.{secrets.token_hex(8)}"
        try:
            os.mkdir(stage_name, 0o700, dir_fd=parent_fd)
        except FileExistsError:
            continue
        except OSError:
            break
        return stage_name
    raise _write_error(
        final_dir,
        operation="create_staging",
        message="deployment projection staging directory could not be created",
    )


def _cleanup_stage(
    parent_fd: int,
    stage_name: str,
    files: tuple[tuple[str, bytes], ...],
) -> bool:
    try:
        stage_fd = open_child_directory(parent_fd, stage_name)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    cleanup_failed = False
    try:
        for name, _ in files:
            try:
                os.unlink(name, dir_fd=stage_fd)
            except FileNotFoundError:
                continue
            except OSError:
                cleanup_failed = True
        if os.listdir(stage_fd):
            cleanup_failed = True
    except OSError:
        cleanup_failed = True
    finally:
        close_descriptor(stage_fd)
    if cleanup_failed:
        return True
    try:
        _remove_staging_directory(parent_fd, stage_name)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return False


def _rename_stage(parent_fd: int, stage_name: str, final_name: str) -> None:
    os.rename(stage_name, final_name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)


def _remove_staging_directory(parent_fd: int, stage_name: str) -> None:
    os.rmdir(stage_name, dir_fd=parent_fd)


def _entry_exists(parent_fd: int, name: str, *, final_dir: Path) -> bool:
    try:
        os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError:
        raise _write_error(
            final_dir,
            operation="inspect",
            message="deployment projection path could not be inspected safely",
        ) from None
    return True


def _write_error(
    final_dir: Path,
    *,
    operation: ProjectionWriteOperation,
    message: str,
    publication_state: ProjectionPublicationState = "not_published",
) -> AirflowDeploymentProjectionError:
    return AirflowDeploymentProjectionError(
        "DPONE_DEPLOYMENT_WRITE_FAILED",
        message,
        path=final_dir.as_posix(),
        operation=operation,
        publication_state=publication_state,
    )


def _flag(name: str) -> int:
    return int(getattr(os, name, 0))


__all__ = ["publish_immutable_projection"]
