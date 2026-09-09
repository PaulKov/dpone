"""POSIX adapter for durable workload-index baseline promotion."""

from __future__ import annotations

import os
import secrets
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

from dpone.contracts.workload_index import MAX_WORKLOAD_INDEX_BYTES
from dpone.ports.workload_index_baseline_store import (
    WorkloadIndexAtomicReplace,
    WorkloadIndexAtomicReplaceError,
    WorkloadIndexBaselineStoreError,
    WorkloadIndexRootIdentity,
)

_DIRECTORY_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)


class ConfinedWorkloadIndexBaselineStore:
    """Create-only or digest-CAS one project-identity-confined baseline."""

    def __init__(
        self,
        root_identity: WorkloadIndexRootIdentity,
        *,
        atomic_replace: WorkloadIndexAtomicReplace,
    ) -> None:
        self._root_identity = root_identity
        self._atomic_replace = atomic_replace

    def promote(
        self,
        relative_path: str,
        *,
        desired: bytes,
        expected_sha256: str | None,
    ) -> None:
        target = Path(PurePosixPath(relative_path))
        if expected_sha256 is None:
            self._create(target, desired=desired)
            return
        self._replace(target, desired=desired, expected_sha256=expected_sha256)

    def _create(self, target: Path, *, desired: bytes) -> None:
        with _open_existing_parent(self._root_identity, target) as parent_fd:
            temporary = _write_temporary(parent_fd, target.name, desired, mode=0o644)
            committed = False
            try:
                prepared = os.stat(temporary, dir_fd=parent_fd, follow_symlinks=False)
                try:
                    os.link(
                        temporary,
                        target.name,
                        src_dir_fd=parent_fd,
                        dst_dir_fd=parent_fd,
                        follow_symlinks=False,
                    )
                except FileExistsError as exc:
                    raise WorkloadIndexBaselineStoreError(
                        "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT",
                        "Workload-index baseline already exists.",
                    ) from exc
                committed = True
                installed = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
                if (
                    not stat.S_ISREG(installed.st_mode)
                    or installed.st_dev != prepared.st_dev
                    or installed.st_ino != prepared.st_ino
                ):
                    raise WorkloadIndexBaselineStoreError(
                        "DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED",
                        "Bootstrap baseline identity could not be verified after creation.",
                        recovery_path=target.as_posix(),
                    )
                os.fsync(parent_fd)
            except WorkloadIndexBaselineStoreError as exc:
                cleanup_error = _discard_temporary(parent_fd, temporary)
                if cleanup_error is not None and exc.code != "DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED":
                    cleanup_recovery_path = (target.parent / temporary).as_posix()
                    raise WorkloadIndexBaselineStoreError(
                        "DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED",
                        "Bootstrap failed and temporary cleanup requires recovery.",
                        recovery_path=cleanup_recovery_path,
                        recovery_artifacts=(cleanup_recovery_path,),
                    ) from cleanup_error
                raise
            except BaseException as exc:
                cleanup_error = _discard_temporary(parent_fd, temporary)
                code = (
                    "DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED"
                    if committed or cleanup_error is not None
                    else "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT"
                )
                failure_recovery_path: str | None = (
                    target.as_posix()
                    if committed
                    else (target.parent / temporary).as_posix()
                    if cleanup_error is not None
                    else None
                )
                raise WorkloadIndexBaselineStoreError(
                    code,
                    "Bootstrap baseline could not be created durably.",
                    recovery_path=failure_recovery_path,
                    recovery_artifacts=((failure_recovery_path,) if failure_recovery_path is not None else ()),
                ) from exc
            cleanup_error = _discard_temporary(parent_fd, temporary)
            if cleanup_error is not None:
                committed_recovery_path = (target.parent / temporary).as_posix()
                raise WorkloadIndexBaselineStoreError(
                    "DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED",
                    "Bootstrap baseline was committed, but temporary cleanup requires recovery.",
                    recovery_path=committed_recovery_path,
                    recovery_artifacts=(committed_recovery_path,),
                ) from cleanup_error

    def _replace(self, target: Path, *, desired: bytes, expected_sha256: str) -> None:
        with _open_existing_parent(self._root_identity, target) as parent_fd:
            try:
                metadata = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
            except OSError as exc:
                raise WorkloadIndexBaselineStoreError(
                    "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT",
                    "Workload-index baseline changed after approval.",
                ) from exc
            if not stat.S_ISREG(metadata.st_mode):
                raise WorkloadIndexBaselineStoreError(
                    "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT",
                    "Workload-index baseline must be a regular file.",
                )
            temporary = _write_temporary(
                parent_fd,
                target.name,
                desired,
                mode=stat.S_IMODE(metadata.st_mode),
            )
            try:
                outcome = self._atomic_replace(
                    parent_fd,
                    target.name,
                    temporary,
                    expected_sha256,
                    MAX_WORKLOAD_INDEX_BYTES,
                )
            except WorkloadIndexAtomicReplaceError as exc:
                preserve_temporary = exc.committed or exc.cleanup_required
                cleanup_error = None if preserve_temporary else _discard_temporary(parent_fd, temporary)
                recovery_artifacts = _existing_recovery_artifacts(
                    parent_fd,
                    target,
                    exc.recovery_name,
                    temporary if preserve_temporary or cleanup_error is not None else None,
                )
                code = (
                    "DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED"
                    if exc.committed or exc.cleanup_required or exc.recovery_name or cleanup_error is not None
                    else "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT"
                )
                raise WorkloadIndexBaselineStoreError(
                    code,
                    "Workload-index baseline could not be promoted with the approved state.",
                    recovery_path=recovery_artifacts[0] if recovery_artifacts else None,
                    recovery_artifacts=recovery_artifacts,
                ) from (cleanup_error or exc)
            except BaseException:
                _discard_temporary(parent_fd, temporary)
                raise
            if outcome.cleanup_required:
                recovery_artifacts = _existing_recovery_artifacts(
                    parent_fd,
                    target,
                    outcome.recovery_name,
                    temporary,
                )
                raise WorkloadIndexBaselineStoreError(
                    "DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED",
                    "Workload-index baseline was committed, but transaction cleanup requires recovery.",
                    recovery_path=recovery_artifacts[0] if recovery_artifacts else None,
                    recovery_artifacts=recovery_artifacts,
                )
            cleanup_error = _discard_temporary(parent_fd, temporary)
            if cleanup_error is not None:
                recovery_path = (target.parent / temporary).as_posix()
                raise WorkloadIndexBaselineStoreError(
                    "DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED",
                    "Workload-index baseline was committed, but temporary cleanup requires recovery.",
                    recovery_path=recovery_path,
                    recovery_artifacts=(recovery_path,),
                ) from cleanup_error


@contextmanager
def _open_existing_parent(
    root_identity: WorkloadIndexRootIdentity,
    target: Path,
) -> Iterator[int]:
    relative = target.as_posix()
    parts = PurePosixPath(relative).parts
    if not parts or "\\" in relative or ".." in parts or "." in parts:
        raise WorkloadIndexBaselineStoreError(
            "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT",
            "Workload-index baseline path is invalid.",
        )
    try:
        descriptor = os.open(root_identity.path, _DIRECTORY_FLAGS)
        if not root_identity.matches(os.fstat(descriptor)):
            raise WorkloadIndexBaselineStoreError(
                "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT",
                "Project root changed during workload-index promotion.",
            )
    except WorkloadIndexBaselineStoreError:
        raise
    except OSError as exc:
        raise WorkloadIndexBaselineStoreError(
            "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT",
            "Project root could not be opened safely.",
        ) from exc
    try:
        for part in parts[:-1]:
            child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        yield descriptor
    except WorkloadIndexBaselineStoreError:
        raise
    except OSError as exc:
        raise WorkloadIndexBaselineStoreError(
            "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT",
            "Workload-index baseline parent could not be opened safely.",
        ) from exc
    finally:
        os.close(descriptor)


def _write_temporary(parent_fd: int, name: str, content: bytes, *, mode: int) -> str:
    temporary = f".{name}.dpone-promote-{secrets.token_hex(8)}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(temporary, flags, 0o600, dir_fd=parent_fd)
    try:
        os.fchmod(descriptor, mode)
        view = memoryview(content)
        while view:
            view = view[os.write(descriptor, view) :]
        os.fsync(descriptor)
    except BaseException:
        _discard_temporary(parent_fd, temporary)
        raise
    finally:
        os.close(descriptor)
    return temporary


def _discard_temporary(parent_fd: int, name: str) -> OSError | None:
    try:
        os.unlink(name, dir_fd=parent_fd)
    except FileNotFoundError:
        return None
    except OSError as exc:
        return exc
    return None


def _existing_recovery_artifacts(
    parent_fd: int,
    target: Path,
    *names: str | None,
) -> tuple[str, ...]:
    existing: list[str] = []
    for name in dict.fromkeys(item for item in names if item is not None):
        try:
            os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            continue
        existing.append((target.parent / name).as_posix())
    return tuple(existing)


__all__ = ["ConfinedWorkloadIndexBaselineStore"]
