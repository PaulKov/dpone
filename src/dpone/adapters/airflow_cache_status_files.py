"""POSIX adapter for bounded Airflow cache-status publication."""

from __future__ import annotations

import os
import stat
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.adapters.deployment_cache_files import (
    AtomicJsonWriteError,
    DeploymentCacheError,
    atomic_write_json,
    durable_fsync_directory,
    encode_json_bytes,
    read_regular_json_document,
)
from dpone.contracts.posix_permissions import has_posix_access_mode
from dpone.ports.airflow_cache_status_publication import (
    AirflowCacheStatusDocument,
    AirflowCacheStatusFileTransaction,
    AirflowCacheStatusStorageError,
)


class PosixAirflowCacheStatusFileStorage:
    """Create one inode-bound publication transaction under a private root."""

    def __init__(
        self,
        *,
        write_json: Callable[[Path, dict[str, Any]], None] = atomic_write_json,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        module_importer: Callable[[str], Any] = import_module,
    ) -> None:
        self._write_json = write_json
        self._monotonic = monotonic
        self._sleep = sleep
        self._module_importer = module_importer

    @contextmanager
    def transaction(
        self,
        status_root: Path,
        *,
        lock_timeout_seconds: float,
    ) -> Iterator[AirflowCacheStatusFileTransaction]:
        root = _require_private_status_root(status_root)
        with _publication_lock(
            root,
            timeout_seconds=lock_timeout_seconds,
            monotonic=self._monotonic,
            sleep=self._sleep,
            module_importer=self._module_importer,
        ):
            yield _PosixAirflowCacheStatusTransaction(root=root, write_json=self._write_json)


class _PosixAirflowCacheStatusTransaction:
    def __init__(
        self,
        *,
        root: Path,
        write_json: Callable[[Path, dict[str, Any]], None],
    ) -> None:
        self._root = root
        self._write_json = write_json

    def read_source(self, name: str, *, max_bytes: int) -> AirflowCacheStatusDocument:
        path = self._root / name
        try:
            document = read_regular_json_document(
                path,
                missing_code="DPONE_AIRFLOW_CACHE_STATUS_SOURCE_MISSING",
                invalid_code="DPONE_AIRFLOW_CACHE_STATUS_JSON_INVALID",
                oversized_code="DPONE_AIRFLOW_CACHE_STATUS_SOURCE_OVERSIZED",
                unsafe_code="DPONE_AIRFLOW_CACHE_STATUS_SOURCE_UNSAFE",
                label="Airflow cache status source",
                root=self._root,
                max_bytes=max_bytes,
                required_owner_uid=os.geteuid(),
                forbid_group_world_permissions=True,
            )
        except DeploymentCacheError as exc:
            code = exc.code
            if code == "DPONE_AIRFLOW_CACHE_STATUS_JSON_INVALID" and (
                "regular file" in str(exc) or "symlink" in str(exc) or "runtime user" in str(exc)
            ):
                code = "DPONE_AIRFLOW_CACHE_STATUS_SOURCE_UNSAFE"
            raise AirflowCacheStatusStorageError(code) from exc
        return AirflowCacheStatusDocument(payload=document.payload, sha256=document.sha256)

    def require_safe_destination(self, name: str) -> None:
        path = self._root / name
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise AirflowCacheStatusStorageError("DPONE_AIRFLOW_CACHE_STATUS_TARGET_UNSAFE") from exc
        if path.is_symlink() or not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
            raise AirflowCacheStatusStorageError("DPONE_AIRFLOW_CACHE_STATUS_TARGET_UNSAFE")

    def commit(self, name: str, payload: Mapping[str, object]) -> str:
        materialized = dict(payload)
        encoded = encode_json_bytes(materialized)
        try:
            self._write_json(self._root / name, materialized)
        except AtomicJsonWriteError as exc:
            raise AirflowCacheStatusStorageError(
                "DPONE_AIRFLOW_CACHE_STATUS_COMMIT_UNKNOWN"
                if exc.state_may_have_changed
                else "DPONE_AIRFLOW_CACHE_STATUS_TARGET_UNSAFE",
                state_may_have_changed=exc.state_may_have_changed,
                target_sha256=_sha256(encoded) if exc.state_may_have_changed else None,
            ) from exc
        except OSError as exc:
            raise AirflowCacheStatusStorageError("DPONE_AIRFLOW_CACHE_STATUS_TARGET_UNSAFE") from exc
        return _sha256(encoded)

    def write_diagnostic(self, name: str, payload: Mapping[str, object]) -> None:
        try:
            self._write_json(self._root / name, dict(payload))
        except OSError as exc:
            raise AirflowCacheStatusStorageError("DPONE_AIRFLOW_CACHE_STATUS_FAILURE_MARKER_UNAVAILABLE") from exc

    def remove_and_sync(self, name: str) -> None:
        try:
            (self._root / name).unlink(missing_ok=True)
            durable_fsync_directory(self._root)
        except OSError as exc:
            raise AirflowCacheStatusStorageError("DPONE_AIRFLOW_CACHE_STATUS_MARKER_CLEANUP_FAILED") from exc


def _require_private_status_root(path: Path) -> Path:
    root = path.absolute()
    try:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        metadata = root.lstat()
    except OSError as exc:
        raise AirflowCacheStatusStorageError("DPONE_AIRFLOW_CACHE_STATUS_ROOT_UNSAFE") from exc
    if (
        root.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or not has_posix_access_mode(metadata.st_mode, 0o700)
        or metadata.st_uid != os.geteuid()
    ):
        raise AirflowCacheStatusStorageError("DPONE_AIRFLOW_CACHE_STATUS_ROOT_UNSAFE")
    return root


@contextmanager
def _publication_lock(
    root: Path,
    *,
    timeout_seconds: float,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
    module_importer: Callable[[str], Any],
) -> Iterator[None]:
    lock_path = root / ".cache-status-publication.lock"
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
        metadata = os.fstat(descriptor)
        current = os.stat(lock_path, follow_symlinks=False)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != os.geteuid()
            or (metadata.st_dev, metadata.st_ino) != (current.st_dev, current.st_ino)
        ):
            raise OSError("unsafe publication lock")
        file_lock = module_importer("fcntl")
        deadline = monotonic() + timeout_seconds
        while True:
            try:
                file_lock.flock(descriptor, file_lock.LOCK_EX | file_lock.LOCK_NB)
                break
            except BlockingIOError:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise OSError("publication lock acquisition timed out")
                sleep(min(0.05, remaining))
    except (ImportError, OSError) as exc:
        if "descriptor" in locals():
            os.close(descriptor)
        raise AirflowCacheStatusStorageError("DPONE_AIRFLOW_CACHE_STATUS_LOCK_UNAVAILABLE") from exc
    try:
        yield
    finally:
        release_error: OSError | None = None
        try:
            file_lock.flock(descriptor, file_lock.LOCK_UN)
        except OSError as exc:
            release_error = exc
        try:
            os.close(descriptor)
        except OSError as exc:
            release_error = release_error or exc
        if release_error is not None:
            raise AirflowCacheStatusStorageError(
                "DPONE_AIRFLOW_CACHE_STATUS_LOCK_RELEASE_UNAVAILABLE",
                state_may_have_changed=True,
            ) from release_error


def _sha256(value: bytes) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(value).hexdigest()


__all__ = ["PosixAirflowCacheStatusFileStorage"]
