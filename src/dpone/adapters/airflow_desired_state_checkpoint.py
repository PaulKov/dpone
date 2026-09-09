"""Atomic filesystem adapters for local Airflow desired-state evidence."""

from __future__ import annotations

import os
import secrets
import stat
import tempfile
from pathlib import Path

from dpone.adapters.deployment_cache_files import (
    DeploymentCacheError,
    durable_fsync_directory,
    open_regular_file,
)
from dpone.contracts.airflow_desired_state_reconcile import (
    MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES,
    DesiredStateCheckpoint,
    DesiredStateReconcileEvidence,
    DesiredStateRecoveryRecord,
)
from dpone.ports.airflow_desired_state import (
    DesiredStateReconcilePortError,
)

MAX_AIRFLOW_DESIRED_STATE_CHECKPOINT_BYTES = MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES


class AtomicAirflowDesiredStateSnapshotWriter:
    """Read and commit bounded bytes with confinement, fsync and atomic replace."""

    def __init__(
        self,
        output_path: Path,
        *,
        root: Path | None = None,
        max_bytes: int = MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES,
    ) -> None:
        if not output_path.name:
            raise ValueError("desired-state snapshot path must name a file")
        if max_bytes <= 0 or max_bytes > MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES:
            raise ValueError("desired-state snapshot max_bytes is invalid")
        self._output_path = output_path
        self._root = None if root is None else root.absolute()
        self._max_bytes = max_bytes

    def read(self, *, max_bytes: int) -> bytes | None:
        effective_max = min(max_bytes, self._max_bytes)
        if effective_max <= 0:
            raise ValueError("desired-state snapshot read bound is invalid")
        root = self._output_path.parent if self._root is None else self._root
        return _read_optional_bytes(
            self._output_path,
            root=root,
            max_bytes=effective_max,
            missing_code="DPONE_AIRFLOW_DESIRED_STATE_SNAPSHOT_NOT_FOUND",
            invalid_code="DPONE_AIRFLOW_DESIRED_STATE_SNAPSHOT_INVALID",
            label="desired-state snapshot",
        )

    def commit(self, body: bytes) -> None:
        if not isinstance(body, bytes) or not body:
            raise ValueError("desired-state snapshot body must be non-empty bytes")
        if len(body) > self._max_bytes:
            raise ValueError("desired-state snapshot exceeds its bounded size")
        if self._root is not None:
            try:
                _commit_confined(self._output_path, body=body, root=self._root)
            except DeploymentCacheError as exc:
                raise DesiredStateReconcilePortError(
                    exc.code,
                    "desired-state control path could not be committed safely",
                ) from exc
            return
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=self._output_path.parent,
            prefix=f".{self._output_path.name}.",
            suffix=".tmp",
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._output_path)
            durable_fsync_directory(self._output_path.parent)
        finally:
            temporary.unlink(missing_ok=True)


class FileDesiredStateCheckpointStore:
    """Persist one bounded canonical checkpoint with atomic replacement."""

    def __init__(self, path: Path, *, root: Path | None = None) -> None:
        if not path.name:
            raise ValueError("desired-state checkpoint path must name a file")
        self._path = path
        self._root = root

    def read(self) -> DesiredStateCheckpoint | None:
        try:
            descriptor = open_regular_file(
                self._path,
                missing_code="DPONE_AIRFLOW_DESIRED_STATE_CHECKPOINT_NOT_FOUND",
                invalid_code="DPONE_AIRFLOW_DESIRED_STATE_CHECKPOINT_INVALID",
                label="desired-state checkpoint",
                root=self._root,
            )
        except DeploymentCacheError as exc:
            if exc.code == "DPONE_AIRFLOW_DESIRED_STATE_CHECKPOINT_NOT_FOUND":
                return None
            raise ValueError("desired-state checkpoint is unsafe") from exc
        try:
            size = os.fstat(descriptor).st_size
            if size <= 0 or size > MAX_AIRFLOW_DESIRED_STATE_CHECKPOINT_BYTES:
                raise ValueError("desired-state checkpoint size is invalid")
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = -1
                body = handle.read(MAX_AIRFLOW_DESIRED_STATE_CHECKPOINT_BYTES + 1)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        if len(body) != size:
            raise ValueError("desired-state checkpoint changed while being read")
        try:
            return DesiredStateCheckpoint.from_json_bytes(body)
        except ValueError as exc:
            raise ValueError("desired-state checkpoint is invalid") from exc

    def commit(self, checkpoint: DesiredStateCheckpoint) -> None:
        body = checkpoint.to_json_bytes()
        _require_bounded_control_body(body)
        AtomicAirflowDesiredStateSnapshotWriter(
            self._path,
            root=self._root,
        ).commit(body)


class FileDesiredStateRecoveryRecordStore:
    """Persist the single pre-activation record needed for crash recovery."""

    def __init__(self, path: Path, *, root: Path) -> None:
        self._path = path
        self._root = root

    def read(self) -> DesiredStateRecoveryRecord | None:
        body = _read_optional_bytes(
            self._path,
            root=self._root,
            max_bytes=MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES,
            missing_code="DPONE_AIRFLOW_DESIRED_STATE_RECOVERY_RECORD_NOT_FOUND",
            invalid_code="DPONE_AIRFLOW_DESIRED_STATE_RECOVERY_RECORD_INVALID",
            label="desired-state recovery record",
        )
        if body is None:
            return None
        try:
            return DesiredStateRecoveryRecord.from_json_bytes(body)
        except ValueError as exc:
            raise ValueError("desired-state recovery record is invalid") from exc

    def commit(self, record: DesiredStateRecoveryRecord) -> None:
        body = record.to_json_bytes()
        _require_bounded_control_body(body)
        AtomicAirflowDesiredStateSnapshotWriter(
            self._path,
            root=self._root,
        ).commit(body)


class FileDesiredStateReconcileEvidenceStore:
    """Persist the latest cycle and immutable activation receipts under one root."""

    def __init__(self, status_root: Path, *, root: Path) -> None:
        self._status_root = status_root
        self._root = root.absolute()

    def read(self, activation_id: str) -> DesiredStateReconcileEvidence | None:
        body = _read_optional_bytes(
            self._receipt_path(activation_id),
            root=self._root,
            max_bytes=MAX_AIRFLOW_DESIRED_STATE_CHECKPOINT_BYTES,
            missing_code="DPONE_AIRFLOW_DESIRED_STATE_RECEIPT_NOT_FOUND",
            invalid_code="DPONE_AIRFLOW_DESIRED_STATE_RECEIPT_INVALID",
            label="desired-state activation receipt",
        )
        if body is None:
            return None
        try:
            evidence = DesiredStateReconcileEvidence.from_json_bytes(body)
        except ValueError as exc:
            raise ValueError("desired-state activation receipt is invalid") from exc
        if evidence.status != "activated" or evidence.activation_id != activation_id:
            raise ValueError("desired-state activation receipt identity is invalid")
        return evidence

    def commit(self, evidence: DesiredStateReconcileEvidence) -> None:
        if evidence.status != "activated":
            raise ValueError("only activation evidence has an immutable receipt")
        body = evidence.to_json_bytes()
        _require_bounded_control_body(body)
        receipt = self._receipt_path(evidence.activation_id)
        if _create_confined(receipt, body=body, root=self._root):
            return
        existing = _read_optional_bytes(
            receipt,
            root=self._root,
            max_bytes=MAX_AIRFLOW_DESIRED_STATE_CHECKPOINT_BYTES,
            missing_code="DPONE_AIRFLOW_DESIRED_STATE_RECEIPT_NOT_FOUND",
            invalid_code="DPONE_AIRFLOW_DESIRED_STATE_RECEIPT_INVALID",
            label="desired-state activation receipt",
        )
        if existing != body:
            raise ValueError("activation receipt identity resolves to different bytes")

    def commit_status(self, body: bytes) -> None:
        _require_bounded_control_body(body)
        AtomicAirflowDesiredStateSnapshotWriter(
            self._status_root / "last-reconcile-status.json",
            root=self._root,
        ).commit(body)

    def _receipt_path(self, activation_id: str) -> Path:
        return self._status_root / "reconcile-receipts" / f"{activation_id}.json"


def _read_optional_bytes(
    path: Path,
    *,
    root: Path,
    max_bytes: int,
    missing_code: str,
    invalid_code: str,
    label: str,
) -> bytes | None:
    try:
        descriptor = open_regular_file(
            path,
            missing_code=missing_code,
            invalid_code=invalid_code,
            label=label,
            root=root,
        )
    except DeploymentCacheError as exc:
        if exc.code == missing_code:
            return None
        raise DesiredStateReconcilePortError(
            exc.code,
            f"{label} is unsafe",
        ) from exc
    try:
        size = os.fstat(descriptor).st_size
        if size <= 0 or size > max_bytes:
            raise DesiredStateReconcilePortError(
                invalid_code,
                f"{label} size is invalid",
            )
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            body = handle.read(max_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(body) != size:
        raise DesiredStateReconcilePortError(
            invalid_code,
            f"{label} changed while being read",
        )
    return body


def _require_bounded_control_body(body: bytes) -> None:
    if not body or len(body) > MAX_AIRFLOW_DESIRED_STATE_CONTROL_BYTES:
        raise ValueError("desired-state control record exceeds its bounded size")


def _commit_confined(path: Path, *, body: bytes, root: Path) -> None:
    directory, name = _open_confined_parent(path, root=root)
    try:
        _replace_relative(directory, name=name, body=body)
    except OSError as exc:
        raise DeploymentCacheError(
            "DPONE_CACHE_PATH_ESCAPE",
            "desired-state control path could not be committed safely",
        ) from exc
    finally:
        os.close(directory)


def _create_confined(path: Path, *, body: bytes, root: Path) -> bool:
    directory, name = _open_confined_parent(path, root=root)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        try:
            descriptor = os.open(name, flags, 0o600, dir_fd=directory)
        except FileExistsError:
            return False
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.fsync(directory)
        return True
    except OSError as exc:
        raise DeploymentCacheError(
            "DPONE_CACHE_PATH_ESCAPE",
            "desired-state control path could not be created safely",
        ) from exc
    finally:
        os.close(directory)


def _open_confined_parent(path: Path, *, root: Path) -> tuple[int, str]:
    root = root.absolute()
    try:
        parts = path.absolute().relative_to(root).parts
    except ValueError as exc:
        raise DeploymentCacheError(
            "DPONE_CACHE_PATH_ESCAPE",
            "desired-state control path is outside the configured cache root",
        ) from exc
    if not parts:
        raise DeploymentCacheError(
            "DPONE_CACHE_PATH_ESCAPE",
            "desired-state control path must be below the configured cache root",
        )
    root.mkdir(parents=True, exist_ok=True)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory = os.open(root, flags)
    try:
        for part in parts[:-1]:
            try:
                os.mkdir(part, mode=0o700, dir_fd=directory)
            except FileExistsError:
                pass
            child = os.open(part, flags, dir_fd=directory)
            os.close(directory)
            directory = child
        return directory, parts[-1]
    except OSError as exc:
        os.close(directory)
        raise DeploymentCacheError(
            "DPONE_CACHE_PATH_ESCAPE",
            "desired-state control parent path is unsafe",
        ) from exc


def _replace_relative(directory: int, *, name: str, body: bytes) -> None:
    temporary = f".{name}.{secrets.token_hex(8)}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(temporary, flags, 0o600, dir_fd=directory)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            existing = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if not stat.S_ISREG(existing.st_mode):
                raise OSError("control target is not a regular file")
        except FileNotFoundError:
            pass
        os.replace(
            temporary,
            name,
            src_dir_fd=directory,
            dst_dir_fd=directory,
        )
        os.fsync(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary, dir_fd=directory)
        except FileNotFoundError:
            return


__all__ = [
    "AtomicAirflowDesiredStateSnapshotWriter",
    "FileDesiredStateCheckpointStore",
    "FileDesiredStateRecoveryRecordStore",
    "FileDesiredStateReconcileEvidenceStore",
    "MAX_AIRFLOW_DESIRED_STATE_CHECKPOINT_BYTES",
]
