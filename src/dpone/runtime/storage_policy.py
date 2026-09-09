from __future__ import annotations

import os
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from shutil import disk_usage
from typing import Any

from dpone.runtime.pinned_directory import PinnedDirectory, PinnedDirectoryIdentityError
from dpone.runtime.transfer_store_models import TransferStorePolicy

_BYTE_UNITS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}
_STORAGE_POLICY_KEYS = frozenset(
    {
        "profile",
        "work_dir",
        "evidence_dir",
        "checkpoint_dir",
        "debug_dir",
        "path_template",
        "create_dirs",
        "require_writable",
        "min_free_bytes",
        "cleanup",
        "transfer_store",
    }
)
MSSQL_SPOOL_DIRECTORY_RELEASE_ERROR_CODE = "mssql_spool_directory_release_failed"


@dataclass(frozen=True, slots=True)
class RuntimeStorageCleanupPolicy:
    temp_files: str = "eager"
    failed_files: str = "keep"
    completed_retention: str = "none"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> RuntimeStorageCleanupPolicy:
        raw = dict(value or {})
        return cls(
            temp_files=str(raw.get("temp_files") or "eager"),
            failed_files=str(raw.get("failed_files") or "keep"),
            completed_retention=str(raw.get("completed_retention") or "none"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RuntimeStoragePolicy:
    profile: str = "worker_local"
    work_dir: Path = field(default_factory=lambda: Path(tempfile.gettempdir()))
    evidence_dir: Path = field(default_factory=lambda: Path(".dpone/runs"))
    checkpoint_dir: Path = field(default_factory=lambda: Path(".dpone/state"))
    debug_dir: Path = field(default_factory=lambda: Path(".dpone/debug"))
    path_template: str = "{pipeline}/{run_id}/{stage}"
    create_dirs: bool = True
    require_writable: bool = True
    min_free_bytes: int = 1024**3
    cleanup: RuntimeStorageCleanupPolicy = field(default_factory=RuntimeStorageCleanupPolicy)
    transfer_store: TransferStorePolicy | None = None
    warnings: tuple[str, ...] = ()

    @classmethod
    def from_options(
        cls,
        options: Mapping[str, Any] | None,
        *,
        env: Mapping[str, str] | None = None,
    ) -> RuntimeStoragePolicy:
        raw = dict(options or {})
        runtime_storage = raw.get("runtime_storage") if isinstance(raw.get("runtime_storage"), Mapping) else {}
        return cls.from_sources(runtime={"storage": runtime_storage}, source_options=raw, env=env)

    @classmethod
    def from_sources(
        cls,
        *,
        runtime: Mapping[str, Any] | None = None,
        source_options: Mapping[str, Any] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> RuntimeStoragePolicy:
        runtime_raw = dict(runtime or {})
        source = dict(source_options or {})
        nested_storage = runtime_raw.get("storage")
        if isinstance(nested_storage, Mapping) and nested_storage:
            storage = dict(nested_storage)
        elif _STORAGE_POLICY_KEYS.intersection(runtime_raw):
            # Preserve the documented convenience API that accepts a storage
            # mapping directly instead of the complete runtime block.
            storage = runtime_raw
        elif isinstance(source.get("runtime_storage"), Mapping):
            # GitOps/runtime boundaries can retain the validated LoadConfig
            # projection while omitting the raw storage block. Unrelated
            # runtime keys must not shadow that authoritative projection.
            storage = dict(source["runtime_storage"])
        else:
            storage = {}
        env_map = dict(os.environ if env is None else env)
        warnings: list[str] = []

        work_dir = storage.get("work_dir")
        if not work_dir and source.get("partition_tmp_dir"):
            work_dir = source["partition_tmp_dir"]
            warnings.append("source.options.partition_tmp_dir is deprecated; use runtime.storage.work_dir.")
        if not work_dir and env_map.get("DPONE_EXPORT_TMP_DIR"):
            work_dir = env_map["DPONE_EXPORT_TMP_DIR"]
            warnings.append("DPONE_EXPORT_TMP_DIR is deprecated; use runtime.storage.work_dir.")
        if not work_dir:
            work_dir = tempfile.gettempdir()
            warnings.append("runtime.storage.work_dir is not configured; falling back to system temp directory.")

        return cls(
            profile=str(storage.get("profile") or "worker_local"),
            work_dir=Path(str(work_dir)),
            evidence_dir=Path(str(storage.get("evidence_dir") or ".dpone/runs")),
            checkpoint_dir=Path(str(storage.get("checkpoint_dir") or ".dpone/state")),
            debug_dir=Path(str(storage.get("debug_dir") or ".dpone/debug")),
            path_template=str(storage.get("path_template") or "{pipeline}/{run_id}/{stage}"),
            create_dirs=_bool(storage.get("create_dirs"), default=True),
            require_writable=_bool(storage.get("require_writable"), default=True),
            min_free_bytes=parse_byte_size(storage.get("min_free_bytes", "1GiB")),
            cleanup=RuntimeStorageCleanupPolicy.from_mapping(
                storage.get("cleanup") if isinstance(storage.get("cleanup"), Mapping) else None
            ),
            transfer_store=TransferStorePolicy.from_mapping(storage["transfer_store"])
            if isinstance(storage.get("transfer_store"), Mapping)
            else None,
            warnings=tuple(warnings),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "work_dir": str(self.work_dir),
            "evidence_dir": str(self.evidence_dir),
            "checkpoint_dir": str(self.checkpoint_dir),
            "debug_dir": str(self.debug_dir),
            "path_template": self.path_template,
            "create_dirs": self.create_dirs,
            "require_writable": self.require_writable,
            "min_free_bytes": self.min_free_bytes,
            "cleanup": self.cleanup.to_dict(),
            "transfer_store": self.transfer_store.to_dict() if self.transfer_store else None,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class StoragePreflightResult:
    passed: bool
    free_bytes: int
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RuntimeStorageAdmissionError(RuntimeError):
    """Stable, path-free failure raised before a local MSSQL spool is used."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class RuntimeSpoolAdmission:
    """Identity- and capacity-bound authority for one provisioned work directory."""

    work_dir: Path
    device: int
    inode: int
    free_bytes: int
    min_free_bytes: int
    max_spool_bytes: int
    directory: PinnedDirectory

    @property
    def directory_fd(self) -> int:
        """Return the descriptor used for every mutable spool operation."""

        return self.directory.descriptor

    def close(self) -> None:
        """Release the descriptor-backed admission exactly once."""

        self.directory.close()


class StoragePreflightService:
    def __init__(
        self,
        *,
        disk_usage_provider: Callable[[Path], Any] = disk_usage,
        spool_free_bytes_provider: Callable[[PinnedDirectory], int] | None = None,
    ) -> None:
        self._disk_usage_provider = disk_usage_provider
        self._spool_free_bytes_provider = spool_free_bytes_provider or (lambda directory: directory.free_bytes())

    def check(self, policy: RuntimeStoragePolicy) -> StoragePreflightResult:
        blockers: list[str] = []
        for path in (policy.work_dir, policy.evidence_dir, policy.checkpoint_dir, policy.debug_dir):
            try:
                if policy.create_dirs:
                    path.mkdir(parents=True, exist_ok=True)
                if policy.require_writable:
                    _probe_writable_directory(path.resolve(strict=True))
            except OSError:
                blockers.append("work_dir_not_writable" if path == policy.work_dir else "storage_dir_not_writable")
        free_bytes = int(self._disk_usage_provider(_nearest_existing_parent(policy.work_dir)).free)
        if free_bytes < policy.min_free_bytes:
            blockers.append("work_dir_low_space")
        return StoragePreflightResult(
            passed=not blockers,
            free_bytes=free_bytes,
            blockers=tuple(dict.fromkeys(blockers)),
            warnings=policy.warnings,
        )

    def require_spool_admission(self, policy: RuntimeStoragePolicy) -> RuntimeSpoolAdmission:
        """Provision and pin the exact filesystem used by character-mode BCP.

        The byte limit is capacity-derived rather than a source-size estimate:
        the configured free-space reserve is subtracted from the observed free
        bytes on this exact directory's filesystem.
        """

        directory: PinnedDirectory | None = None
        try:
            if policy.create_dirs:
                policy.work_dir.mkdir(parents=True, exist_ok=True)
            resolved = policy.work_dir.resolve(strict=True)
            if not resolved.is_dir():
                raise OSError("not_a_directory")
            directory = PinnedDirectory.open(resolved)
            directory.require_identity()
            directory.probe_writable()
            free_bytes = int(self._spool_free_bytes_provider(directory))
        except (OSError, RuntimeError):
            error = RuntimeStorageAdmissionError("mssql_spool_work_dir_not_writable")
            if directory is not None:
                _close_directory_preserving_error(directory, error)
            # Configured paths can contain tenant or user identifiers.  The
            # stable public error must therefore not retain pathlib/OS text in
            # its rendered exception chain.
            raise error from None
        if free_bytes < policy.min_free_bytes:
            error = RuntimeStorageAdmissionError("mssql_spool_work_dir_low_space")
            _close_directory_preserving_error(directory, error)
            raise error
        return RuntimeSpoolAdmission(
            work_dir=resolved,
            device=directory.device,
            inode=directory.inode,
            free_bytes=free_bytes,
            min_free_bytes=policy.min_free_bytes,
            max_spool_bytes=max(0, free_bytes - policy.min_free_bytes),
            directory=directory,
        )

    def refresh_spool_admission(self, admission: RuntimeSpoolAdmission) -> RuntimeSpoolAdmission:
        """Re-prove path identity and capacity immediately before row iteration."""

        try:
            admission.directory.require_identity()
        except PinnedDirectoryIdentityError:
            raise RuntimeStorageAdmissionError("mssql_spool_work_dir_identity_changed") from None
        try:
            admission.directory.probe_writable()
            free_bytes = int(self._spool_free_bytes_provider(admission.directory))
        except OSError:
            raise RuntimeStorageAdmissionError("mssql_spool_work_dir_not_writable") from None
        if free_bytes < admission.min_free_bytes:
            raise RuntimeStorageAdmissionError("mssql_spool_work_dir_low_space")
        return RuntimeSpoolAdmission(
            work_dir=admission.work_dir,
            device=admission.device,
            inode=admission.inode,
            free_bytes=free_bytes,
            min_free_bytes=admission.min_free_bytes,
            max_spool_bytes=min(
                admission.max_spool_bytes,
                max(0, free_bytes - admission.min_free_bytes),
            ),
            directory=admission.directory,
        )


def parse_byte_size(value: Any) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return max(0, value)
    text = str(value).strip()
    if not text:
        return 0
    number = ""
    unit = ""
    for char in text:
        if char.isdigit() or char == ".":
            number += char
        elif not char.isspace():
            unit += char
    if not number:
        raise ValueError(f"Invalid byte size: {value}")
    multiplier = _BYTE_UNITS.get(unit.lower() or "b")
    if multiplier is None:
        raise ValueError(f"Unsupported byte size unit: {value}")
    return int(float(number) * multiplier)


def _bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _nearest_existing_parent(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and candidate.parent != candidate:
        candidate = candidate.parent
    return candidate


def _probe_writable_directory(path: Path) -> None:
    descriptor, probe_path = tempfile.mkstemp(prefix=".dpone_write_probe_", dir=path)
    try:
        os.write(descriptor, b"ok")
    finally:
        try:
            os.close(descriptor)
        finally:
            Path(probe_path).unlink(missing_ok=True)


def _close_directory_preserving_error(
    directory: PinnedDirectory,
    primary: BaseException,
) -> None:
    try:
        directory.close()
    except OSError:
        mark_mssql_spool_directory_release_failure(primary)


def mark_mssql_spool_directory_release_failure(error: BaseException) -> None:
    """Attach path-free directory-release evidence to a primary failure."""

    try:
        setattr(
            error,
            "directory_release_error_code",
            MSSQL_SPOOL_DIRECTORY_RELEASE_ERROR_CODE,
        )
    except (AttributeError, TypeError):  # pragma: no cover - immutable foreign exception.
        pass
    notes = getattr(error, "__notes__", ())
    add_note = getattr(error, "add_note", None)
    if callable(add_note) and MSSQL_SPOOL_DIRECTORY_RELEASE_ERROR_CODE not in notes:
        add_note(MSSQL_SPOOL_DIRECTORY_RELEASE_ERROR_CODE)


__all__ = [
    "MSSQL_SPOOL_DIRECTORY_RELEASE_ERROR_CODE",
    "RuntimeStorageCleanupPolicy",
    "RuntimeStorageAdmissionError",
    "RuntimeStoragePolicy",
    "RuntimeSpoolAdmission",
    "StoragePreflightResult",
    "StoragePreflightService",
    "mark_mssql_spool_directory_release_failure",
    "parse_byte_size",
]
