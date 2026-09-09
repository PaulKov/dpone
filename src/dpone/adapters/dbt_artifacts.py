"""Bounded local readers and atomic writers for dbt runtime artifacts."""

from __future__ import annotations

import os
import stat
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone_airflow_pack.dev_evidence_store import (
    DevEvidenceConfinedStore,
    DevEvidenceStoreError,
    evidence_set_directory_parts,
    logical_evidence_filename,
)

from dpone.adapters.deployment_cache_files import DeploymentCacheError, open_regular_file
from dpone.contracts.dbt_publishing import (
    DbtExecutionEvidence,
    DbtPublishingError,
    canonical_dbt_execution_evidence_bytes,
)
from dpone.contracts.strict_json import StrictJsonError, strict_json_object

if TYPE_CHECKING:
    from dpone.ports.dbt_publishing import DbtExecutionEvidenceWriter

DBT_DEV_EVIDENCE_ROOT_ENV = "DPONE_DBT_EVIDENCE_EXPORT_ROOT"
DBT_DEV_EVIDENCE_SET_ENV = "DPONE_DBT_EVIDENCE_SET_ID"


class LocalDbtRunResultsReader:
    """Read one confined regular JSON artifact without following symlinks."""

    def read(self, path: Path, *, root: Path, max_bytes: int) -> Mapping[str, Any]:
        try:
            descriptor = open_regular_file(
                path,
                missing_code="DPONE_DBT_RESULTS_INVALID",
                invalid_code="DPONE_DBT_RESULTS_INVALID",
                label="dbt run-results",
                root=root,
            )
            with os.fdopen(descriptor, "rb") as handle:
                raw = handle.read(max_bytes + 1)
        except (DeploymentCacheError, OSError) as exc:
            raise DbtPublishingError("DPONE_DBT_RESULTS_INVALID", "dbt run-results is unavailable or unsafe") from exc
        if len(raw) > max_bytes:
            raise DbtPublishingError("DPONE_DBT_RESULTS_INVALID", "dbt run-results exceeds its byte limit")
        try:
            payload = strict_json_object(raw)
        except StrictJsonError as exc:
            raise DbtPublishingError("DPONE_DBT_RESULTS_INVALID", "dbt run-results JSON is invalid") from exc
        return payload


class LocalDbtExecutionEvidenceWriter:
    """Publish canonical mode-0600 evidence once, never overwrite it."""

    def __init__(self, path: Path) -> None:
        self._path = Path(path).absolute()

    def write(self, evidence: DbtExecutionEvidence) -> Path:
        try:
            data = dbt_execution_evidence_bytes(evidence)
        except (TypeError, ValueError) as exc:
            raise DbtPublishingError(
                "DPONE_DBT_EVIDENCE_WRITE_FAILED",
                "dbt evidence contains a non-JSON value",
            ) from exc
        parent = self._path.parent
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        mode = parent.lstat().st_mode
        if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
            raise DbtPublishingError("DPONE_DBT_EVIDENCE_WRITE_FAILED", "dbt evidence parent is unsafe")
        if self._path.exists() or self._path.is_symlink():
            if self._read_existing() == data:
                return self._path
            raise DbtPublishingError("DPONE_DBT_EVIDENCE_WRITE_FAILED", "dbt evidence already exists with other bytes")
        temporary: Path | None = None
        try:
            descriptor, raw_path = tempfile.mkstemp(prefix=f".{self._path.name}.", dir=parent)
            temporary = Path(raw_path)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600, follow_symlinks=False)
            os.link(temporary, self._path, follow_symlinks=False)
            directory_descriptor = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except FileExistsError:
            if self._read_existing() != data:
                raise DbtPublishingError(
                    "DPONE_DBT_EVIDENCE_WRITE_FAILED",
                    "dbt evidence publication lost a conflicting create race",
                ) from None
        except OSError as exc:
            raise DbtPublishingError("DPONE_DBT_EVIDENCE_WRITE_FAILED", "dbt evidence could not be persisted") from exc
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        return self._path

    def _read_existing(self) -> bytes:
        try:
            descriptor = open_regular_file(
                self._path,
                missing_code="DPONE_DBT_EVIDENCE_WRITE_FAILED",
                invalid_code="DPONE_DBT_EVIDENCE_WRITE_FAILED",
                label="dbt execution evidence",
                root=self._path.parent,
            )
            with os.fdopen(descriptor, "rb") as handle:
                return handle.read()
        except (DeploymentCacheError, OSError) as exc:
            raise DbtPublishingError("DPONE_DBT_EVIDENCE_WRITE_FAILED", "existing dbt evidence is unsafe") from exc


class CampaignDbtExecutionEvidenceWriter:
    """Persist local evidence and optionally mirror it to the pinned dev set."""

    def __init__(
        self,
        primary: DbtExecutionEvidenceWriter,
        *,
        evidence_root: str | None,
        evidence_set_id: str | None,
    ) -> None:
        self._primary = primary
        self._evidence_root = str(evidence_root or "")
        self._evidence_set_id = str(evidence_set_id or "")

    def write(self, evidence: DbtExecutionEvidence) -> Path:
        local_path = self._primary.write(evidence)
        if not self._evidence_set_id:
            return local_path
        try:
            store = DevEvidenceConfinedStore(Path(self._evidence_root))
            base = evidence_set_directory_parts(
                evidence.release_id,
                evidence.deployment_id,
                self._evidence_set_id,
            )
            store.install(
                directory_parts=(*base, "dbt"),
                filename=logical_evidence_filename(evidence.workflow_id),
                payload=dbt_execution_evidence_bytes(evidence),
            )
        except (DevEvidenceStoreError, OSError, TypeError, ValueError) as exc:
            raise DbtPublishingError(
                "DPONE_DBT_DEV_EVIDENCE_EXPORT_FAILED",
                "exact dbt dev evidence could not be installed safely",
            ) from exc
        return local_path


def dbt_execution_evidence_bytes(evidence: DbtExecutionEvidence) -> bytes:
    """Return the single canonical on-disk representation of dbt evidence."""

    return canonical_dbt_execution_evidence_bytes(evidence.to_dict())


__all__ = [
    "DBT_DEV_EVIDENCE_ROOT_ENV",
    "DBT_DEV_EVIDENCE_SET_ENV",
    "CampaignDbtExecutionEvidenceWriter",
    "LocalDbtExecutionEvidenceWriter",
    "LocalDbtRunResultsReader",
    "dbt_execution_evidence_bytes",
]
