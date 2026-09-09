"""Install a compiled dbt release under its canonical content address."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.contracts.dbt_release import (
    dbt_release_authority_violation,
    dbt_release_producer_violation,
    dbt_release_runtime_wire_contract,
    is_workspace_dbt_wire,
)
from dpone.contracts.strict_json import StrictJsonError, strict_json_object
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.manifest.confined_files import read_confined_file
from dpone.runtime.deployment_cache_common import DeploymentCacheError, promotion_lock
from dpone.runtime.immutable_local_release import (
    ImmutableLocalReleaseError,
    materialize_immutable_local_release,
)
from dpone.version import installed_version

if TYPE_CHECKING:
    from dpone.ports.dbt_release_files import VerifiedWorkspaceReleaseCapture

MAX_DBT_RELEASE_SET_BYTES = 8 * 1024 * 1024
MAX_DBT_RELEASE_ARTIFACT_BYTES = 256 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class MaterializedDbtRelease:
    release_id: str
    release_dir: Path
    no_op: bool


class DbtReleaseMaterializationError(RuntimeError):
    """Stable build-plane failure before an immutable release is installed."""

    def __init__(self, message: str, *, code: str = "DPONE_DBT_PROJECT_BUNDLE_INVALID") -> None:
        super().__init__(message)
        self.code = code


class DbtReleaseMaterializer:
    """Validate descriptors, then publish one immutable local release tree."""

    def __init__(self, *, workspace_source_reader: VerifiedWorkspaceReleaseCapture | None = None) -> None:
        self._workspace_sources = workspace_source_reader

    def materialize(
        self,
        *,
        compiled_root: Path,
        cache_root: Path,
        expected_release_id: str | None = None,
    ) -> MaterializedDbtRelease:
        compiled = compiled_root.absolute()
        release_bytes = _read_release_file(
            compiled,
            "release-set.json",
            max_bytes=MAX_DBT_RELEASE_SET_BYTES,
        )
        release = _json_object(release_bytes)
        if release.get("schema") != "dpone.release-set.v2":
            raise DbtReleaseMaterializationError("compiled dbt release must use dpone.release-set.v2")
        try:
            wire_contract = dbt_release_runtime_wire_contract(release)
        except ValueError as exc:
            raise DbtReleaseMaterializationError("compiled dbt release producer identity is invalid") from exc
        authority_violation = dbt_release_authority_violation(release, expected_wire_contract=wire_contract)
        if authority_violation is not None:
            raise DbtReleaseMaterializationError(authority_violation)
        issues = GitOpsSchemaValidator().validate(
            release,
            expected_kind="dpone.release-set.v2",
        )
        if issues:
            raise DbtReleaseMaterializationError("compiled dbt release violates dpone.release-set.v2")
        claimed_value = release.get("release_id")
        if not isinstance(claimed_value, str):
            raise DbtReleaseMaterializationError("compiled dbt release id is invalid")
        claimed = claimed_value
        if claimed != compute_release_id(release):
            raise DbtReleaseMaterializationError("compiled dbt release identity does not match its content")
        if expected_release_id is not None and claimed != expected_release_id:
            raise DbtReleaseMaterializationError("compiled dbt release differs from the expected release")
        producer_violation = dbt_release_producer_violation(
            release,
            expected_dpone_version=installed_version(),
            expected_wire_contract=wire_contract,
        )
        if producer_violation is not None:
            raise DbtReleaseMaterializationError(producer_violation)
        if is_workspace_dbt_wire(wire_contract):
            if self._workspace_sources is None:
                raise DbtReleaseMaterializationError("workspace installation requires a complete-source verifier")
            try:
                files = dict(
                    self._workspace_sources.capture_verified_files(
                        compiled, release_payload=release_bytes, expected_release_id=claimed
                    )
                )
            except (OSError, ValueError) as exc:
                raise DbtReleaseMaterializationError(
                    "compiled workspace sources or integrity subject are invalid"
                ) from exc
        else:
            files = _legacy_files(compiled, release, release_bytes)
        release_dir = cache_root.absolute() / "releases" / claimed.replace(":", "-", 1)
        try:
            with promotion_lock(cache_root.absolute()):
                pass
        except DeploymentCacheError as exc:
            raise DbtReleaseMaterializationError(
                "dbt release cache writer lease could not be initialized",
                code="DPONE_DBT_RELEASE_CACHE_LOCK_FAILED",
            ) from exc
        try:
            publication_status = materialize_immutable_local_release(release_dir, files)
        except (ImmutableLocalReleaseError, OSError, ValueError) as exc:
            raise DbtReleaseMaterializationError("compiled dbt release could not be installed atomically") from exc
        return MaterializedDbtRelease(
            claimed,
            release_dir,
            no_op=publication_status == "no_op",
        )


def _legacy_files(compiled: Path, release: Mapping[str, Any], release_bytes: bytes) -> dict[str, bytes]:
    """Preserve the existing singleton descriptor capture and layout."""
    files = {"release-set.json": release_bytes}
    artifacts = release.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise DbtReleaseMaterializationError("compiled dbt release artifacts must be an object")
    for section in ("dag_specs", "workload_packs", "canonical_schemas", "runtime_payloads"):
        items = artifacts.get(section)
        if not isinstance(items, list):
            raise DbtReleaseMaterializationError(f"compiled dbt release artifacts.{section} must be an array")
        for item in items:
            if not isinstance(item, Mapping):
                raise DbtReleaseMaterializationError("compiled dbt release descriptor must be an object")
            relative_value = item.get("path")
            if not isinstance(relative_value, str):
                raise DbtReleaseMaterializationError("compiled dbt release descriptor path is invalid")
            relative = relative_value
            data = _read_release_file(
                compiled,
                relative,
                max_bytes=MAX_DBT_RELEASE_ARTIFACT_BYTES,
            )
            if len(data) != item.get("bytes") or _sha256(data) != item.get("sha256"):
                raise DbtReleaseMaterializationError("compiled dbt release descriptor differs from artifact bytes")
            if relative in files:
                raise DbtReleaseMaterializationError("compiled dbt release contains duplicate artifact paths")
            files[relative] = data
    return files


def _json_object(payload: bytes) -> dict[str, Any]:
    try:
        value = strict_json_object(payload)
    except StrictJsonError as exc:
        raise DbtReleaseMaterializationError("compiled dbt release-set is invalid JSON") from exc
    return value


def _sha256(payload: bytes) -> str:
    import hashlib

    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _read_release_file(root: Path, relative_path: str, *, max_bytes: int) -> bytes:
    try:
        return read_confined_file(root, relative_path, max_bytes=max_bytes)
    except (OSError, ValueError) as exc:
        raise DbtReleaseMaterializationError(
            "compiled dbt release is missing, unsafe, or exceeds its size limit"
        ) from exc


__all__ = [
    "DbtReleaseMaterializationError",
    "DbtReleaseMaterializer",
    "MaterializedDbtRelease",
]
