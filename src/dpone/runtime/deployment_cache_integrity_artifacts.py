"""Artifact parsing primitives for deployment-cache integrity verification."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.contracts.airflow_release_artifacts import ReleaseArtifactLocatorError, release_artifact_path
from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    read_regular_json_object,
)


@dataclass(frozen=True, slots=True)
class ReleaseArtifact:
    """One immutable release-set artifact descriptor."""

    logical_id: str
    relative_path: PurePosixPath
    sha256: str


@dataclass(frozen=True, slots=True)
class IndexArtifact:
    """One Airflow index artifact descriptor."""

    logical_id: str
    artifact_ref: str
    sha256: str
    declared_bytes: int | None


def read_release_set(path: Path, *, root: Path) -> dict[str, Any]:
    """Read one confined release-set document."""

    try:
        return read_regular_json_object(
            path,
            missing_code="DPONE_RELEASE_NOT_FOUND",
            invalid_code="DPONE_RELEASE_INVALID",
            label="release-set",
            root=root,
        )
    except OSError as exc:
        raise DeploymentCacheError(
            "DPONE_RELEASE_READ_FAILED",
            "release-set could not be read",
            path=path.as_posix(),
        ) from exc


def release_artifacts(value: Any, *, section: str, path: Path) -> dict[str, ReleaseArtifact]:
    """Parse one closed release-set artifact section."""

    if not isinstance(value, list):
        raise DeploymentCacheError(
            "DPONE_RELEASE_ARTIFACTS_INVALID",
            f"release-set {section} must be an array",
            path=path.as_posix(),
        )
    artifacts: dict[str, ReleaseArtifact] = {}
    for item in value:
        if not isinstance(item, Mapping):
            raise DeploymentCacheError(
                "DPONE_RELEASE_ARTIFACTS_INVALID",
                f"release-set {section} entries must be objects",
                path=path.as_posix(),
            )
        logical_id = required_text(item, "id", code="DPONE_RELEASE_ARTIFACTS_INVALID", path=path)
        relative_path = release_artifact_relative_path(item, path=path)
        sha256 = required_digest(item, "sha256", code="DPONE_RELEASE_ARTIFACTS_INVALID", path=path)
        if logical_id in artifacts:
            raise DeploymentCacheError(
                "DPONE_RELEASE_ARTIFACTS_INVALID",
                f"release-set {section} contains duplicate artifact ids",
                path=path.as_posix(),
            )
        artifacts[logical_id] = ReleaseArtifact(logical_id, relative_path, sha256)
    return artifacts


def release_artifact_relative_path(item: Mapping[str, Any], *, path: Path) -> PurePosixPath:
    """Parse and confine one release artifact path."""

    try:
        return release_artifact_path(item)
    except ReleaseArtifactLocatorError as exc:
        raise DeploymentCacheError(
            "DPONE_RELEASE_ARTIFACTS_INVALID",
            str(exc),
            path=path.as_posix(),
        ) from exc


def index_artifacts(value: Any, *, section: str, path: Path) -> dict[str, IndexArtifact]:
    """Parse one closed Airflow index artifact section."""

    if not isinstance(value, list):
        raise DeploymentCacheError(
            "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
            f"airflow index {section} must be an array",
            path=path.as_posix(),
        )
    artifacts: dict[str, IndexArtifact] = {}
    for item in value:
        if not isinstance(item, Mapping):
            raise DeploymentCacheError(
                "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
                f"airflow index {section} entries must be objects",
                path=path.as_posix(),
            )
        logical_id = required_text(item, "id", code="DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID", path=path)
        artifact_ref = required_text(item, "artifact_ref", code="DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID", path=path)
        sha256 = required_digest(item, "sha256", code="DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID", path=path)
        declared_bytes = optional_size(item.get("bytes"), path=path)
        if logical_id in artifacts:
            raise DeploymentCacheError(
                "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
                f"airflow index {section} contains duplicate artifact ids",
                path=path.as_posix(),
            )
        artifacts[logical_id] = IndexArtifact(logical_id, artifact_ref, sha256, declared_bytes)
    return artifacts


def required_text(payload: Mapping[str, Any], key: str, *, code: str, path: Path) -> str:
    """Read one required non-empty artifact field."""

    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise DeploymentCacheError(code, f"artifact {key} is required", path=path.as_posix())
    return value


def required_digest(payload: Mapping[str, Any], key: str, *, code: str, path: Path) -> str:
    """Read one required artifact SHA-256 digest."""

    value = required_text(payload, key, code=code, path=path)
    if not _is_sha256_metadata_digest(value):
        raise DeploymentCacheError(code, f"artifact {key} must be a sha256 digest", path=path.as_posix())
    return value


def _is_sha256_metadata_digest(value: str) -> bool:
    """Accept the case-insensitive hex spelling allowed by release metadata."""

    prefix = "sha256:"
    if not value.startswith(prefix):
        return False
    digest = value[len(prefix) :]
    return len(digest) == 64 and all(character in "0123456789abcdefABCDEF" for character in digest)


def optional_size(value: Any, *, path: Path) -> int | None:
    """Validate an optional non-negative byte size."""

    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DeploymentCacheError(
            "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
            "artifact bytes must be a non-negative integer",
            path=path.as_posix(),
        )
    return value


def required_positive_size(value: Any, *, path: Path) -> int:
    """Validate a required positive sidecar byte size."""

    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise DeploymentCacheError(
            "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
            "semantic-refresh artifact_bytes must be a positive integer",
            path=path.as_posix(),
        )
    return value


def digest_dir(value: str) -> str:
    """Map a canonical digest to its cache-directory spelling."""

    return value.replace(":", "-", 1)


def require_inside_root(
    candidate: Path,
    *,
    root: Path,
    code: str,
    message: str,
    error_path: Path,
) -> None:
    """Reject a resolved path outside its trusted cache root."""

    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise DeploymentCacheError(code, message, path=error_path.as_posix()) from exc
