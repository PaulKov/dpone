"""Schema-aware artifact projections for Airflow deployment indexes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_artifact_contract import (
    resolve_cache_artifact,
    verify_cache_artifact,
)
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError

_SHA256_PREFIX = "sha256:"


@dataclass(frozen=True)
class AirflowIndexArtifact:
    """One immutable deployment-index artifact projection."""

    id: str
    artifact_ref: str
    sha256: str
    path: Path
    bytes: int | None
    cache_root: Path | None = None
    pack_fingerprint: str | None = None
    runtime_payload_ids: tuple[str, ...] = ()


def load_index_artifacts(
    payload: Mapping[str, Any],
    key: str,
    *,
    release_id: str,
    cache_root: Path,
    max_artifact_bytes: int,
    verify_artifacts: bool,
    strict_v2: bool,
    workload_packs: bool,
    warn_legacy_missing_bytes: Callable[[], None],
    path: Path,
) -> tuple[AirflowIndexArtifact, ...]:
    """Project and optionally verify one indexed artifact collection."""

    raw = payload.get(key)
    if raw is None:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_MISSING",
            f"{key} is required",
            path=path.as_posix(),
        )
    if not isinstance(raw, list):
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
            f"{key} must be a list",
            path=path.as_posix(),
        )
    artifacts: list[AirflowIndexArtifact] = []
    seen_ids: set[str] = set()
    for item in raw:
        artifacts.append(
            _load_index_artifact(
                item,
                key=key,
                release_id=release_id,
                cache_root=cache_root,
                max_artifact_bytes=max_artifact_bytes,
                verify_artifacts=verify_artifacts,
                strict_v2=strict_v2,
                workload_packs=workload_packs,
                warn_legacy_missing_bytes=warn_legacy_missing_bytes,
                seen_ids=seen_ids,
                path=path,
            )
        )
    return tuple(sorted(artifacts, key=lambda artifact: artifact.id))


def _load_index_artifact(
    value: object,
    *,
    key: str,
    release_id: str,
    cache_root: Path,
    max_artifact_bytes: int,
    verify_artifacts: bool,
    strict_v2: bool,
    workload_packs: bool,
    warn_legacy_missing_bytes: Callable[[], None],
    seen_ids: set[str],
    path: Path,
) -> AirflowIndexArtifact:
    if not isinstance(value, Mapping):
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
            "artifact must be an object",
            path=path.as_posix(),
        )
    _validate_v2_fields(value, key=key, workload_packs=workload_packs, strict_v2=strict_v2, path=path)
    artifact_id = _required_text(value, "id", path)
    if artifact_id in seen_ids:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_ARTIFACT_DUPLICATE",
            f"{key} contains duplicate artifact id: {artifact_id}",
            path=path.as_posix(),
        )
    seen_ids.add(artifact_id)
    artifact_ref = _required_text(value, "artifact_ref", path)
    expected_sha256 = required_sha256(value, "sha256", path)
    declared_bytes = _required_positive_int(value, "bytes", path) if "bytes" in value else None
    if declared_bytes is None:
        if strict_v2:
            raise AirflowDeploymentIndexError(
                "DPONE_AIRFLOW_INDEX_FIELD_MISSING",
                f"{key}[].bytes is required by wire v2",
                path=path.as_posix(),
            )
        warn_legacy_missing_bytes()
    pack_fingerprint = (
        required_sha256(value, "pack_fingerprint", path)
        if strict_v2 and workload_packs
        else optional_sha256(value.get("pack_fingerprint"), "pack_fingerprint", path)
    )
    runtime_payload_ids = (
        _runtime_payload_ids(value.get("runtime_payload_ids"), path=path) if strict_v2 and workload_packs else ()
    )
    artifact_path = resolve_cache_artifact(artifact_ref, cache_root=cache_root)
    _require_release_artifact_path(
        artifact_path,
        artifact_ref=artifact_ref,
        release_id=release_id,
        cache_root=cache_root,
    )
    artifact = AirflowIndexArtifact(
        id=artifact_id,
        artifact_ref=artifact_ref,
        sha256=expected_sha256,
        path=artifact_path,
        bytes=declared_bytes,
        cache_root=cache_root,
        pack_fingerprint=pack_fingerprint,
        runtime_payload_ids=runtime_payload_ids,
    )
    if not verify_artifacts:
        return artifact
    return AirflowIndexArtifact(
        id=artifact.id,
        artifact_ref=artifact.artifact_ref,
        sha256=artifact.sha256,
        path=artifact.path,
        bytes=verify_airflow_index_artifact(
            artifact,
            max_artifact_bytes=max_artifact_bytes,
        ),
        cache_root=artifact.cache_root,
        pack_fingerprint=artifact.pack_fingerprint,
        runtime_payload_ids=artifact.runtime_payload_ids,
    )


def _validate_v2_fields(
    item: Mapping[str, Any],
    *,
    key: str,
    workload_packs: bool,
    strict_v2: bool,
    path: Path,
) -> None:
    if not strict_v2:
        return
    expected_fields = {"id", "artifact_ref", "sha256", "bytes"}
    if workload_packs:
        expected_fields.add("pack_fingerprint")
    actual_fields = set(item)
    allowed_fields = expected_fields | {"runtime_payload_ids"} if workload_packs else expected_fields
    if not expected_fields.issubset(actual_fields) or not actual_fields.issubset(allowed_fields):
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
            f"{key}[] contains missing or unknown wire v2 fields",
            path=path.as_posix(),
        )


def _runtime_payload_ids(
    value: object,
    *,
    path: Path,
) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or len(value) > 16:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
            "workload_packs[].runtime_payload_ids must contain at most 16 entries",
            path=path.as_posix(),
        )
    result = []
    for item in value:
        if not isinstance(item, str) or not item or len(item) > 256:
            raise AirflowDeploymentIndexError(
                "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                "workload runtime payload id is invalid",
                path=path.as_posix(),
            )
        result.append(item)
    if len(result) != len(set(result)):
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
            "workload runtime payload ids must be unique",
            path=path.as_posix(),
        )
    return tuple(sorted(result))


def verify_airflow_index_artifact(
    artifact: AirflowIndexArtifact,
    *,
    max_artifact_bytes: int,
) -> int:
    """Verify one listed artifact at its isolated consumption boundary."""

    return verify_cache_artifact(
        artifact.path,
        expected_sha256=artifact.sha256,
        declared_bytes=artifact.bytes,
        max_artifact_bytes=max_artifact_bytes,
        cache_root=artifact.cache_root,
    )


def required_sha256(
    payload: Mapping[str, Any],
    key: str,
    path: Path | None,
) -> str:
    value = _required_text(payload, key, path)
    if not _is_canonical_sha256_digest(value):
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_ID_INVALID",
            f"{key} must be a sha256 digest",
            path=path.as_posix() if path else None,
        )
    return value


def optional_text(value: object, key: str, path: Path | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
            f"{key} must be a non-empty string or null",
            path=path.as_posix() if path else None,
        )
    return value


def optional_sha256(
    value: object,
    key: str,
    path: Path | None,
) -> str | None:
    text = optional_text(value, key, path)
    if text is None:
        return None
    if not _is_canonical_sha256_digest(text):
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_ID_INVALID",
            f"{key} must be a sha256 digest or null",
            path=path.as_posix() if path else None,
        )
    return text


def _required_text(
    payload: Mapping[str, Any],
    key: str,
    path: Path | None,
) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_MISSING",
            f"{key} is required",
            path=path.as_posix() if path else None,
        )
    return value


def _required_positive_int(
    payload: Mapping[str, Any],
    key: str,
    path: Path | None,
) -> int:
    if key not in payload:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_MISSING",
            f"{key} is required",
            path=path.as_posix() if path else None,
        )
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
            f"{key} must be a positive integer",
            path=path.as_posix() if path else None,
        )
    return value


def _require_release_artifact_path(
    artifact_path: Path,
    *,
    artifact_ref: str,
    release_id: str,
    cache_root: Path,
) -> None:
    expected_release_root = cache_root / "releases" / release_id.replace(":", "-", 1)
    try:
        artifact_path.relative_to(expected_release_root)
    except ValueError as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_RELEASE_ID_MISMATCH",
            "cache artifact path does not match deployment index release_id",
            path=artifact_ref,
        ) from exc


def _is_canonical_sha256_digest(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith(_SHA256_PREFIX):
        return False
    digest = value[len(_SHA256_PREFIX) :]
    return len(digest) == 64 and all(char in "0123456789abcdef" for char in digest)


__all__ = [
    "AirflowIndexArtifact",
    "load_index_artifacts",
    "optional_sha256",
    "optional_text",
    "required_sha256",
    "verify_airflow_index_artifact",
]
