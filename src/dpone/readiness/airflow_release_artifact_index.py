"""Validate release identities and project immutable artifacts into an Airflow index."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment import (
    is_canonical_sha256_digest,
    is_sha256_digest,
)
from dpone.contracts.airflow_deployment import (
    release_id as compute_release_id,
)
from dpone.contracts.airflow_release_artifacts import (
    ReleaseArtifactLocatorError,
    release_artifact_path,
)
from dpone.manifest.confined_files import read_confined_file
from dpone.readiness.airflow_deployment_artifacts_io import (
    capture_confined_source,
    digest_dir,
    parse_json_object,
    sha256_bytes,
    workload_pack_fingerprint,
)
from dpone.readiness.airflow_deployment_projection_errors import (
    AirflowDeploymentProjectionError,
)

MAX_RELEASE_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_RUNTIME_PAYLOAD_BYTES = 256 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class ReleaseArtifactRules:
    """Validation rules for one release wire-contract version."""

    require_canonical_checksums: bool
    require_positive_bytes: bool
    require_pack_fingerprint: bool


def index_release_artifacts(
    release: Mapping[str, Any],
    *,
    release_id: str,
    cache_root: Path,
    section: str,
    rules: ReleaseArtifactRules,
    max_release_artifact_bytes: int = MAX_RELEASE_ARTIFACT_BYTES,
    max_runtime_payload_bytes: int = MAX_RUNTIME_PAYLOAD_BYTES,
    reader: Any = read_confined_file,
) -> list[dict[str, Any]]:
    """Validate and deterministically project one release artifact section."""

    entries = _validated_release_artifacts(release, section=section)
    indexed = [
        _index_release_artifact(
            raw_item,
            logical_id=logical_id,
            release_id=release_id,
            cache_root=cache_root,
            section=section,
            rules=rules,
            max_release_artifact_bytes=max_release_artifact_bytes,
            max_runtime_payload_bytes=max_runtime_payload_bytes,
            reader=reader,
        )
        for raw_item, logical_id in entries
    ]
    return sorted(indexed, key=lambda item: str(item["id"]))


def require_release_identity(
    release: Mapping[str, Any],
    *,
    requested_release_id: str,
    path: Path,
) -> None:
    """Verify that the release schema, claim, and content identity agree."""

    if release.get("schema") not in {
        "dpone.release-set.v1",
        "dpone.release-set.v2",
    }:
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_SCHEMA_INVALID",
            "release-set schema is invalid",
            path=path.as_posix(),
        )
    claimed_release_id = release.get("release_id")
    if not is_canonical_sha256_digest(claimed_release_id):
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_ID_INVALID",
            "release-set identity must be a canonical sha256 digest",
            path=path.as_posix(),
        )
    computed_release_id = compute_release_id(release)
    if claimed_release_id != computed_release_id:
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_FINGERPRINT_MISMATCH",
            "release-set content does not match its claimed identity",
            path=path.as_posix(),
        )
    if requested_release_id != computed_release_id:
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_ID_MISMATCH",
            "requested release identity does not match release-set content",
            path=path.as_posix(),
        )


def _validated_release_artifacts(
    release: Mapping[str, Any],
    *,
    section: str,
) -> tuple[tuple[Mapping[str, Any], str], ...]:
    artifacts = release.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_ARTIFACTS_INVALID",
            "release artifacts must be a mapping",
        )
    raw_items = artifacts.get(section, [])
    if not isinstance(raw_items, list):
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_ARTIFACTS_INVALID",
            f"{section} must be a list",
        )
    validated: list[tuple[Mapping[str, Any], str]] = []
    seen_ids: set[str] = set()
    for raw_item in raw_items:
        if not isinstance(raw_item, Mapping):
            raise AirflowDeploymentProjectionError(
                "DPONE_RELEASE_ARTIFACTS_INVALID",
                f"{section} item is invalid",
            )
        logical_id = raw_item.get("id")
        if not isinstance(logical_id, str) or not logical_id:
            raise AirflowDeploymentProjectionError(
                "DPONE_RELEASE_ARTIFACT_ID_MISSING",
                f"{section} id is missing",
            )
        if logical_id in seen_ids:
            raise AirflowDeploymentProjectionError(
                "DPONE_RELEASE_ARTIFACT_ID_DUPLICATE",
                f"{section} contains duplicate artifact ids",
            )
        seen_ids.add(logical_id)
        validated.append((raw_item, logical_id))
    return tuple(validated)


def _index_release_artifact(
    raw_item: Mapping[str, Any],
    *,
    logical_id: str,
    release_id: str,
    cache_root: Path,
    section: str,
    rules: ReleaseArtifactRules,
    max_release_artifact_bytes: int,
    max_runtime_payload_bytes: int,
    reader: Any,
) -> dict[str, Any]:
    try:
        artifact_path = release_artifact_path(raw_item)
    except ReleaseArtifactLocatorError as exc:
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_ARTIFACT_PATH_INVALID",
            "release artifact path is invalid",
        ) from exc
    expected_sha256 = str(raw_item.get("sha256") or "")
    digest_is_valid = (
        is_canonical_sha256_digest(expected_sha256)
        if rules.require_canonical_checksums
        else is_sha256_digest(expected_sha256)
    )
    if not digest_is_valid:
        raise AirflowDeploymentProjectionError(
            "DPONE_DEPLOYMENT_DIGEST_INVALID",
            "sha256 does not satisfy the selected projection wire contract",
        )
    relative_path = f"releases/{digest_dir(release_id)}/{artifact_path.as_posix()}"
    local_path = cache_root / relative_path
    content = capture_confined_source(
        cache_root,
        relative_path,
        path=local_path,
        max_bytes=(max_runtime_payload_bytes if section == "runtime_payloads" else max_release_artifact_bytes),
        missing_code="DPONE_RELEASE_ARTIFACT_NOT_FOUND",
        too_large_code="DPONE_RELEASE_ARTIFACT_TOO_LARGE",
        changed_code="DPONE_RELEASE_ARTIFACT_CHANGED",
        unsafe_code="DPONE_RELEASE_ARTIFACT_UNSAFE",
        reader=reader,
    )
    if rules.require_positive_bytes and not content:
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_ARTIFACT_EMPTY",
            "required release artifact is empty",
            path=local_path.as_posix(),
        )
    if rules.require_positive_bytes and section == "runtime_payloads":
        claimed_bytes = raw_item.get("bytes")
        if isinstance(claimed_bytes, bool) or not isinstance(claimed_bytes, int) or claimed_bytes <= 0:
            raise AirflowDeploymentProjectionError(
                "DPONE_RELEASE_ARTIFACT_BYTES_INVALID",
                "release artifact bytes must be a positive integer",
                path=local_path.as_posix(),
            )
        if claimed_bytes != len(content):
            raise AirflowDeploymentProjectionError(
                "DPONE_RELEASE_ARTIFACT_BYTES_MISMATCH",
                "release artifact byte count does not match release-set",
                path=local_path.as_posix(),
            )
    if sha256_bytes(content).lower() != expected_sha256.lower():
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_ARTIFACT_CHECKSUM_MISMATCH",
            "release artifact bytes do not match release-set",
            path=local_path.as_posix(),
        )
    payload: Mapping[str, Any] | None = None
    if rules.require_positive_bytes and section != "runtime_payloads":
        payload = parse_json_object(
            content,
            path=local_path,
            invalid_code="DPONE_RELEASE_ARTIFACT_INVALID",
            label=f"{section} artifact",
        )
    item = {
        "id": logical_id,
        "artifact_ref": f"cache://{relative_path}",
        "sha256": expected_sha256,
        "bytes": len(content),
    }
    if section == "workload_packs":
        _add_workload_fields(
            item,
            content=content,
            payload=payload,
            path=local_path,
            require_fingerprint=rules.require_pack_fingerprint,
        )
    elif section == "runtime_payloads":
        item["kind"] = _required_release_text(raw_item, "kind")
        item["media_type"] = _required_release_text(raw_item, "media_type")
    return item


def _add_workload_fields(
    item: dict[str, Any],
    *,
    content: bytes,
    payload: Mapping[str, Any] | None,
    path: Path,
    require_fingerprint: bool,
) -> None:
    fingerprint = workload_pack_fingerprint(
        content,
        payload=payload,
        path=path,
        required=require_fingerprint,
    )
    if fingerprint is not None:
        item["pack_fingerprint"] = fingerprint
    runtime_payload_ids = _runtime_payload_ids(payload, path=path)
    if runtime_payload_ids:
        item["runtime_payload_ids"] = list(runtime_payload_ids)


def _runtime_payload_ids(
    pack: Mapping[str, Any] | None,
    *,
    path: Path,
) -> tuple[str, ...]:
    if pack is None or pack.get("runtime_payload_ids") is None:
        return ()
    value = pack["runtime_payload_ids"]
    if not isinstance(value, list) or len(value) > 16:
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_RUNTIME_PAYLOAD_REFS_INVALID",
            "workload runtime_payload_ids must contain at most 16 entries",
            path=path.as_posix(),
        )
    if any(not isinstance(item, str) for item in value):
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_RUNTIME_PAYLOAD_REFS_INVALID",
            "workload runtime_payload_ids must be strings",
            path=path.as_posix(),
        )
    result = tuple(value)
    if any(not item or len(item) > 256 for item in result) or len(result) != len(set(result)):
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_RUNTIME_PAYLOAD_REFS_INVALID",
            "workload runtime_payload_ids must be unique bounded logical ids",
            path=path.as_posix(),
        )
    return tuple(sorted(result))


def _required_release_text(item: Mapping[str, Any], field: str) -> str:
    value = item.get(field)
    if not isinstance(value, str) or not value or len(value) > 256:
        raise AirflowDeploymentProjectionError(
            "DPONE_RELEASE_RUNTIME_PAYLOAD_INVALID",
            f"runtime payload {field} is invalid",
        )
    return value


__all__ = [
    "MAX_RELEASE_ARTIFACT_BYTES",
    "MAX_RUNTIME_PAYLOAD_BYTES",
    "ReleaseArtifactRules",
    "index_release_artifacts",
    "require_release_identity",
]
