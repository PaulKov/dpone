"""Verified deployment-index artifacts for run-neutral semantic-refresh DAGs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_artifact_contract import (
    read_confined_cache_file,
    resolve_cache_artifact,
)
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError
from dpone_airflow_pack.semantic_refresh_dag_authority import (
    LocalSemanticRefreshDagProjectionAuthority,
    SemanticRefreshDagAuthorityError,
)
from dpone_airflow_pack.semantic_refresh_dag_projection import (
    AuthenticatedSemanticRefreshDagProjection,
    SemanticRefreshProjectionError,
    authenticate_semantic_refresh_dag_projection,
)

_DESCRIPTOR_FIELDS = frozenset(
    {
        "artifact_bytes",
        "artifact_ref",
        "artifact_sha256",
        "authority",
        "dag_id",
        "dag_projection_sha256",
        "projection_id",
        "workflow_name",
    }
)
_MAX_PROJECTIONS = 64


@dataclass(frozen=True, slots=True)
class SemanticRefreshDagProjectionArtifact:
    """One immutable sidecar authorized by a verified deployment index."""

    projection_id: str
    workflow_name: str
    dag_id: str
    dag_projection_sha256: str
    artifact_ref: str
    artifact_sha256: str
    artifact_bytes: int
    path: Path
    cache_root: Path
    authority: LocalSemanticRefreshDagProjectionAuthority


def load_semantic_refresh_index_artifacts(
    payload: Mapping[str, Any],
    *,
    release_id: str,
    deployment_id: str,
    environment: str,
    cache_root: Path,
    max_artifact_bytes: int,
    verify_artifacts: bool,
    path: Path,
) -> tuple[SemanticRefreshDagProjectionArtifact, ...]:
    """Parse a closed, deployment-confined semantic-refresh sidecar inventory."""

    raw = payload.get("semantic_refresh_dag_projections", [])
    if not isinstance(raw, list) or len(raw) > _MAX_PROJECTIONS:
        raise _invalid("semantic_refresh_dag_projections must contain at most 64 entries", path)
    artifacts = tuple(
        _parse_artifact(
            value,
            release_id=release_id,
            deployment_id=deployment_id,
            environment=environment,
            cache_root=cache_root,
            max_artifact_bytes=max_artifact_bytes,
            path=path,
        )
        for value in raw
    )
    projection_ids = tuple(item.projection_id for item in artifacts)
    dag_ids = tuple(item.dag_id for item in artifacts)
    if len(set(projection_ids)) != len(projection_ids) or len(set(dag_ids)) != len(dag_ids):
        raise _invalid("semantic-refresh projection and DAG identities must be unique", path)
    if verify_artifacts:
        for artifact in artifacts:
            load_semantic_refresh_dag_projection_artifact(
                artifact,
                max_artifact_bytes=max_artifact_bytes,
            )
    return tuple(sorted(artifacts, key=lambda item: item.projection_id))


def load_semantic_refresh_dag_projection_artifact(
    artifact: SemanticRefreshDagProjectionArtifact,
    *,
    max_artifact_bytes: int,
) -> AuthenticatedSemanticRefreshDagProjection:
    """Read and authenticate one exact local sidecar before DAG construction."""

    if artifact.artifact_bytes > max_artifact_bytes:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_TOO_LARGE",
            "semantic-refresh DAG projection exceeds the configured size limit",
            path=artifact.path.as_posix(),
        )
    raw = read_confined_cache_file(
        artifact.path,
        cache_root=artifact.cache_root,
        max_bytes=max_artifact_bytes,
    )
    if len(raw) != artifact.artifact_bytes:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH",
            "semantic-refresh DAG projection size differs from the deployment index",
            path=artifact.path.as_posix(),
        )
    if "sha256:" + hashlib.sha256(raw).hexdigest() != artifact.artifact_sha256:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_CHECKSUM_MISMATCH",
            "semantic-refresh DAG projection digest differs from the deployment index",
            path=artifact.path.as_posix(),
        )
    payload = _strict_json_object(raw, path=artifact.path)
    try:
        projection = authenticate_semantic_refresh_dag_projection(
            payload,
            authority=artifact.authority,
        )
    except (SemanticRefreshDagAuthorityError, SemanticRefreshProjectionError) as exc:
        raise _invalid("semantic-refresh DAG projection authority is invalid", artifact.path) from exc
    if (
        projection.identity.dag_projection_sha256 != artifact.dag_projection_sha256
        or projection.workflow_name != artifact.workflow_name
        or projection.projection.dag_id != artifact.dag_id
    ):
        raise _invalid("semantic-refresh DAG projection descriptor differs from its bytes", artifact.path)
    return projection


def _parse_artifact(
    value: object,
    *,
    release_id: str,
    deployment_id: str,
    environment: str,
    cache_root: Path,
    max_artifact_bytes: int,
    path: Path,
) -> SemanticRefreshDagProjectionArtifact:
    if not isinstance(value, Mapping) or set(value) != _DESCRIPTOR_FIELDS:
        raise _invalid("semantic-refresh DAG projection descriptor fields are not closed", path)
    projection_id = _text(value.get("projection_id"), "projection_id", path)
    workflow_name = _text(value.get("workflow_name"), "workflow_name", path)
    dag_id = _text(value.get("dag_id"), "dag_id", path)
    if projection_id != f"semantic_refresh_v2::{dag_id}":
        raise _invalid("semantic-refresh projection_id must bind the exact dag_id", path)
    projection_sha256 = _digest(value.get("dag_projection_sha256"), "dag_projection_sha256", path)
    artifact_sha256 = _digest(value.get("artifact_sha256"), "artifact_sha256", path)
    artifact_bytes = _positive_int(value.get("artifact_bytes"), "artifact_bytes", path)
    if artifact_bytes > max_artifact_bytes:
        raise _invalid("semantic-refresh DAG projection declared size exceeds the limit", path)
    artifact_ref = _text(value.get("artifact_ref"), "artifact_ref", path)
    filename = f"semantic-refresh-{projection_sha256.removeprefix('sha256:')}.dag-projection.json"
    expected_ref = f"cache://deployments/{environment}/{deployment_id.replace(':', '-', 1)}/{filename}"
    if artifact_ref != expected_ref:
        raise _invalid("semantic-refresh DAG projection is outside its exact deployment", path)
    try:
        authority = LocalSemanticRefreshDagProjectionAuthority.from_mapping(value.get("authority"))
    except SemanticRefreshDagAuthorityError as exc:
        raise _invalid("semantic-refresh DAG projection authority record is invalid", path) from exc
    identity = authority.identity
    if (
        identity.dag_projection_sha256 != projection_sha256
        or identity.release_id != release_id
        or identity.deployment_id != deployment_id
    ):
        raise _invalid("semantic-refresh DAG projection authority differs from its index", path)
    return SemanticRefreshDagProjectionArtifact(
        projection_id=projection_id,
        workflow_name=workflow_name,
        dag_id=dag_id,
        dag_projection_sha256=projection_sha256,
        artifact_ref=artifact_ref,
        artifact_sha256=artifact_sha256,
        artifact_bytes=artifact_bytes,
        path=resolve_cache_artifact(artifact_ref, cache_root=cache_root),
        cache_root=cache_root,
        authority=authority,
    )


def _strict_json_object(raw: bytes, *, path: Path) -> Mapping[str, object]:
    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise _invalid("semantic-refresh DAG projection must be strict UTF-8 JSON", path) from exc
    if not isinstance(payload, Mapping):
        raise _invalid("semantic-refresh DAG projection must be a JSON object", path)
    return payload


def _unique_object(items: list[tuple[str, object]]) -> dict[str, object]:
    result = dict(items)
    if len(result) != len(items):
        raise ValueError("duplicate JSON key")
    return result


def _text(value: object, field: str, path: Path) -> str:
    if not isinstance(value, str) or not value or len(value) > 512:
        raise _invalid(f"semantic-refresh {field} is invalid", path)
    return value


def _digest(value: object, field: str, path: Path) -> str:
    text = _text(value, field, path)
    if (
        not text.startswith("sha256:")
        or len(text) != 71
        or any(character not in "0123456789abcdef" for character in text[7:])
    ):
        raise _invalid(f"semantic-refresh {field} must be a canonical digest", path)
    return text


def _positive_int(value: object, field: str, path: Path) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _invalid(f"semantic-refresh {field} must be a positive integer", path)
    return value


def _invalid(message: str, path: Path) -> AirflowDeploymentIndexError:
    return AirflowDeploymentIndexError(
        "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
        message,
        path=path.as_posix(),
    )


__all__ = [
    "SemanticRefreshDagProjectionArtifact",
    "load_semantic_refresh_dag_projection_artifact",
    "load_semantic_refresh_index_artifacts",
]
