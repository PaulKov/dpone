"""Deployment-index schema parsing and artifact contract helpers."""

from __future__ import annotations

import hashlib
import json
import math
import os
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone_airflow_pack.cache_artifact_contract import (
    DEFAULT_MAX_ARTIFACT_BYTES,
    CacheActivationIdentity,
    infer_cache_root,
    read_confined_cache_file_with_identity,
    resolve_cache_artifact,
)
from dpone_airflow_pack.composition_supervisor_contract import (
    parse_composition_supervisor,
)
from dpone_airflow_pack.deployment_index_artifacts import (
    AirflowIndexArtifact,
    _is_canonical_sha256_digest,
    load_index_artifacts,
    optional_sha256,
    optional_text,
    required_sha256,
    verify_airflow_index_artifact,
)
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError
from dpone_airflow_pack.init_fetch_contract import (
    AIRFLOW_INDEX_SCHEMA_V2,
    AIRFLOW_INDEX_SCHEMA_V3,
    InitFetchDeliveryContext,
    init_fetch_context_from_payload,
)
from dpone_airflow_pack.init_fetch_validation import ENVIRONMENT_RE
from dpone_airflow_pack.runtime_artifact_delivery_contract import (
    validate_runtime_artifact_delivery,
)
from dpone_airflow_pack.semantic_refresh_index_artifacts import (
    SemanticRefreshDagProjectionArtifact,
    load_semantic_refresh_index_artifacts,
)

INDEX_SCHEMA_V1 = "dpone.airflow-deployment-index.v1"
INDEX_SCHEMA_V2 = AIRFLOW_INDEX_SCHEMA_V2
INDEX_SCHEMA_V3 = AIRFLOW_INDEX_SCHEMA_V3
INDEX_SCHEMA = INDEX_SCHEMA_V1
_INDEX_SCHEMAS = frozenset({INDEX_SCHEMA_V1, INDEX_SCHEMA_V2, INDEX_SCHEMA_V3})
_STRICT_INDEX_SCHEMAS = frozenset({INDEX_SCHEMA_V2, INDEX_SCHEMA_V3})
DEFAULT_MAX_INDEX_BYTES = 8 * 1024 * 1024
_legacy_missing_bytes_warning_pid: int | None = None


@dataclass(frozen=True)
class AirflowDeploymentIndex:
    path: Path
    cache_root: Path
    release_id: str
    deployment_id: str
    dag_specs: tuple[AirflowIndexArtifact, ...]
    workload_packs: tuple[AirflowIndexArtifact, ...]
    semantic_refresh_dag_projections: tuple[SemanticRefreshDagProjectionArtifact, ...] = ()
    airflow_index_sha256: str | None = None
    activation_id: str | None = None
    workspace_authority_connection_ref: str | None = None
    binding_set_ref: str | None = None
    connection_registry_ref: str | None = None
    credential_runtime_ref: str | None = None
    runtime_image_digest: str | None = None
    airflow_bundle_ref: str | None = None
    runtime_artifact_delivery: Mapping[str, Any] | None = None
    max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES
    schema: str = INDEX_SCHEMA_V1
    runtime_image_ref: str | None = None
    delivery_context: InitFetchDeliveryContext | None = None
    composition_supervisor: Mapping[str, object] | None = None


def load_airflow_deployment_index(
    index_path: str | Path,
    *,
    cache_root: str | Path | None = None,
    max_index_bytes: int = DEFAULT_MAX_INDEX_BYTES,
    max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
) -> AirflowDeploymentIndex:
    """Load one index and verify every referenced artifact."""

    return _load_airflow_deployment_index(
        index_path,
        cache_root=cache_root,
        max_index_bytes=max_index_bytes,
        max_artifact_bytes=max_artifact_bytes,
        verify_artifacts=True,
    )


def _load_airflow_deployment_index_descriptor(
    index_path: str | Path,
    *,
    cache_root: str | Path | None = None,
    max_index_bytes: int = DEFAULT_MAX_INDEX_BYTES,
    max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
) -> AirflowDeploymentIndex:
    """Load one bounded descriptor for isolated final-consumer verification."""

    return _load_airflow_deployment_index(
        index_path,
        cache_root=cache_root,
        max_index_bytes=max_index_bytes,
        max_artifact_bytes=max_artifact_bytes,
        verify_artifacts=False,
    )


def _load_airflow_deployment_index(
    index_path: str | Path,
    *,
    cache_root: str | Path | None,
    max_index_bytes: int,
    max_artifact_bytes: int,
    verify_artifacts: bool,
) -> AirflowDeploymentIndex:
    """Parse one bounded deployment index with an explicit trust boundary."""

    lexical_path = Path(index_path).absolute()
    root = Path(cache_root).resolve(strict=False) if cache_root is not None else infer_cache_root(lexical_path)
    path = lexical_path
    payload, activation, airflow_index_sha256 = _load_json_object(
        path,
        cache_root=root,
        max_bytes=max_index_bytes,
    )
    schema = payload.get("schema")
    if not isinstance(schema, str) or schema not in _INDEX_SCHEMAS:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_SCHEMA_INVALID",
            f"Expected schema {INDEX_SCHEMA_V1}, {INDEX_SCHEMA_V2}, or {INDEX_SCHEMA_V3}",
            path=path.as_posix(),
        )
    composition_supervisor = _composition_supervisor_from_payload(payload, path=path)
    release = required_sha256(payload, "release_id", path)
    deployment = required_sha256(payload, "deployment_id", path)
    runtime_artifact_delivery = validate_runtime_artifact_delivery(payload, path=path)
    strict_index = schema in _STRICT_INDEX_SCHEMAS
    delivery_context = init_fetch_context_from_payload(payload, path=path) if strict_index else None
    _validate_current_activation_identity(
        activation,
        payload=payload,
        deployment_id=deployment,
        strict_v2=strict_index,
        delivery_context=delivery_context,
        path=path,
    )
    _validate_index_artifact_ids(payload, path=path)
    return AirflowDeploymentIndex(
        path=path,
        cache_root=root,
        release_id=release,
        deployment_id=deployment,
        airflow_index_sha256=airflow_index_sha256,
        activation_id=activation.activation_id if activation is not None else None,
        workspace_authority_connection_ref=(
            activation.workspace_authority_connection_ref if activation is not None else None
        ),
        dag_specs=load_index_artifacts(
            payload,
            "dag_specs",
            release_id=release,
            cache_root=root,
            max_artifact_bytes=max_artifact_bytes,
            verify_artifacts=verify_artifacts,
            strict_v2=strict_index,
            workload_packs=False,
            warn_legacy_missing_bytes=_warn_legacy_missing_bytes_once,
            path=path,
        ),
        workload_packs=load_index_artifacts(
            payload,
            "workload_packs",
            release_id=release,
            cache_root=root,
            max_artifact_bytes=max_artifact_bytes,
            verify_artifacts=verify_artifacts,
            strict_v2=strict_index,
            workload_packs=True,
            warn_legacy_missing_bytes=_warn_legacy_missing_bytes_once,
            path=path,
        ),
        semantic_refresh_dag_projections=load_semantic_refresh_index_artifacts(
            payload,
            release_id=release,
            deployment_id=deployment,
            environment=(delivery_context.environment if delivery_context is not None else ""),
            cache_root=root,
            max_artifact_bytes=max_artifact_bytes,
            verify_artifacts=verify_artifacts,
            path=path,
        )
        if strict_index
        else (),
        binding_set_ref=optional_sha256(payload.get("binding_set_ref"), "binding_set_ref", path),
        connection_registry_ref=optional_sha256(
            payload.get("connection_registry_ref"),
            "connection_registry_ref",
            path,
        ),
        credential_runtime_ref=optional_sha256(
            payload.get("credential_runtime_ref"),
            "credential_runtime_ref",
            path,
        ),
        runtime_image_digest=optional_sha256(
            payload.get("runtime_image_digest"),
            "runtime_image_digest",
            path,
        ),
        airflow_bundle_ref=optional_text(payload.get("airflow_bundle_ref"), "airflow_bundle_ref", path),
        runtime_artifact_delivery=runtime_artifact_delivery,
        max_artifact_bytes=max_artifact_bytes,
        schema=str(schema),
        runtime_image_ref=optional_text(payload.get("runtime_image_ref"), "runtime_image_ref", path),
        delivery_context=delivery_context,
        composition_supervisor=composition_supervisor,
    )


def _composition_supervisor_from_payload(
    payload: Mapping[str, Any],
    *,
    path: Path,
) -> Mapping[str, object] | None:
    value = payload.get("composition_supervisor")
    schema = payload.get("schema")
    if value is None:
        return None
    if schema != INDEX_SCHEMA_V3:
        raise AirflowDeploymentIndexError(
            "DPONE_COMPOSITION_SUPERVISOR_FORBIDDEN",
            "composition supervisor is forbidden on v1/v2 deployment indexes",
            path=path.as_posix(),
        )
    try:
        projection = parse_composition_supervisor(value)
    except ValueError as exc:
        raise _invalid_composition_supervisor(path) from exc
    if projection is None:
        raise _invalid_composition_supervisor(path)
    return projection.to_dict()


def _invalid_composition_supervisor(path: Path) -> AirflowDeploymentIndexError:
    return AirflowDeploymentIndexError(
        "DPONE_COMPOSITION_SUPERVISOR_INVALID",
        "composition supervisor deployment capability is invalid",
        path=path.as_posix(),
    )


def _load_json_object(
    path: Path,
    *,
    cache_root: Path,
    max_bytes: int,
) -> tuple[dict[str, Any], CacheActivationIdentity | None, str]:
    if max_bytes <= 0:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_SIZE_LIMIT_INVALID",
            "airflow deployment index size limit must be positive",
            path=path.as_posix(),
        )
    try:
        confined_read = read_confined_cache_file_with_identity(
            path,
            cache_root=cache_root,
            max_bytes=max_bytes,
            allow_current_pointer=True,
        )
    except AirflowDeploymentIndexError as exc:
        mapped_code, message = _index_read_error(exc.code)
        raise AirflowDeploymentIndexError(mapped_code, message, path=path.as_posix()) from exc
    try:
        text = confined_read.content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_JSON_INVALID",
            "airflow deployment index must be UTF-8 JSON",
            path=path.as_posix(),
        ) from exc
    try:
        payload = json.loads(
            text,
            object_pairs_hook=_unique_json_object,
            parse_constant=_parse_finite_json_number,
            parse_float=_parse_finite_json_number,
        )
    except (ValueError, RecursionError) as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_JSON_INVALID",
            "airflow deployment index must be strict UTF-8 JSON",
            path=path.as_posix(),
        ) from exc
    if not isinstance(payload, dict):
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_PAYLOAD_INVALID",
            "airflow deployment index must be a JSON object",
            path=path.as_posix(),
        )
    digest = "sha256:" + hashlib.sha256(confined_read.content).hexdigest()
    return payload, confined_read.activation, digest


def _unique_json_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
    payload = dict(items)
    if len(payload) != len(items):
        raise ValueError("duplicate JSON key")
    return payload


def _parse_finite_json_number(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("non-finite JSON number")
    return parsed


def _validate_index_artifact_ids(payload: Mapping[str, Any], *, path: Path) -> None:
    """Reject ambiguous logical identities before any indexed artifact read."""

    for key in ("dag_specs", "workload_packs"):
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
        seen_ids: set[str] = set()
        for item in raw:
            if not isinstance(item, Mapping):
                raise AirflowDeploymentIndexError(
                    "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
                    "artifact must be an object",
                    path=path.as_posix(),
                )
            artifact_id = item.get("id")
            if not isinstance(artifact_id, str) or not artifact_id:
                raise AirflowDeploymentIndexError(
                    "DPONE_AIRFLOW_INDEX_FIELD_MISSING",
                    "id is required",
                    path=path.as_posix(),
                )
            if artifact_id in seen_ids:
                raise AirflowDeploymentIndexError(
                    "DPONE_AIRFLOW_INDEX_ARTIFACT_DUPLICATE",
                    f"{key} contains duplicate artifact ids",
                    path=path.as_posix(),
                )
            seen_ids.add(artifact_id)


def _validate_current_activation_identity(
    activation: CacheActivationIdentity | None,
    *,
    payload: Mapping[str, Any],
    deployment_id: str,
    strict_v2: bool,
    delivery_context: InitFetchDeliveryContext | None,
    path: Path,
) -> None:
    if activation is None:
        return
    if strict_v2 and activation.activation_id is None:
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_CURRENT_IDENTITY_MISMATCH",
            "strict v2 current activation requires a canonical activation_id",
            path=path.as_posix(),
        )
    if delivery_context is not None:
        index_environment: object = delivery_context.environment
    else:
        candidate = payload.get("environment")
        index_environment = candidate if isinstance(candidate, str) and ENVIRONMENT_RE.fullmatch(candidate) else None
    if (
        activation.deployment_id != deployment_id
        or (activation.release_id is not None and activation.release_id != payload.get("release_id"))
        or index_environment != activation.environment
    ):
        raise AirflowDeploymentIndexError(
            "DPONE_AIRFLOW_INDEX_CURRENT_IDENTITY_MISMATCH",
            "current activation identity does not match the airflow deployment index",
            path=path.as_posix(),
        )


def _index_read_error(code: str) -> tuple[str, str]:
    if code == "DPONE_CACHE_ARTIFACT_MISSING":
        return "DPONE_AIRFLOW_INDEX_NOT_FOUND", "airflow deployment index does not exist"
    if code == "DPONE_CACHE_ARTIFACT_TOO_LARGE":
        return "DPONE_AIRFLOW_INDEX_TOO_LARGE", "airflow deployment index exceeds configured size limit"
    if code == "DPONE_CACHE_ARTIFACT_SIZE_LIMIT_INVALID":
        return "DPONE_AIRFLOW_INDEX_SIZE_LIMIT_INVALID", "airflow deployment index size limit must be positive"
    return "DPONE_AIRFLOW_INDEX_READ_FAILED", "airflow deployment index could not be read safely"


def _warn_legacy_missing_bytes_once() -> None:
    global _legacy_missing_bytes_warning_pid

    pid = os.getpid()
    if _legacy_missing_bytes_warning_pid == pid:
        return
    _legacy_missing_bytes_warning_pid = pid
    warnings.warn(
        "Legacy dpone.airflow-deployment-index.v1 artifact without 'bytes' is "
        "deprecated; regenerate the deployment index.",
        DeprecationWarning,
        stacklevel=4,
    )


__all__ = [
    "AirflowDeploymentIndex",
    "AirflowDeploymentIndexError",
    "AirflowIndexArtifact",
    "DEFAULT_MAX_ARTIFACT_BYTES",
    "DEFAULT_MAX_INDEX_BYTES",
    "INDEX_SCHEMA",
    "INDEX_SCHEMA_V1",
    "INDEX_SCHEMA_V2",
    "_is_canonical_sha256_digest",
    "infer_cache_root",
    "load_airflow_deployment_index",
    "resolve_cache_artifact",
    "verify_airflow_index_artifact",
]
