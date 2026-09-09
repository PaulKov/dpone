"""Derive observed deployment subjects from verified local artifact bytes."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.runtime.runtime_init_fetch_attestation import StagedRuntimeArtifact

from dpone.ports.airflow_deployment_attestation import (
    AirflowArtifactExpectedSubject,
    AirflowArtifactObservedSubject,
    sha256_bytes,
)
from dpone.runtime.deployment_cache_common import DeploymentCacheError, open_regular_file
from dpone.runtime.deployment_cache_models import ValidatedDeploymentProjection
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import (
    RuntimeInitFetchPlan,
    plan_matches_declared_runtime_image,
)

_MAX_ROOT_BYTES = 16 * 1024 * 1024


def read_airflow_artifact_control_roots(
    *,
    cache_root: Path,
    release_id: str,
    deployment_id: str,
    environment: str,
) -> tuple[bytes, bytes, bytes]:
    """Read the exact three bounded control roots from one local projection."""

    deployment_root = cache_root / "deployments" / environment / _digest_dir(deployment_id)
    release_root = cache_root / "releases" / _digest_dir(release_id)
    return (
        _read_regular(release_root / "release-set.json", root=cache_root),
        _read_regular(deployment_root / "deployment.json", root=cache_root),
        _read_regular(deployment_root / "airflow-index.json", root=cache_root),
    )


def subject_from_cache_projection(
    *,
    cache_root: Path,
    projection: ValidatedDeploymentProjection,
    registry_scope_id: str,
) -> AirflowArtifactObservedSubject:
    """Bind the exact three verified control roots in a staged cache projection."""

    release_bytes, deployment_bytes, index_bytes = read_airflow_artifact_control_roots(
        cache_root=cache_root,
        release_id=projection.release_id,
        deployment_id=projection.deployment_id,
        environment=str(projection.deployment["environment"]),
    )
    return AirflowArtifactObservedSubject.from_expected(
        _subject(
            release_bytes=release_bytes,
            deployment_bytes=deployment_bytes,
            index_bytes=index_bytes,
            release_id=projection.release_id,
            deployment_id=projection.deployment_id,
            environment=str(projection.deployment["environment"]),
            artifact_registry_ref=_registry_ref(projection.deployment),
            registry_scope_id=registry_scope_id,
            runtime_image_digest=_runtime_image_digest(projection.deployment),
        )
    )


def subject_from_runtime_artifacts(
    *,
    plan: RuntimeInitFetchPlan,
    staged_artifacts: Sequence[StagedRuntimeArtifact],
    registry_scope_id: str,
) -> AirflowArtifactObservedSubject:
    """Bind only control roots independently present in the runtime plan.

    Dual-digest deployments may pin the ``runtime_image_dbt_*`` flavor in the
    plan while the signed attestation subject always commits to the canonical
    ``runtime_image_digest`` declared by the deployment-set. The plan image is
    therefore validated as a (ref, digest) pair against one declared flavor,
    and the observed subject binds the canonical digest read from the verified
    deployment bytes so it matches the signed claims.
    """

    staged = {item.descriptor.artifact_ref: item.path for item in staged_artifacts}
    # The shared read/parse helpers keep their ValueError contract for the
    # scheduler-side cache-projection path; only coded InitFetchError may
    # cross the runtime CLI boundary, so convert at this boundary instead.
    try:
        release_bytes = _read_regular(staged[plan.release.artifact_ref], root=staged[plan.release.artifact_ref].parent)
        deployment_bytes = _read_regular(
            staged[plan.deployment.artifact_ref],
            root=staged[plan.deployment.artifact_ref].parent,
        )
        deployment = _strict_json(deployment_bytes, "deployment")
        registry_ref = _registry_ref(deployment)
        canonical_image_digest = _runtime_image_digest(deployment)
    except KeyError as exc:
        raise _integrity_error("runtime attestation roots are missing from the staged artifact set") from exc
    except DeploymentCacheError as exc:
        raise InitFetchError(exc.code, str(exc)) from exc
    except ValueError as exc:
        raise _integrity_error(str(exc)) from exc
    if (
        deployment.get("release_ref") != plan.release_id
        or deployment.get("deployment_id") != plan.deployment_id
        or deployment.get("environment") != plan.environment
        or registry_ref != plan.artifact_registry_ref
    ):
        raise _integrity_error("runtime attestation roots do not match the pinned init-fetch plan")
    if not plan_matches_declared_runtime_image(plan, deployment):
        raise _integrity_error(
            "runtime attestation image pair does not match one declared deployment flavor: "
            f"plan=({plan.runtime_image_ref}, {plan.runtime_image_digest}) "
            "declared_canonical="
            f"({deployment.get('runtime_image_ref')}, {deployment.get('runtime_image_digest')}) "
            "declared_dbt="
            f"({deployment.get('runtime_image_dbt_ref')}, {deployment.get('runtime_image_dbt_digest')})",
        )
    return AirflowArtifactObservedSubject(
        release_id=plan.release_id,
        deployment_id=plan.deployment_id,
        environment=plan.environment,
        artifact_registry_ref=plan.artifact_registry_ref,
        registry_scope_id=registry_scope_id,
        release_set_sha256=sha256_bytes(release_bytes),
        deployment_sha256=sha256_bytes(deployment_bytes),
        runtime_image_digest=canonical_image_digest,
        airflow_index_sha256=None,
    )


def _subject(
    *,
    release_bytes: bytes,
    deployment_bytes: bytes,
    index_bytes: bytes,
    release_id: str,
    deployment_id: str,
    environment: str,
    artifact_registry_ref: str,
    registry_scope_id: str,
    runtime_image_digest: str,
) -> AirflowArtifactExpectedSubject:
    return AirflowArtifactExpectedSubject(
        release_id=release_id,
        deployment_id=deployment_id,
        environment=environment,
        artifact_registry_ref=artifact_registry_ref,
        registry_scope_id=registry_scope_id,
        release_set_sha256=sha256_bytes(release_bytes),
        deployment_sha256=sha256_bytes(deployment_bytes),
        airflow_index_sha256=sha256_bytes(index_bytes),
        runtime_image_digest=runtime_image_digest,
    )


def _registry_ref(deployment: Mapping[str, Any]) -> str:
    delivery = deployment.get("runtime_artifact_delivery")
    value = delivery.get("artifact_registry_ref") if isinstance(delivery, Mapping) else None
    if not isinstance(value, str) or not value:
        raise ValueError("deployment artifact registry reference is missing")
    return value


def _runtime_image_digest(deployment: Mapping[str, Any]) -> str:
    value = deployment.get("runtime_image_digest")
    if not isinstance(value, str) or not value:
        raise ValueError("deployment runtime image digest is missing")
    return value


def _read_regular(path: Path, *, root: Path) -> bytes:
    descriptor = open_regular_file(
        path,
        missing_code="DPONE_ARTIFACT_ATTESTATION_INVALID",
        invalid_code="DPONE_ARTIFACT_ATTESTATION_INVALID",
        label="artifact attestation subject root",
        root=root,
    )
    chunks: list[bytes] = []
    total = 0
    try:
        while total <= _MAX_ROOT_BYTES:
            chunk = os.read(descriptor, min(64 * 1024, _MAX_ROOT_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
    finally:
        os.close(descriptor)
    if total > _MAX_ROOT_BYTES:
        raise ValueError("artifact attestation subject root exceeds the byte limit")
    return b"".join(chunks)


def _strict_json(raw: bytes, label: str) -> dict[str, Any]:
    def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"{label} contains duplicate keys")
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} must be valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _integrity_error(message: str) -> InitFetchError:
    return InitFetchError("DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED", message)


def _digest_dir(value: str) -> str:
    return value.replace(":", "-", 1)


__all__ = [
    "read_airflow_artifact_control_roots",
    "subject_from_cache_projection",
    "subject_from_runtime_artifacts",
]
