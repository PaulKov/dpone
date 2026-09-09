"""Validation and descriptor policy for Airflow deployment projections."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment import (
    canonical_fingerprint,
    is_canonical_sha256_digest,
    requires_release_set_v2_for_runtime_payloads,
)
from dpone.contracts.airflow_deployment_projection import (
    is_versioned_airflow_bundle_ref,
)
from dpone.contracts.runtime_artifact_delivery import (
    is_pinned_artifact_registry_ref,
    is_safe_artifact_registry_logical_ref,
    validate_runtime_image_reference,
)
from dpone.readiness.airflow_connection_runtime_registry import runtime_connection_snapshot_fingerprint
from dpone.readiness.airflow_deployment_artifacts import (
    bytes_descriptor,
    digest_dir,
)
from dpone.readiness.airflow_deployment_projection_errors import (
    AirflowDeploymentProjectionError,
)

_RUNTIME_CONNECTION_SNAPSHOT_FILES = {
    "binding_set": "binding-set.json",
    "connection_registry": "connection-registry.json",
    "credential_runtime": "credential-runtime.json",
}
_DNS_LABEL = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
_QUEUE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


def dev_evidence_delivery(
    *,
    trust_tier: str,
    claim_name: str | None,
    worker_queue: str | None,
) -> dict[str, str] | None:
    """Return a non-production-only, category-scoped PVC delivery contract."""

    if claim_name is None and worker_queue is None:
        return None
    if trust_tier != "non_production":
        raise AirflowDeploymentProjectionError(
            "DPONE_DBT_DEV_EVIDENCE_DELIVERY_INVALID",
            "dev evidence delivery is allowed only for non-production deployments",
        )
    if (
        not isinstance(claim_name, str)
        or len(claim_name) > 63
        or _DNS_LABEL.fullmatch(claim_name) is None
        or not isinstance(worker_queue, str)
        or _QUEUE.fullmatch(worker_queue) is None
    ):
        raise AirflowDeploymentProjectionError(
            "DPONE_DBT_DEV_EVIDENCE_DELIVERY_INVALID",
            "dev evidence PVC claim and dedicated worker queue are required and invalid",
        )
    return {
        "mode": "shared_pvc",
        "claim_name": claim_name,
        "mount_path": "/var/lib/dpone/dev-evidence",
        "worker_queue": worker_queue,
    }


def require_runtime_payload_authority(release_schema: str, has_runtime_payloads: bool, trust_tier: str) -> None:
    """Keep v1 payload inventory outside production dbt authority."""

    if requires_release_set_v2_for_runtime_payloads(
        release_schema=release_schema,
        has_runtime_payloads=has_runtime_payloads,
        trust_tier=trust_tier,
    ):
        raise AirflowDeploymentProjectionError(
            "DPONE_DBT_PRODUCTION_RELEASE_SCHEMA_REQUIRED",
            "production dbt runtime payloads require release-set.v2; rebuild with dpone dbt compile",
        )


def optional_runtime_image_fields(
    *,
    runtime_image_ref: str | None,
    runtime_image_digest: str | None,
) -> dict[str, str]:
    """Validate and return one optional exact dbt runtime-image pair."""

    if (runtime_image_digest is None) != (runtime_image_ref is None):
        raise AirflowDeploymentProjectionError(
            "DPONE_RUNTIME_IMAGE_DBT_PAIR_INVALID",
            "runtime_image_dbt_ref and runtime_image_dbt_digest must both be set or both be absent",
        )
    if runtime_image_digest is None:
        return {}
    require_digest("runtime_image_dbt_digest", runtime_image_digest)
    try:
        exact_ref = validate_runtime_image_reference(runtime_image_ref, runtime_image_digest)
    except ValueError as exc:
        raise AirflowDeploymentProjectionError("DPONE_RUNTIME_IMAGE_DBT_REF_INVALID", str(exc)) from exc
    return {
        "runtime_image_dbt_ref": exact_ref,
        "runtime_image_dbt_digest": runtime_image_digest,
    }


def runtime_connection_descriptors(
    snapshots: Mapping[str, bytes],
) -> dict[str, dict[str, Any]]:
    """Build content-addressed descriptors for runtime connection snapshots."""

    content_identity = canonical_fingerprint(
        {
            name: {
                "sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
                "bytes": len(payload),
            }
            for name, payload in snapshots.items()
        }
    )
    context_root = f"cache://runtime-connection-contexts/{digest_dir(content_identity)}"
    return {
        name: bytes_descriptor(
            artifact_ref=(f"{context_root}/{_RUNTIME_CONNECTION_SNAPSHOT_FILES[name]}"),
            payload=snapshots[name],
        )
        for name in _RUNTIME_CONNECTION_SNAPSHOT_FILES
    }


def runtime_connection_fingerprints(snapshots: Mapping[str, bytes]) -> tuple[str, str, str]:
    """Return binding, registry, and credential snapshot identities in contract order."""

    fingerprint = runtime_connection_snapshot_fingerprint
    return (
        fingerprint(snapshots["binding_set"]),
        fingerprint(snapshots["connection_registry"]),
        fingerprint(snapshots["credential_runtime"]),
    )


def workload_projection_inventory(workloads: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Project workload descriptors to the deployment identity fields."""

    return [
        {"id": item["id"], "sha256": item["sha256"], "pack_fingerprint": item["pack_fingerprint"]} for item in workloads
    ]


def require_digest(name: str, value: str) -> None:
    """Require one canonical SHA-256 identity."""

    if not is_canonical_sha256_digest(value):
        raise AirflowDeploymentProjectionError(
            "DPONE_DEPLOYMENT_DIGEST_INVALID",
            f"{name} must be a canonical sha256 digest",
        )


def require_artifact_registry_ref(value: str) -> None:
    """Require a pinned, bounded artifact registry logical reference."""

    if not is_pinned_artifact_registry_ref(value):
        raise AirflowDeploymentProjectionError(
            "DPONE_DEPLOYMENT_ARTIFACT_REGISTRY_UNPINNED",
            "artifact_registry_ref must not resolve current or latest",
        )
    if not is_safe_artifact_registry_logical_ref(value):
        raise AirflowDeploymentProjectionError(
            "DPONE_DEPLOYMENT_ARTIFACT_REGISTRY_INVALID",
            "artifact_registry_ref must be a bounded logical name",
        )


def require_versioned_airflow_bundle_ref(value: object) -> None:
    """Require the production-supported versioned Git DAG bundle identity."""

    if not is_versioned_airflow_bundle_ref(value):
        raise AirflowDeploymentProjectionError(
            "DPONE_DEPLOYMENT_AIRFLOW_BUNDLE_UNPINNED",
            "runnable deployment-set v2 requires airflow_bundle_ref git:<40-hex-commit>",
        )


def require_legacy_artifact_registry_ref(value: str) -> None:
    """Preserve the local-safe-sample v1 pinned-reference rule."""

    if not is_pinned_artifact_registry_ref(value):
        raise AirflowDeploymentProjectionError(
            "DPONE_DEPLOYMENT_ARTIFACT_REGISTRY_UNPINNED",
            "artifact_registry_ref must not resolve current or latest",
        )


def confined_cache_root(
    root: Path,
    configured: str | Path | None,
) -> Path:
    """Resolve a deployment cache root confined to the project root."""

    candidate = Path(configured) if configured is not None else root / ".dpone-cache"
    if not candidate.is_absolute():
        candidate = root / candidate
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError("deployment cache root must stay inside the project root") from exc
    return resolved


__all__ = [
    "confined_cache_root",
    "dev_evidence_delivery",
    "optional_runtime_image_fields",
    "require_artifact_registry_ref",
    "require_digest",
    "require_legacy_artifact_registry_ref",
    "require_runtime_payload_authority",
    "require_versioned_airflow_bundle_ref",
    "runtime_connection_descriptors",
    "runtime_connection_fingerprints",
    "workload_projection_inventory",
]
