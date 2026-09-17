"""Local projection admission and inventory preparation for publication."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from dpone.runtime.airflow_artifact_delivery_models import (
    AirflowArtifactDeliveryError,
    ArtifactInventory,
    PublishRequest,
)
from dpone.runtime.airflow_artifact_delivery_support import (
    DevelopmentTargetAdmission,
    DevelopmentTargetAdmissionVerifier,
    from_cache_error,
    require_development_delivery_authority,
    require_registry_ref,
)
from dpone.runtime.airflow_artifact_inventory import build_publish_inventory
from dpone.runtime.deployment_cache_common import DeploymentCacheError, read_regular_json_object
from dpone.runtime.deployment_cache_models import ValidatedDeploymentProjection
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator


def prepare_publication(
    request: PublishRequest,
    *,
    development_admission: DevelopmentTargetAdmission | None = None,
    development_admission_verifier: DevelopmentTargetAdmissionVerifier | None = None,
    checked_at: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
) -> ArtifactInventory:
    """Validate and inventory all local bytes without registry or credential I/O."""

    projection = _validate_local_projection(request)
    require_registry_ref(projection.deployment, projection.airflow_index, request.artifact_registry_ref)
    try:
        release = read_regular_json_object(
            request.cache_root / "releases" / request.release_dir_name / "release-set.json",
            missing_code="DPONE_RELEASE_NOT_FOUND",
            invalid_code="DPONE_RELEASE_INVALID",
            label="release-set",
            root=request.cache_root,
        )
        require_development_delivery_authority(
            release,
            deployment=projection.deployment,
            admission=development_admission,
            admission_verifier=development_admission_verifier,
            operation="publish",
            checked_at=_current_time(checked_at=checked_at, clock=clock),
        )
        inventory = build_publish_inventory(request, projection)
        require_development_delivery_authority(
            release,
            deployment=projection.deployment,
            admission=development_admission,
            admission_verifier=development_admission_verifier,
            operation="publish",
            checked_at=_current_time(checked_at=checked_at, clock=clock),
            source_bytes=sum(item.size_bytes for item in inventory.release if not item.completion_marker),
        )
        return inventory
    except DeploymentCacheError as exc:
        raise from_cache_error(exc) from exc


def _current_time(*, checked_at: datetime | None, clock: Callable[[], datetime] | None) -> datetime | None:
    return clock() if clock is not None else checked_at


def require_exact_publication_projection(
    request: PublishRequest,
    projection: ValidatedDeploymentProjection,
) -> None:
    """Require v2/v3 deployment roots for exact publication mode."""

    if request.publication_mode != "exact":
        return
    if (projection.deployment.get("schema"), projection.airflow_index.get("schema")) not in {
        ("dpone.deployment-set.v2", "dpone.airflow-deployment-index.v2"),
        ("dpone.deployment-set.v3", "dpone.airflow-deployment-index.v3"),
    }:
        raise AirflowArtifactDeliveryError(
            "DPONE_EXACT_PUBLICATION_PROJECTION_REQUIRED",
            "exact publication requires matching deployment-set/index v2 or v3",
        )


def _validate_local_projection(request: PublishRequest) -> ValidatedDeploymentProjection:
    deployment_dir = request.cache_root / "deployments" / request.environment / request.deployment_dir_name
    try:
        projection = DeploymentCacheProjectionValidator(
            request.cache_root,
            max_artifact_bytes=request.max_object_bytes,
        ).validate_details(deployment_dir, environment=request.environment)
    except DeploymentCacheError as exc:
        raise from_cache_error(exc) from exc
    if projection.release_id != request.release_id or projection.deployment_id != request.deployment_id:
        raise AirflowArtifactDeliveryError(
            "DPONE_DEPLOYMENT_ID_MISMATCH",
            "validated deployment does not match the requested release/deployment pins",
        )
    require_exact_publication_projection(request, projection)
    return projection


__all__ = ["prepare_publication", "require_exact_publication_projection"]
