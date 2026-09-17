"""Identity and schema admission for remote Airflow artifact roots."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.contracts.airflow_deployment_projection import deployment_projection_violation
from dpone.runtime.airflow_artifact_delivery_models import AirflowArtifactDeliveryError, MaterializeRequest


def validate_remote_artifact_headers(
    request: MaterializeRequest,
    *,
    release: Mapping[str, Any],
    deployment: Mapping[str, Any],
    index: Mapping[str, Any],
) -> None:
    """Validate exact remote roots before downloading executable artifacts."""

    if (
        release.get("schema")
        not in {
            "dpone.release-set.v1",
            "dpone.release-set.v2",
            "dpone.dbt-release-set.development.v1",
            "dpone.release-set.v3",
        }
        or release.get("release_id") != request.release_id
    ):
        raise AirflowArtifactDeliveryError("DPONE_RELEASE_ID_MISMATCH", "remote release identity is invalid")
    if compute_release_id(release) != request.release_id:
        raise AirflowArtifactDeliveryError(
            "DPONE_RELEASE_FINGERPRINT_MISMATCH",
            "remote release content does not match its identity",
        )
    schema_pair = (deployment.get("schema"), index.get("schema"))
    if schema_pair not in {
        ("dpone.deployment-set.v1", "dpone.airflow-deployment-index.v1"),
        ("dpone.deployment-set.v2", "dpone.airflow-deployment-index.v2"),
        ("dpone.deployment-set.v3", "dpone.airflow-deployment-index.v3"),
    }:
        raise AirflowArtifactDeliveryError("DPONE_DEPLOYMENT_SCHEMA_INVALID", "remote deployment schema is invalid")
    if deployment.get("environment") != request.environment:
        raise AirflowArtifactDeliveryError(
            "DPONE_DEPLOYMENT_ENVIRONMENT_MISMATCH",
            "remote deployment environment does not match the request",
        )
    violation = deployment_projection_violation(deployment, index)
    if violation is not None:
        raise AirflowArtifactDeliveryError(violation.code, violation.message)
    if deployment.get("deployment_id") != request.deployment_id or deployment.get("release_ref") != request.release_id:
        raise AirflowArtifactDeliveryError(
            "DPONE_DEPLOYMENT_ID_MISMATCH",
            "remote deployment does not match the requested pins",
        )


__all__ = ["validate_remote_artifact_headers"]
