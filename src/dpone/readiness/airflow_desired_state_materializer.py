"""Immutable deployment materialization adapter for desired-state reconcile."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.ports.airflow_desired_state import DesiredStateReconcilePortError
from dpone.runtime.airflow_artifact_delivery import (
    AirflowArtifactDeliveryError,
    AirflowArtifactMaterializer,
    ArtifactAttestationVerifier,
    MaterializeRequest,
)
from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

if TYPE_CHECKING:
    from dpone.contracts.airflow_desired_state import AirflowDesiredDeployment
    from dpone.ports.airflow_deployment_attestation import (
        AirflowDeploymentAttestationVerifier,
    )
    from dpone.ports.artifact_registry import ArtifactRegistry


class ArtifactRegistryDesiredDeploymentMaterializer:
    """Materialize and bind immutable cache bytes to desired-state evidence."""

    def __init__(
        self,
        *,
        cache_root: Path,
        artifact_registry_ref: str,
        registry: ArtifactRegistry,
        max_object_bytes: int,
        max_total_bytes: int,
        attestation_verifier: ArtifactAttestationVerifier | None = None,
        deployment_attestation_verifier: AirflowDeploymentAttestationVerifier | None = None,
    ) -> None:
        self._cache_root = cache_root
        self._artifact_registry_ref = artifact_registry_ref
        self._registry = registry
        self._max_object_bytes = max_object_bytes
        self._max_total_bytes = max_total_bytes
        self._attestation_verifier = attestation_verifier
        self._deployment_attestation_verifier = deployment_attestation_verifier

    def materialize(self, desired: AirflowDesiredDeployment) -> None:
        request = MaterializeRequest(
            cache_root=self._cache_root,
            release_id=desired.promotion.release_id,
            deployment_id=desired.promotion.deployment_id,
            environment=desired.environment,
            artifact_registry_ref=self._artifact_registry_ref,
            max_object_bytes=self._max_object_bytes,
            max_total_bytes=self._max_total_bytes,
        )
        try:
            AirflowArtifactMaterializer(
                registry=self._registry,
                attestation_verifier=self._attestation_verifier,
                deployment_attestation_verifier=self._deployment_attestation_verifier,
            ).materialize(request)
            _verify_desired_projection(request, desired=desired)
        except AirflowArtifactDeliveryError as exc:
            raise DesiredStateReconcilePortError(
                exc.code,
                "immutable Airflow deployment could not be materialized",
            ) from exc
        except (OSError, ValueError) as exc:
            raise DesiredStateReconcilePortError(
                "DPONE_AIRFLOW_DESIRED_STATE_DEPLOYMENT_MISMATCH",
                "materialized deployment does not match desired-state evidence",
            ) from exc


def _verify_desired_projection(
    request: MaterializeRequest,
    *,
    desired: AirflowDesiredDeployment,
) -> None:
    deployment_dir = request.cache_root / "deployments" / request.environment / request.deployment_dir_name
    index_bytes = (deployment_dir / "airflow-index.json").read_bytes()
    projection = DeploymentCacheMaterializer(request.cache_root).validate_details(
        deployment_dir,
        environment=request.environment,
    )
    index = projection.airflow_index
    dag_specs = index.get("dag_specs")
    dag_ids = (
        sorted(str(item.get("id")) for item in dag_specs if isinstance(item, Mapping))
        if isinstance(dag_specs, list)
        else []
    )
    if (
        "sha256:" + hashlib.sha256(index_bytes).hexdigest() != desired.promotion.airflow_index_sha256
        or tuple(dag_ids) != desired.promotion.expected_dag_ids
        or index.get("runtime_image_digest") != desired.promotion.runtime_image_digest
        or (
            desired.promotion.runtime_image_dbt_digest is not None
            and index.get("runtime_image_dbt_digest") != desired.promotion.runtime_image_dbt_digest
        )
        or index.get("airflow_bundle_ref") != f"git:{desired.source.git_sha}"
    ):
        raise ValueError("materialized deployment differs from desired-state evidence")


__all__ = ["ArtifactRegistryDesiredDeploymentMaterializer"]
