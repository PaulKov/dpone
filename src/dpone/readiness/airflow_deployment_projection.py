"""Build-plane materialization of environment Airflow deployment projections."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.contracts.composition_supervisor import (
    CompositionSupervisorProjection,
    supervisor_projection_for_release,
)
from dpone.manifest.confined_files import read_confined_file
from dpone.readiness.airflow_connection_runtime_registry import (
    runtime_connection_snapshots as build_runtime_connection_snapshots,
)
from dpone.readiness.airflow_deployment_artifacts import (
    bytes_descriptor,
    digest_dir,
    json_bytes,
    load_local_safe_sample_v1_inputs,
    load_strict_projection_inputs,
    normalize_environment_segment,
)
from dpone.readiness.airflow_deployment_projection_errors import (
    AirflowDeploymentProjectionError,
)
from dpone.readiness.airflow_deployment_projection_io import publish_immutable_projection
from dpone.readiness.airflow_deployment_projection_models import (
    AirflowDeploymentProjection,
    compute_deployment_id,
    compute_release_id,
    deployment_payload_fingerprint,
)
from dpone.readiness.airflow_deployment_projection_policy import (
    confined_cache_root,
    optional_runtime_image_fields,
    require_artifact_registry_ref,
    require_digest,
    require_legacy_artifact_registry_ref,
    require_runtime_payload_authority,
    require_versioned_airflow_bundle_ref,
    runtime_connection_fingerprints,
    workload_projection_inventory,
)
from dpone.readiness.airflow_deployment_projection_policy import (
    dev_evidence_delivery as build_dev_evidence_delivery,
)
from dpone.readiness.airflow_deployment_projection_policy import (
    runtime_connection_descriptors as build_runtime_connection_descriptors,
)
from dpone.readiness.airflow_deployment_projection_writer import write_projection
from dpone.readiness.airflow_init_fetch_projection import (
    InitFetchProjectionContractError,
    build_init_fetch_delivery,
    build_local_safe_sample_v1_projection,
)
from dpone.readiness.airflow_mssql_deployment_index import (
    build_environment_deployment_documents,
)
from dpone.readiness.airflow_mssql_outlet_projection import (
    build_mssql_asset_outlet_projection,
    load_pack_payloads_from_descriptors,
)
from dpone.readiness.airflow_semantic_refresh_projection import (
    SemanticRefreshDagSidecarFactory,
)
from dpone.runtime.deployment_cache_common import DeploymentCacheError, promotion_lock

if TYPE_CHECKING:
    from dpone.readiness.airflow_deployment_artifacts import DeploymentProjectionInputs


class AirflowDeploymentProjectionService:
    """Materialize one immutable environment deployment projection.

    The service reads local release/environment files and writes a deployment
    directory that can later be promoted by the cache materializer. It never
    reads secret values, Vault, Airflow Connections, Kubernetes, object storage,
    databases, or Airflow metadata.
    """

    def __init__(
        self,
        *,
        root: str | Path = ".",
        cache_root: str | Path | None = None,
    ) -> None:
        self._root = Path(root).resolve(strict=False)
        self._cache_root = confined_cache_root(self._root, cache_root)

    def materialize(
        self,
        *,
        release_id: str,
        environment: str,
        trust_tier: str,
        runtime_image_digest: str,
        runtime_image_ref: str | None = None,
        runtime_image_dbt_digest: str | None = None,
        runtime_image_dbt_ref: str | None = None,
        artifact_registry_ref: str,
        registry_config_ref: Mapping[str, Any] | None = None,
        trust_policy_ref: Mapping[str, Any] | None = None,
        airflow_bundle_ref: str | None = None,
        dev_evidence_pvc_claim: str | None = None,
        dev_evidence_worker_queue: str | None = None,
        composition_supervisor: Mapping[str, object] | None = None,
        semantic_refresh_sidecars: SemanticRefreshDagSidecarFactory | None = None,
    ) -> AirflowDeploymentProjection:
        """Materialize a strict executable v2 environment projection."""

        environment = normalize_environment_segment(environment)
        require_digest("release_id", release_id)
        require_digest("runtime_image_digest", runtime_image_digest)
        runtime_image_dbt_fields = optional_runtime_image_fields(
            runtime_image_ref=runtime_image_dbt_ref,
            runtime_image_digest=runtime_image_dbt_digest,
        )
        require_artifact_registry_ref(artifact_registry_ref)
        require_versioned_airflow_bundle_ref(airflow_bundle_ref)
        inputs = load_strict_projection_inputs(
            root=self._root,
            cache_root=self._cache_root,
            release_id=release_id,
            environment=environment,
        )
        supervisor_projection = _supervisor_projection(
            release_schema=inputs.release_schema,
            value=composition_supervisor,
        )
        if not inputs.workload_packs:
            raise AirflowDeploymentProjectionError(
                "DPONE_DEPLOYMENT_WORKLOADS_EMPTY",
                "strict init_fetch requires at least one workload pack",
            )
        try:
            exact_runtime_image_ref, runtime_delivery = build_init_fetch_delivery(
                trust_tier=trust_tier,
                binding_set=inputs.binding_set,
                runtime_image_ref=runtime_image_ref,
                runtime_image_digest=runtime_image_digest,
                artifact_registry_ref=artifact_registry_ref,
                registry_config_ref=registry_config_ref,
                trust_policy_ref=trust_policy_ref,
            )
        except InitFetchProjectionContractError as exc:
            raise AirflowDeploymentProjectionError(exc.code, str(exc)) from exc
        normalized_trust_tier = str(runtime_delivery["trust_tier"])
        require_runtime_payload_authority(inputs.release_schema, bool(inputs.runtime_payloads), normalized_trust_tier)
        dev_evidence_delivery = build_dev_evidence_delivery(
            trust_tier=normalized_trust_tier,
            claim_name=dev_evidence_pvc_claim,
            worker_queue=dev_evidence_worker_queue,
        )
        runtime_connection_snapshots = build_runtime_connection_snapshots(
            binding_set=inputs.binding_set,
            connection_registry=inputs.connection_registry,
            credential_runtime=inputs.credential_runtime,
        )
        # Authority must fingerprint the published RuntimeConnectionContext
        # payloads (rewritten registry), not the Git/source registry identity.
        binding_set_ref, connection_registry_ref, credential_runtime_ref = runtime_connection_fingerprints(
            runtime_connection_snapshots
        )
        runtime_connection_descriptors = build_runtime_connection_descriptors(runtime_connection_snapshots)
        workload_inventory = workload_projection_inventory(inputs.workload_packs)
        pack_payloads = load_pack_payloads_from_descriptors(
            inputs.workload_packs,
            cache_root=self._cache_root,
            reader=read_confined_file,
        )
        mssql_outlet_projection = build_mssql_asset_outlet_projection(
            environment=environment,
            binding_set=inputs.binding_set,
            binding_set_ref=binding_set_ref,
            connection_registry=inputs.connection_registry,
            connection_registry_ref=connection_registry_ref,
            workload_packs=inputs.workload_packs,
            pack_payloads=pack_payloads,
        )
        deployment, deployment_bytes, airflow_index = build_environment_deployment_documents(
            environment=environment,
            release_id=release_id,
            trust_tier=normalized_trust_tier,
            binding_set_ref=binding_set_ref,
            connection_registry_ref=connection_registry_ref,
            credential_runtime_ref=credential_runtime_ref,
            runtime_connection_descriptors=runtime_connection_descriptors,
            runtime_image_ref=exact_runtime_image_ref,
            runtime_image_digest=runtime_image_digest,
            runtime_image_dbt_fields=runtime_image_dbt_fields,
            airflow_bundle_ref=airflow_bundle_ref,
            runtime_delivery=runtime_delivery,
            dev_evidence_delivery=dev_evidence_delivery,
            workload_inventory=workload_inventory,
            dag_specs=inputs.dag_specs,
            workload_packs=inputs.workload_packs,
            runtime_payloads=inputs.runtime_payloads,
            release_bytes=inputs.release_bytes,
            mssql_outlet_projection=mssql_outlet_projection,
        )
        if supervisor_projection is not None:
            deployment_bytes = _seal_composition_supervisor_projection(
                deployment=deployment,
                airflow_index=airflow_index,
                projection=supervisor_projection,
            )
        sidecar_files: dict[str, bytes] = {}
        if semantic_refresh_sidecars is not None:
            sidecars = semantic_refresh_sidecars.build(
                release_id=release_id,
                deployment_id=str(deployment["deployment_id"]),
            )
            descriptors: list[dict[str, object]] = []
            for sidecar in sidecars:
                filename, descriptor = sidecar.index_descriptor(
                    release_id=release_id,
                    deployment_id=str(deployment["deployment_id"]),
                    environment=environment,
                )
                if filename in sidecar_files:
                    raise AirflowDeploymentProjectionError(
                        "DPONE_SEMANTIC_REFRESH_DAG_PROJECTION_INVALID",
                        "semantic-refresh sidecar identities must be unique",
                    )
                sidecar_files[filename] = bytes(sidecar.content)
                descriptors.append(descriptor)
            if sidecar_files:
                airflow_index["semantic_refresh_dag_projections"] = sorted(
                    descriptors,
                    key=lambda item: str(item["projection_id"]),
                )
        return self._projection_result(
            inputs=inputs,
            deployment=deployment,
            deployment_bytes=deployment_bytes,
            airflow_index=airflow_index,
            runtime_connection_snapshots=runtime_connection_snapshots,
            additional_files=sidecar_files,
        )

    def materialize_local_safe_sample_v1(
        self,
        *,
        release_id: str,
        environment: str,
        runtime_image_digest: str,
        artifact_registry_ref: str,
        airflow_bundle_ref: str | None = None,
    ) -> AirflowDeploymentProjection:
        """Materialize the explicit local-only v1 compatibility lane.

        This method intentionally has no trust tier, exact runtime image, or
        ConfigMap arguments. Callers that need an executable environment KPO
        must use :meth:`materialize`, which emits v2 and requires those inputs.
        """

        environment = normalize_environment_segment(environment)
        require_digest("release_id", release_id)
        require_digest("runtime_image_digest", runtime_image_digest)
        require_legacy_artifact_registry_ref(artifact_registry_ref)
        inputs = load_local_safe_sample_v1_inputs(
            root=self._root,
            cache_root=self._cache_root,
            release_id=release_id,
            environment=environment,
        )
        deployment, airflow_index = build_local_safe_sample_v1_projection(
            release_id=release_id,
            environment=environment,
            runtime_image_digest=runtime_image_digest,
            artifact_registry_ref=artifact_registry_ref,
            airflow_bundle_ref=airflow_bundle_ref,
            binding_set=inputs.binding_set,
            binding_fingerprint=inputs.binding_fingerprint,
            registry_fingerprint=inputs.registry_fingerprint,
            credential_runtime_fingerprint=inputs.credential_runtime_fingerprint,
            dag_specs=inputs.dag_specs,
            workload_packs=inputs.workload_packs,
        )
        return self._projection_result(
            inputs=inputs,
            deployment=deployment,
            deployment_bytes=json_bytes(deployment),
            airflow_index=airflow_index,
            runtime_connection_snapshots=None,
        )

    def _projection_result(
        self,
        *,
        inputs: DeploymentProjectionInputs,
        deployment: dict[str, Any],
        deployment_bytes: bytes,
        airflow_index: dict[str, Any],
        runtime_connection_snapshots: Mapping[str, bytes] | None,
        additional_files: Mapping[str, bytes] | None = None,
    ) -> AirflowDeploymentProjection:
        if runtime_connection_snapshots is not None:
            binding_set_fingerprint, connection_registry_fingerprint, credential_runtime_fingerprint = (
                runtime_connection_fingerprints(runtime_connection_snapshots)
            )
        else:
            binding_set_fingerprint = inputs.binding_fingerprint
            connection_registry_fingerprint = inputs.registry_fingerprint
            credential_runtime_fingerprint = inputs.credential_runtime_fingerprint
        try:
            with promotion_lock(self._cache_root):
                pass
        except DeploymentCacheError as exc:
            raise AirflowDeploymentProjectionError(
                "DPONE_DEPLOYMENT_CACHE_LOCK_FAILED",
                "deployment cache writer lease could not be initialized",
            ) from exc
        deployment_dir = write_projection(
            root=self._root,
            cache_root=self._cache_root,
            environment=inputs.environment,
            deployment_id=str(deployment["deployment_id"]),
            deployment_bytes=deployment_bytes,
            airflow_index=airflow_index,
            binding_set=inputs.binding_set,
            connection_registry_fingerprint=connection_registry_fingerprint,
            credential_runtime_fingerprint=credential_runtime_fingerprint,
            runtime_connection_snapshots=runtime_connection_snapshots,
            additional_files=additional_files,
            publisher=publish_immutable_projection,
        )
        return AirflowDeploymentProjection(
            deployment_dir=deployment_dir,
            deployment=deployment,
            airflow_index=airflow_index,
            binding_set_fingerprint=binding_set_fingerprint,
            connection_registry_fingerprint=connection_registry_fingerprint,
            credential_runtime_fingerprint=credential_runtime_fingerprint,
        )


def _supervisor_projection(
    *,
    release_schema: str,
    value: Mapping[str, object] | None,
) -> CompositionSupervisorProjection | None:
    try:
        return supervisor_projection_for_release(release_schema=release_schema, value=value)
    except ValueError as exc:
        reason = str(exc)
        if reason == "composition_supervisor_required":
            code = "DPONE_COMPOSITION_SUPERVISOR_REQUIRED"
            message = "composition release requires a complete supervisor deployment capability"
        elif reason == "composition_supervisor_forbidden":
            code = "DPONE_COMPOSITION_SUPERVISOR_FORBIDDEN"
            message = "native v1/v2 and ordinary releases cannot carry a composition supervisor capability"
        else:
            code = "DPONE_COMPOSITION_SUPERVISOR_INVALID"
            message = "composition supervisor deployment capability is invalid"
        raise AirflowDeploymentProjectionError(code, message) from exc


def _seal_composition_supervisor_projection(
    *,
    deployment: dict[str, Any],
    airflow_index: dict[str, Any],
    projection: CompositionSupervisorProjection,
) -> bytes:
    """Mirror the capability and reseal deployment identity and descriptor."""

    if (
        deployment.get("schema") != "dpone.deployment-set.v3"
        or airflow_index.get("schema") != "dpone.airflow-deployment-index.v3"
    ):
        raise AirflowDeploymentProjectionError(
            "DPONE_COMPOSITION_SUPERVISOR_INVALID",
            "composition supervisor requires the v3 deployment wire pair",
        )
    projection_payload = projection.to_dict()
    deployment["composition_supervisor"] = projection_payload
    deployment_id = compute_deployment_id(deployment)
    deployment["deployment_id"] = deployment_id
    deployment_bytes = json_bytes(deployment)
    airflow_index["deployment_id"] = deployment_id
    airflow_index["composition_supervisor"] = projection.to_dict()
    airflow_index["deployment"] = bytes_descriptor(
        artifact_ref=(f"cache://deployments/{deployment['environment']}/{digest_dir(deployment_id)}/deployment.json"),
        payload=deployment_bytes,
    )
    return deployment_bytes


__all__ = [
    "AirflowDeploymentProjection",
    "AirflowDeploymentProjectionError",
    "AirflowDeploymentProjectionService",
    "compute_deployment_id",
    "compute_release_id",
    "deployment_payload_fingerprint",
]
