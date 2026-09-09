"""Assemble deployment + airflow-index payloads with optional MSSQL projection."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.contracts.airflow_deployment import deployment_id
from dpone.readiness.airflow_deployment_artifacts import (
    bytes_descriptor,
    digest_dir,
    json_bytes,
)

DEPLOYMENT_SET_SCHEMA_V2 = "dpone.deployment-set.v2"
DEPLOYMENT_SET_SCHEMA_V3 = "dpone.deployment-set.v3"
AIRFLOW_INDEX_SCHEMA_V2 = "dpone.airflow-deployment-index.v2"
AIRFLOW_INDEX_SCHEMA_V3 = "dpone.airflow-deployment-index.v3"


def build_environment_deployment_documents(
    *,
    environment: str,
    release_id: str,
    trust_tier: str,
    binding_set_ref: str,
    connection_registry_ref: str,
    credential_runtime_ref: str,
    runtime_connection_descriptors: Mapping[str, Any],
    runtime_image_ref: str,
    runtime_image_digest: str,
    runtime_image_dbt_fields: Mapping[str, Any],
    airflow_bundle_ref: str | None,
    runtime_delivery: Mapping[str, Any],
    dev_evidence_delivery: Mapping[str, Any] | None,
    workload_inventory: Sequence[Mapping[str, Any]],
    dag_specs: Sequence[Mapping[str, Any]],
    workload_packs: Sequence[Mapping[str, Any]],
    runtime_payloads: Sequence[Mapping[str, Any]],
    release_bytes: bytes,
    mssql_outlet_projection: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], bytes, dict[str, Any]]:
    """Return ``(deployment, deployment_bytes, airflow_index)``.

    When ``mssql_outlet_projection`` is present the closed v3 wire pair is
    emitted (projection is required on v3). Otherwise the producer keeps the
    exact v2 pair so older closed readers are unaffected.
    """

    use_v3 = mssql_outlet_projection is not None
    optional_projection = {"mssql_asset_outlet_projection": dict(mssql_outlet_projection)} if use_v3 else {}
    deployment: dict[str, Any] = {
        "schema": DEPLOYMENT_SET_SCHEMA_V3 if use_v3 else DEPLOYMENT_SET_SCHEMA_V2,
        "deployment_id": "",
        "deployment_type": "environment",
        "runnable": True,
        "environment": environment,
        "trust_tier": trust_tier,
        "release_ref": release_id,
        "binding_set_ref": binding_set_ref,
        "connection_registry_ref": connection_registry_ref,
        "credential_runtime_ref": credential_runtime_ref,
        **runtime_connection_descriptors,
        "runtime_image_ref": runtime_image_ref,
        "runtime_image_digest": runtime_image_digest,
        **runtime_image_dbt_fields,
        "airflow_bundle_ref": airflow_bundle_ref,
        "runtime_artifact_delivery": runtime_delivery,
        **({"dev_evidence_delivery": dev_evidence_delivery} if dev_evidence_delivery is not None else {}),
        "workloads": list(workload_inventory),
        **optional_projection,
    }
    computed_deployment_id = deployment_id(deployment)
    deployment["deployment_id"] = computed_deployment_id
    deployment_bytes = json_bytes(deployment)
    airflow_index: dict[str, Any] = {
        "schema": AIRFLOW_INDEX_SCHEMA_V3 if use_v3 else AIRFLOW_INDEX_SCHEMA_V2,
        "release_id": release_id,
        "deployment_id": computed_deployment_id,
        "trust_tier": trust_tier,
        "dag_specs": list(dag_specs),
        "workload_packs": list(workload_packs),
        **({"runtime_payloads": list(runtime_payloads)} if runtime_payloads else {}),
        "binding_set_ref": binding_set_ref,
        "connection_registry_ref": connection_registry_ref,
        "credential_runtime_ref": credential_runtime_ref,
        **runtime_connection_descriptors,
        "runtime_image_ref": runtime_image_ref,
        "runtime_image_digest": runtime_image_digest,
        **runtime_image_dbt_fields,
        "airflow_bundle_ref": airflow_bundle_ref,
        "runtime_artifact_delivery": runtime_delivery,
        **({"dev_evidence_delivery": dev_evidence_delivery} if dev_evidence_delivery is not None else {}),
        "release": bytes_descriptor(
            artifact_ref=f"cache://releases/{digest_dir(release_id)}/release-set.json",
            payload=release_bytes,
        ),
        "deployment": bytes_descriptor(
            artifact_ref=(f"cache://deployments/{environment}/{digest_dir(computed_deployment_id)}/deployment.json"),
            payload=deployment_bytes,
        ),
        **optional_projection,
    }
    return deployment, deployment_bytes, airflow_index


__all__ = [
    "AIRFLOW_INDEX_SCHEMA_V2",
    "AIRFLOW_INDEX_SCHEMA_V3",
    "DEPLOYMENT_SET_SCHEMA_V2",
    "DEPLOYMENT_SET_SCHEMA_V3",
    "build_environment_deployment_documents",
]
