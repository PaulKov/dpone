"""Executable v3 deployment-set and airflow-index schema contracts.

Wire decision
-------------
The v3 pair carries either the mandatory supervisor capability of a
``dpone.release-set.v3`` composition, an MSSQL outlet projection for a legacy
release, or both. The v2 pair stays exact when neither v3 capability is needed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract

from dpone.gitops.schema_contract_primitives import documented_contract
from dpone.gitops.schema_release_deployment_definitions import (
    MSSQL_OUTLET_PROJECTION_PROPERTY,
    airflow_deployment_index_v2_defs,
    deployment_set_v2_defs,
    deployment_workload_inventory_v2_schema,
    trust_tier_mirror_guards,
    trust_tier_schema,
)
from dpone.gitops.schema_release_deployment_v2_contracts import (
    dev_evidence_delivery_schema,
    dev_evidence_non_production_guard,
)
from dpone.gitops.schema_runtime_artifact_delivery import (
    runtime_artifact_delivery_schema,
    runtime_image_ref_schema,
)
from dpone.gitops.schema_runtime_connection_context import (
    runtime_connection_artifact_schema,
)


def composition_supervisor_schema() -> dict[str, object]:
    """Return the closed non-secret Kubernetes supervisor capability."""

    return {
        "type": "object",
        "required": [
            "schema",
            "persistent_volume_claim",
            "child_uid_start",
            "child_gid_start",
            "child_identity_count",
        ],
        "additionalProperties": False,
        "properties": {
            "schema": {"const": "dpone.composition-supervisor.v1"},
            "persistent_volume_claim": {
                "type": "string",
                "minLength": 1,
                "maxLength": 63,
                "pattern": "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
            },
            "child_uid_start": {"type": "integer", "minimum": 1_000_000, "maximum": 2_146_483_647},
            "child_gid_start": {"type": "integer", "minimum": 1_000_000, "maximum": 2_146_483_647},
            "child_identity_count": {"type": "integer", "minimum": 1_000_000},
        },
    }


_V3_CAPABILITY_GUARD = {
    "anyOf": [
        {"required": ["composition_supervisor"]},
        {"required": ["mssql_asset_outlet_projection"]},
    ]
}


def deployment_set_v3_contract() -> GitOpsSchemaContract:
    result = documented_contract(
        name="deployment-set-v3",
        kind="dpone.deployment-set.v3",
        title="dpone GitOps executable deployment-set v3",
        required=(
            "schema",
            "deployment_id",
            "deployment_type",
            "runnable",
            "environment",
            "trust_tier",
            "release_ref",
            "binding_set_ref",
            "connection_registry_ref",
            "credential_runtime_ref",
            "binding_set",
            "connection_registry",
            "credential_runtime",
            "runtime_image_ref",
            "runtime_image_digest",
            "airflow_bundle_ref",
            "runtime_artifact_delivery",
            "workloads",
        ),
        properties={
            "schema": {"const": "dpone.deployment-set.v3"},
            "deployment_id": {"$ref": "#/$defs/identity"},
            "deployment_type": {"const": "environment"},
            "runnable": {"const": True},
            "environment": {"type": "string", "minLength": 1},
            "trust_tier": trust_tier_schema(),
            "release_ref": {"$ref": "#/$defs/identity"},
            "binding_set_ref": {"$ref": "#/$defs/identity"},
            "connection_registry_ref": {"$ref": "#/$defs/identity"},
            "credential_runtime_ref": {"$ref": "#/$defs/identity"},
            "binding_set": runtime_connection_artifact_schema("binding-set.json"),
            "connection_registry": runtime_connection_artifact_schema("connection-registry.json"),
            "credential_runtime": runtime_connection_artifact_schema("credential-runtime.json"),
            "runtime_image_ref": runtime_image_ref_schema(),
            "runtime_image_digest": {"$ref": "#/$defs/identity"},
            "airflow_bundle_ref": {"type": ["string", "null"]},
            "runtime_artifact_delivery": runtime_artifact_delivery_schema(strict_init_fetch=True),
            "dev_evidence_delivery": dev_evidence_delivery_schema(),
            "workloads": deployment_workload_inventory_v2_schema(),
            "composition_supervisor": composition_supervisor_schema(),
            **MSSQL_OUTLET_PROJECTION_PROPERTY,
        },
        defs=deployment_set_v2_defs(),
        additional_properties=False,
    )
    result.schema["allOf"] = [
        *trust_tier_mirror_guards(),
        dev_evidence_non_production_guard(),
        _V3_CAPABILITY_GUARD,
    ]
    return result


def airflow_deployment_index_v3_contract() -> GitOpsSchemaContract:
    result = documented_contract(
        name="airflow-deployment-index-v3",
        kind="dpone.airflow-deployment-index.v3",
        title="dpone GitOps executable Airflow deployment index v3",
        required=(
            "schema",
            "release_id",
            "deployment_id",
            "trust_tier",
            "dag_specs",
            "workload_packs",
            "binding_set_ref",
            "connection_registry_ref",
            "credential_runtime_ref",
            "binding_set",
            "connection_registry",
            "credential_runtime",
            "runtime_image_ref",
            "runtime_image_digest",
            "airflow_bundle_ref",
            "runtime_artifact_delivery",
            "release",
            "deployment",
        ),
        properties={
            "schema": {"const": "dpone.airflow-deployment-index.v3"},
            "release_id": {"$ref": "#/$defs/identity"},
            "deployment_id": {"$ref": "#/$defs/identity"},
            "trust_tier": trust_tier_schema(),
            "dag_specs": {"$ref": "#/$defs/artifacts"},
            "workload_packs": {"$ref": "#/$defs/workloadArtifacts"},
            "runtime_payloads": {"$ref": "#/$defs/runtimePayloads"},
            "semantic_refresh_dag_projections": {"$ref": "#/$defs/semanticRefreshDagProjections"},
            "composition_supervisor": composition_supervisor_schema(),
            **MSSQL_OUTLET_PROJECTION_PROPERTY,
            "binding_set_ref": {"$ref": "#/$defs/identity"},
            "connection_registry_ref": {"$ref": "#/$defs/identity"},
            "credential_runtime_ref": {"$ref": "#/$defs/identity"},
            "binding_set": runtime_connection_artifact_schema("binding-set.json"),
            "connection_registry": runtime_connection_artifact_schema("connection-registry.json"),
            "credential_runtime": runtime_connection_artifact_schema("credential-runtime.json"),
            "runtime_image_ref": runtime_image_ref_schema(),
            "runtime_image_digest": {"$ref": "#/$defs/identity"},
            "airflow_bundle_ref": {"type": ["string", "null"]},
            "runtime_artifact_delivery": runtime_artifact_delivery_schema(strict_init_fetch=True),
            "dev_evidence_delivery": dev_evidence_delivery_schema(),
            "release": {"$ref": "#/$defs/exactArtifact"},
            "deployment": {"$ref": "#/$defs/exactArtifact"},
        },
        defs=airflow_deployment_index_v2_defs(),
        additional_properties=False,
    )
    result.schema["allOf"] = [
        *trust_tier_mirror_guards(),
        dev_evidence_non_production_guard(),
        _V3_CAPABILITY_GUARD,
    ]
    return result


__all__ = [
    "airflow_deployment_index_v3_contract",
    "composition_supervisor_schema",
    "deployment_set_v3_contract",
]
