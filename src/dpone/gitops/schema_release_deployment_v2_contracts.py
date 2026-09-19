"""Executable v2 deployment-set and airflow-index schema contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract

from dpone.contracts.development_delivery_authority import (
    DEVELOPMENT_AUTHORITY_SCHEMA,
    DEVELOPMENT_COMPOSITION_PROFILE,
    DEVELOPMENT_RELEASE_SCHEMA,
)
from dpone.gitops.schema_contract_primitives import documented_contract
from dpone.gitops.schema_release_deployment_definitions import (
    airflow_deployment_index_v2_defs,
    deployment_set_v2_defs,
    deployment_workload_inventory_v2_schema,
    release_artifacts_v2_schema,
    release_v2_defs,
    trust_tier_mirror_guards,
    trust_tier_schema,
)
from dpone.gitops.schema_release_set_promotion import COMPACT_PROMOTION_SCHEMA
from dpone.gitops.schema_runtime_artifact_delivery import (
    runtime_artifact_delivery_schema,
    runtime_image_ref_schema,
)
from dpone.gitops.schema_runtime_connection_context import (
    runtime_connection_artifact_schema,
)


def dev_evidence_delivery_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["mode", "claim_name", "mount_path", "worker_queue"],
        "properties": {
            "mode": {"const": "shared_pvc"},
            "claim_name": {
                "type": "string",
                "pattern": "^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$",
                "maxLength": 63,
            },
            "mount_path": {"const": "/var/lib/dpone/dev-evidence"},
            "worker_queue": {
                "type": "string",
                "pattern": "^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$",
            },
        },
    }


def dev_evidence_non_production_guard() -> dict[str, Any]:
    return {
        "if": {"required": ["dev_evidence_delivery"]},
        "then": {"properties": {"trust_tier": {"const": "non_production"}}},
    }


def deployment_set_v2_contract() -> GitOpsSchemaContract:
    result = documented_contract(
        name="deployment-set-v2",
        kind="dpone.deployment-set.v2",
        title="dpone GitOps executable deployment-set v2",
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
            "schema": {"const": "dpone.deployment-set.v2"},
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
        },
        defs=deployment_set_v2_defs(),
        additional_properties=False,
    )
    result.schema["allOf"] = [
        *trust_tier_mirror_guards(),
        dev_evidence_non_production_guard(),
    ]
    return result


def airflow_deployment_index_v2_contract() -> GitOpsSchemaContract:
    result = documented_contract(
        name="airflow-deployment-index-v2",
        kind="dpone.airflow-deployment-index.v2",
        title="dpone GitOps executable Airflow deployment index v2",
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
            "schema": {"const": "dpone.airflow-deployment-index.v2"},
            "release_id": {"$ref": "#/$defs/identity"},
            "deployment_id": {"$ref": "#/$defs/identity"},
            "trust_tier": trust_tier_schema(),
            "dag_specs": {"$ref": "#/$defs/artifacts"},
            "workload_packs": {"$ref": "#/$defs/workloadArtifacts"},
            "runtime_payloads": {"$ref": "#/$defs/runtimePayloads"},
            "semantic_refresh_dag_projections": {"$ref": "#/$defs/semanticRefreshDagProjections"},
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
    ]
    return result


def development_release_set_contract() -> GitOpsSchemaContract:
    """Return the development authority schema over the stable v2 wire."""

    identity = {"$ref": "#/$defs/identity"}
    token = {"type": "string", "pattern": "^[a-z0-9][a-z0-9_.-]{0,249}$"}
    return documented_contract(
        name="dbt-release-set-development-v1",
        kind=DEVELOPMENT_RELEASE_SCHEMA,
        title="dpone GitOps development dbt workspace release",
        required=(
            "schema",
            "release_id",
            "producer",
            "selection_authority",
            "selection_fingerprint",
            "artifacts",
            "provenance",
            "development_authority",
        ),
        properties={
            "schema": {"const": DEVELOPMENT_RELEASE_SCHEMA},
            "release_id": identity,
            "producer": {
                "type": "object",
                "additionalProperties": False,
                "required": ["dpone_version", "wire_contract"],
                "properties": {
                    "dpone_version": {"type": "string", "pattern": "^[A-Za-z0-9][A-Za-z0-9.!+_-]{0,63}$"},
                    "wire_contract": {"const": "dpone.dbt-airflow-self-service.v2"},
                },
            },
            "selection_authority": {"const": "dbt_cli"},
            "selection_fingerprint": identity,
            "artifacts": release_artifacts_v2_schema(),
            "promotion": {
                "type": "object",
                "additionalProperties": False,
                "required": ["schema", "profile"],
                "properties": {
                    "schema": {"const": COMPACT_PROMOTION_SCHEMA},
                    "profile": {"const": DEVELOPMENT_COMPOSITION_PROFILE},
                },
            },
            "provenance": {
                "type": "object",
                "additionalProperties": False,
                "required": ["source", "source_snapshot_sha256", "selection_fingerprints", "route_certifications"],
                "properties": {
                    "source": {"const": "dpone dbt compile"},
                    "source_snapshot_sha256": identity,
                    "selection_fingerprints": {
                        "type": "array",
                        "minItems": 1,
                        "uniqueItems": True,
                        "items": identity,
                    },
                    "route_certifications": {"type": "array", "maxItems": 0},
                },
            },
            "development_authority": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "schema",
                    "policy_sha256",
                    "grant_sha256",
                    "signature_subject_sha256",
                    "environment",
                    "source_repository_sha256",
                    "source_commit",
                    "revocation_epoch",
                    "max_workloads",
                    "max_source_bytes",
                    "execution_subjects",
                ],
                "properties": {
                    "schema": {"const": DEVELOPMENT_AUTHORITY_SCHEMA},
                    "policy_sha256": identity,
                    "grant_sha256": identity,
                    "signature_subject_sha256": identity,
                    "environment": {"type": "string", "pattern": "^[a-z0-9][a-z0-9_-]{0,62}$"},
                    "source_repository_sha256": identity,
                    "source_commit": {"type": "string", "pattern": "^[0-9a-f]{40}$"},
                    "revocation_epoch": {"type": "integer", "minimum": 0},
                    "max_workloads": {"type": "integer", "minimum": 1, "maximum": 10000},
                    "max_source_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 9_223_372_036_854_775_807,
                    },
                    "execution_subjects": {
                        "type": "array",
                        "maxItems": 10_000,
                        "uniqueItems": True,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["workload_id", "kind"],
                            "properties": {
                                "workload_id": token,
                                "kind": {"enum": ["runtime", "pre_hook"]},
                                "hook_id": token,
                            },
                            "allOf": [
                                {
                                    "if": {"properties": {"kind": {"const": "pre_hook"}}},
                                    "then": {"required": ["hook_id"]},
                                    "else": {"not": {"required": ["hook_id"]}},
                                }
                            ],
                        },
                    },
                },
            },
        },
        defs=release_v2_defs(),
        additional_properties=False,
    )


__all__ = [
    "airflow_deployment_index_v2_contract",
    "development_release_set_contract",
    "deployment_set_v2_contract",
    "dev_evidence_delivery_schema",
    "dev_evidence_non_production_guard",
]
