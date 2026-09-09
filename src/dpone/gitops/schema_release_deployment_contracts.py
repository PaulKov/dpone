from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract


from dpone.gitops.schema_contract_primitives import (
    boolean_schema,
    documented_contract,
    string_schema,
)
from dpone.gitops.schema_release_deployment_definitions import (
    identity_schema,
    mssql_asset_outlet_projection_schema,
    nullable_sha256_schema,
    release_artifacts_schema,
    release_artifacts_v2_schema,
    release_defs,
    release_provenance_schema,
    release_v2_defs,
)
from dpone.gitops.schema_release_deployment_v2_contracts import (
    airflow_deployment_index_v2_contract,
    deployment_set_v2_contract,
)
from dpone.gitops.schema_release_deployment_v3_contracts import (
    airflow_deployment_index_v3_contract,
    deployment_set_v3_contract,
)
from dpone.gitops.schema_release_set_promotion import compact_pack_promotion_guards
from dpone.gitops.schema_runtime_artifact_delivery import (
    runtime_artifact_delivery_schema,
)


def release_deployment_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        release_set_contract(),
        release_set_v2_contract(),
        deployment_set_contract(),
        deployment_set_v2_contract(),
        deployment_set_v3_contract(),
        airflow_deployment_index_v2_contract(),
        airflow_deployment_index_v3_contract(),
        mssql_asset_outlet_projection_contract(),
        current_pointer_contract(),
    )


def mssql_asset_outlet_projection_contract() -> GitOpsSchemaContract:
    """Standalone closed contract for deployment-owned MSSQL outlet projections."""

    identity = {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
    schema = mssql_asset_outlet_projection_schema()
    return documented_contract(
        name="mssql-asset-outlet-projection",
        kind="dpone.mssql-asset-outlet-projection.v1",
        title="dpone GitOps MSSQL asset outlet projection",
        required=tuple(schema["required"]),
        properties=dict(schema["properties"]),
        defs={"identity": identity},
        additional_properties=False,
    )


def release_set_contract() -> GitOpsSchemaContract:
    contract = documented_contract(
        name="release-set",
        kind="dpone.release-set.v1",
        title="dpone GitOps release-set",
        required=("schema", "release_id", "artifacts"),
        properties={
            "schema": {"const": "dpone.release-set.v1"},
            "release_id": {"$ref": "#/$defs/identity"},
            "selection_fingerprint": {"$ref": "#/$defs/identity"},
            "artifacts": release_artifacts_schema(),
            "promotion": {},
            "provenance": release_provenance_schema(),
        },
        defs=release_defs(),
    )
    contract.schema["allOf"] = list(compact_pack_promotion_guards())
    return contract


def release_set_v2_contract() -> GitOpsSchemaContract:
    """Environment-neutral release with digest-pinned external runtime payloads."""

    return documented_contract(
        name="release-set-v2",
        kind="dpone.release-set.v2",
        title="dpone GitOps release-set v2",
        required=(
            "schema",
            "release_id",
            "producer",
            "selection_authority",
            "selection_fingerprint",
            "artifacts",
            "provenance",
        ),
        properties={
            "schema": {"const": "dpone.release-set.v2"},
            "release_id": {"$ref": "#/$defs/identity"},
            "producer": {
                "type": "object",
                "additionalProperties": False,
                "required": ["dpone_version", "wire_contract"],
                "properties": {
                    "dpone_version": {
                        "type": "string",
                        "pattern": "^[A-Za-z0-9][A-Za-z0-9.!+_-]{0,63}$",
                    },
                    "wire_contract": {
                        "enum": [
                            "dpone.dbt-airflow-self-service.v1",
                            "dpone.dbt-airflow-self-service.v2",
                        ],
                    },
                },
            },
            "selection_authority": {"const": "dbt_cli"},
            "selection_fingerprint": {"$ref": "#/$defs/identity"},
            "artifacts": release_artifacts_v2_schema(),
            "provenance": _dbt_release_provenance_schema(),
        },
        defs=release_v2_defs(),
        additional_properties=False,
    )


def _dbt_release_provenance_schema() -> dict[str, object]:
    route_certification = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "variant_id",
            "route_id",
            "transport",
            "schema_evolution",
            "airflow_runtime_mode",
            "snapshot_id",
            "support",
            "certification_level",
            "evidence_status",
            "evidence_refs",
            "evidence_reason_codes",
        ],
        "properties": {
            "variant_id": {"type": "string", "minLength": 1},
            "route_id": {"type": "string", "minLength": 1},
            "transport": {"type": "string", "minLength": 1},
            "schema_evolution": {"type": "string", "minLength": 1},
            "airflow_runtime_mode": {"type": "string", "minLength": 1},
            "snapshot_id": {"$ref": "#/$defs/identity"},
            "support": {"enum": ["supported", "conditional"]},
            "certification_level": {
                "enum": ["production-certified", "enterprise-certified"],
            },
            "evidence_status": {"const": "PASS"},
            "evidence_refs": {
                "type": "array",
                "minItems": 1,
                "items": {"$ref": "#/$defs/identity"},
                "uniqueItems": True,
            },
            "evidence_reason_codes": {
                "type": "array",
                "items": {"type": "string"},
                "uniqueItems": True,
            },
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "source",
            "source_snapshot_sha256",
            "selection_fingerprints",
            "route_certifications",
        ],
        "properties": {
            "source": {"const": "dpone dbt compile"},
            "source_snapshot_sha256": {"$ref": "#/$defs/identity"},
            "selection_fingerprints": {
                "type": "array",
                "minItems": 1,
                "items": {"$ref": "#/$defs/identity"},
                "uniqueItems": True,
            },
            "route_certifications": {
                "type": "array",
                "minItems": 1,
                "items": route_certification,
            },
        },
    }


def deployment_set_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="deployment-set",
        kind="dpone.deployment-set.v1",
        title="dpone GitOps deployment-set",
        required=("schema", "deployment_id", "environment", "release_ref", "runtime_artifact_delivery"),
        properties={
            "schema": {"const": "dpone.deployment-set.v1"},
            "deployment_id": {"$ref": "#/$defs/identity"},
            "deployment_type": {"enum": ["preview", "environment"]},
            "runnable": boolean_schema(),
            "environment": {"type": "string", "minLength": 1},
            "release_ref": {"$ref": "#/$defs/identity"},
            "binding_set_ref": nullable_sha256_schema(),
            "connection_registry_ref": nullable_sha256_schema(),
            "credential_runtime_ref": nullable_sha256_schema(),
            "runtime_image_digest": nullable_sha256_schema(),
            "airflow_bundle_ref": {"type": ["string", "null"]},
            "runtime_artifact_delivery": runtime_artifact_delivery_schema(),
        },
        defs={"identity": identity_schema()},
    )


def current_pointer_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="current-pointer",
        kind="dpone.current-pointer.v1",
        title="dpone GitOps current deployment pointer",
        required=("schema", "environment", "deployment_id", "release_id", "promoted_by", "promoted_at"),
        properties={
            "schema": {"const": "dpone.current-pointer.v1"},
            "activation_id": {
                "type": "string",
                "pattern": ("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"),
            },
            "environment": {"type": "string", "minLength": 1},
            "deployment_id": {"$ref": "#/$defs/identity"},
            "release_id": {"$ref": "#/$defs/identity"},
            "promoted_by": {"type": "string", "minLength": 1},
            "promoted_at": {"type": "string", "format": "date-time"},
            "source_commit": string_schema(),
            "previous_deployment_id": nullable_sha256_schema(),
            "attestation_ref": {"type": ["string", "null"]},
            "workspace_authority_connection_ref": {
                "type": "string",
                "pattern": "^[a-z][a-z0-9_]{0,127}$",
            },
        },
        defs={"identity": identity_schema()},
    )


__all__ = ["release_deployment_schema_contracts"]
