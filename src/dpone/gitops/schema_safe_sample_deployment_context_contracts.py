from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract


from typing import Any

from dpone.gitops.schema_contract_primitives import documented_contract, pinned_cache_ref_schema
from dpone.gitops.schema_runtime_artifact_delivery import runtime_artifact_delivery_schema


def safe_sample_deployment_context_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (safe_sample_airflow_deployment_context_contract(),)


def safe_sample_airflow_deployment_context_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="safe-sample-airflow-deployment-context",
        kind="dpone.safe-sample-airflow-deployment-context.v1",
        title="dpone GitOps safe sample Airflow deployment context",
        required=(
            "schema",
            "release_id",
            "deployment_id",
            "environment",
            "deployment_type",
            "runnable",
            "runtime_artifact_delivery",
            "workload_packs",
            "binding_set_ref",
            "connection_registry_ref",
            "credential_runtime_ref",
            "runtime_image_digest",
            "airflow_bundle_ref",
            "index_path",
            "deployment_path",
            "parse_side_effects",
        ),
        properties=airflow_deployment_context_properties(),
        defs={
            "sha256": sha256_schema(),
            "artifact": artifact_schema(),
        },
        additional_properties=False,
    )


def airflow_deployment_context_properties() -> dict[str, Any]:
    return {
        "schema": {"const": "dpone.safe-sample-airflow-deployment-context.v1"},
        "release_id": {"$ref": "#/$defs/sha256"},
        "deployment_id": {"$ref": "#/$defs/sha256"},
        "environment": {"type": ["string", "null"]},
        "deployment_type": non_empty_string_schema(),
        "runnable": {"type": "boolean"},
        "runtime_artifact_delivery": runtime_artifact_delivery_schema(),
        "workload_packs": {"type": "array", "items": {"$ref": "#/$defs/artifact"}},
        "binding_set_ref": nullable_sha256_schema(),
        "connection_registry_ref": nullable_sha256_schema(),
        "credential_runtime_ref": nullable_sha256_schema(),
        "runtime_image_digest": nullable_sha256_schema(),
        "airflow_bundle_ref": {"type": ["string", "null"]},
        "index_path": non_empty_string_schema(),
        "deployment_path": {"type": ["string", "null"]},
        "parse_side_effects": {
            "type": "object",
            "additionalProperties": {"type": "boolean"},
        },
    }


def artifact_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["id", "artifact_ref", "sha256", "bytes"],
        "additionalProperties": True,
        "properties": {
            "id": non_empty_string_schema(),
            "artifact_ref": pinned_cache_ref_schema(),
            "sha256": {"$ref": "#/$defs/sha256"},
            "bytes": {"type": "integer", "minimum": 1},
        },
    }


def sha256_schema() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}


def nullable_sha256_schema() -> dict[str, Any]:
    return {"anyOf": [{"$ref": "#/$defs/sha256"}, {"type": "null"}]}


def non_empty_string_schema() -> dict[str, Any]:
    return {"type": "string", "minLength": 1}


__all__ = ["airflow_deployment_context_properties", "safe_sample_deployment_context_schema_contracts"]
