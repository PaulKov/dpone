"""Reusable JSON Schema fragments for release/deployment contracts."""

from __future__ import annotations

from typing import Any

from dpone.contracts.airflow_release_artifacts import RELEASE_ARTIFACT_PATH_PATTERN
from dpone.contracts.runtime_artifact_delivery import TRUST_TIERS
from dpone.gitops.schema_contract_primitives import (
    pinned_cache_ref_schema,
    string_schema,
)
from dpone.gitops.schema_mssql_outlet_projection import (
    MSSQL_OUTLET_PROJECTION_PROPERTY as _MSSQL_OUTLET_PROJECTION_PROPERTY,
)
from dpone.gitops.schema_mssql_outlet_projection import (
    mssql_asset_outlet_projection_schema,
)
from dpone.gitops.schema_runtime_connection_context import (
    exact_runtime_artifact_schema,
)

MSSQL_OUTLET_PROJECTION_PROPERTY = _MSSQL_OUTLET_PROJECTION_PROPERTY


def deployment_workload_inventory_v2_schema() -> dict[str, Any]:
    return {
        "type": "array",
        "minItems": 1,
        "items": {
            "type": "object",
            "required": ["id", "sha256", "pack_fingerprint"],
            "additionalProperties": False,
            "properties": {
                "id": {"type": "string", "minLength": 1},
                "sha256": {"$ref": "#/$defs/identity"},
                "pack_fingerprint": {"$ref": "#/$defs/identity"},
            },
        },
    }


def airflow_deployment_index_v2_defs() -> dict[str, Any]:
    artifact_properties = {
        "id": {"type": "string", "minLength": 1},
        "artifact_ref": pinned_cache_ref_schema(),
        "sha256": {"$ref": "#/$defs/sha256"},
        "bytes": {"type": "integer", "minimum": 1},
    }
    return {
        "identity": identity_schema(),
        "sha256": identity_schema(),
        "artifact": _artifact_schema(artifact_properties),
        "artifacts": {
            "type": "array",
            "items": {"$ref": "#/$defs/artifact"},
        },
        "workloadArtifact": _workload_artifact_schema(artifact_properties),
        "workloadArtifacts": {
            "type": "array",
            "minItems": 1,
            "items": {"$ref": "#/$defs/workloadArtifact"},
        },
        "runtimePayload": _runtime_payload_schema(artifact_ref=pinned_cache_ref_schema()),
        "runtimePayloads": {
            "type": "array",
            "maxItems": 64,
            "items": {"$ref": "#/$defs/runtimePayload"},
        },
        "semanticRefreshDagProjectionAuthority": (_semantic_refresh_dag_projection_authority_schema()),
        "semanticRefreshDagProjection": (_semantic_refresh_dag_projection_schema()),
        "semanticRefreshDagProjections": {
            "type": "array",
            "maxItems": 64,
            "items": {"$ref": "#/$defs/semanticRefreshDagProjection"},
        },
        "mssqlAssetOutletProjection": mssql_asset_outlet_projection_schema(),
        "exactArtifact": exact_runtime_artifact_schema(
            artifact_ref_schema=pinned_cache_ref_schema(),
        ),
    }


def _semantic_refresh_dag_projection_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "projection_id",
            "workflow_name",
            "dag_id",
            "dag_projection_sha256",
            "artifact_ref",
            "artifact_sha256",
            "artifact_bytes",
            "authority",
        ],
        "properties": {
            "projection_id": {
                "type": "string",
                "pattern": "^semantic_refresh_v2::[A-Za-z0-9_.-]{1,250}$",
            },
            "workflow_name": {"type": "string", "minLength": 1, "maxLength": 512},
            "dag_id": {
                "type": "string",
                "pattern": "^[A-Za-z0-9_.-]{1,250}$",
            },
            "dag_projection_sha256": {"$ref": "#/$defs/identity"},
            "artifact_ref": pinned_cache_ref_schema(),
            "artifact_sha256": {"$ref": "#/$defs/identity"},
            "artifact_bytes": {"type": "integer", "minimum": 1},
            "authority": {"$ref": "#/$defs/semanticRefreshDagProjectionAuthority"},
        },
    }


def _semantic_refresh_dag_projection_authority_schema() -> dict[str, Any]:
    digest_fields = (
        "authority_record_sha256",
        "dag_projection_sha256",
        "deployment_id",
        "package_artifacts_sha256",
        "plan_bundle_sha256",
        "pre_release_bundle_sha256",
        "release_id",
        "template_pack_fingerprint",
        "topology_sha256",
        "workflow_plan_sha256",
    )
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["schema", *digest_fields],
        "properties": {
            "schema": {"const": ("dpone.semantic-refresh-v2-dag-projection-authority.v1")},
            **{field: {"$ref": "#/$defs/identity"} for field in digest_fields},
        },
    }


def deployment_set_v2_defs() -> dict[str, Any]:
    return {
        "identity": identity_schema(),
        "sha256": identity_schema(),
        "exactArtifact": exact_runtime_artifact_schema(
            artifact_ref_schema=pinned_cache_ref_schema(),
        ),
        "mssqlAssetOutletProjection": mssql_asset_outlet_projection_schema(),
    }


def trust_tier_schema() -> dict[str, Any]:
    return {"enum": list(TRUST_TIERS)}


def trust_tier_mirror_guards() -> list[dict[str, Any]]:
    return [
        {
            "if": {
                "properties": {"trust_tier": {"const": trust_tier}},
                "required": ["trust_tier"],
            },
            "then": {
                "properties": {"runtime_artifact_delivery": {"properties": {"trust_tier": {"const": trust_tier}}}}
            },
        }
        for trust_tier in TRUST_TIERS
    ]


def release_artifacts_schema() -> dict[str, Any]:
    # Compact v1 may optionally carry digest-pinned runtime payloads when packs
    # declare runtime_payload_ids (dbt self-service). additionalProperties stays
    # false so unknown artifact sections still fail closed.
    return {
        "type": "object",
        "required": ["dag_specs", "workload_packs", "canonical_schemas"],
        "additionalProperties": False,
        "properties": {
            "dag_specs": {"$ref": "#/$defs/artifacts"},
            "workload_packs": {"$ref": "#/$defs/artifacts"},
            "canonical_schemas": {"$ref": "#/$defs/artifacts"},
            "runtime_payloads": {
                "type": "array",
                "minItems": 1,
                "maxItems": 64,
                "items": {"$ref": "#/$defs/runtimePayload"},
            },
        },
    }


def release_artifacts_v2_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "dag_specs",
            "workload_packs",
            "canonical_schemas",
            "runtime_payloads",
        ],
        "additionalProperties": False,
        "properties": {
            "dag_specs": {"$ref": "#/$defs/artifacts"},
            "workload_packs": {"$ref": "#/$defs/artifacts"},
            "canonical_schemas": {"$ref": "#/$defs/artifacts"},
            "runtime_payloads": {
                "type": "array",
                "minItems": 1,
                "maxItems": 64,
                "items": {"$ref": "#/$defs/runtimePayload"},
            },
        },
    }


def release_provenance_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "source_commit": string_schema(),
            "build_id": string_schema(),
            "built_at": {"type": "string", "format": "date-time"},
        },
    }


def release_defs(*, runtime_payload_id_pattern: str = "^[A-Za-z0-9_.-]{1,256}$") -> dict[str, Any]:
    artifact_path = _relative_artifact_path()
    artifact_properties = {
        "id": {"type": "string", "minLength": 1},
        "path": artifact_path,
        "artifact_ref": artifact_path,
        "sha256": {"$ref": "#/$defs/sha256"},
    }
    return {
        "identity": identity_schema(),
        "sha256": checksum_schema(),
        "artifact": {
            "oneOf": [
                _release_artifact_locator_schema(
                    "path",
                    properties=artifact_properties,
                ),
                _release_artifact_locator_schema(
                    "artifact_ref",
                    properties=artifact_properties,
                ),
            ]
        },
        "artifacts": {
            "type": "array",
            "items": {"$ref": "#/$defs/artifact"},
        },
        "runtimePayload": _runtime_payload_schema(
            artifact_ref=artifact_path,
            locator="path",
            id_pattern=runtime_payload_id_pattern,
        ),
    }


def release_v2_defs() -> dict[str, Any]:
    return release_defs(runtime_payload_id_pattern="^[A-Za-z0-9_.:-]{1,256}$")


def nullable_sha256_schema() -> dict[str, Any]:
    return {"anyOf": [{"$ref": "#/$defs/identity"}, {"type": "null"}]}


def identity_schema() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}


def checksum_schema() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[A-Fa-f0-9]{64}$"}


def _artifact_schema(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["id", "artifact_ref", "sha256", "bytes"],
        "additionalProperties": False,
        "properties": properties,
    }


def _workload_artifact_schema(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "id",
            "artifact_ref",
            "sha256",
            "bytes",
            "pack_fingerprint",
        ],
        "additionalProperties": False,
        "properties": {
            **properties,
            "pack_fingerprint": {"$ref": "#/$defs/sha256"},
            "runtime_payload_ids": {
                "type": "array",
                "maxItems": 16,
                "uniqueItems": True,
                "items": {
                    "type": "string",
                    "pattern": "^[A-Za-z0-9_.:-]{1,256}$",
                },
            },
        },
    }


def _runtime_payload_schema(
    *,
    artifact_ref: dict[str, Any],
    locator: str = "artifact_ref",
    id_pattern: str = "^[A-Za-z0-9_.:-]{1,256}$",
) -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "id",
            "kind",
            locator,
            "sha256",
            "bytes",
            "media_type",
        ],
        "additionalProperties": False,
        "properties": {
            "id": {
                "type": "string",
                "pattern": id_pattern,
            },
            "kind": {
                "enum": [
                    "dbt_project_bundle",
                    "dbt_manifest",
                    "dbt_selection_lock",
                ]
            },
            locator: artifact_ref,
            "sha256": {"$ref": "#/$defs/sha256"},
            "bytes": {
                "type": "integer",
                "minimum": 1,
                "maximum": 256 * 1024 * 1024,
            },
            "media_type": {
                "type": "string",
                "minLength": 1,
                "maxLength": 200,
            },
        },
    }


def _relative_artifact_path() -> dict[str, str]:
    return {"type": "string", "pattern": RELEASE_ARTIFACT_PATH_PATTERN}


def _release_artifact_locator_schema(
    locator: str,
    *,
    properties: dict[str, Any],
) -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["id", "sha256", locator],
        "additionalProperties": True,
        "properties": properties,
    }


__all__ = [
    "airflow_deployment_index_v2_defs",
    "checksum_schema",
    "deployment_set_v2_defs",
    "deployment_workload_inventory_v2_schema",
    "identity_schema",
    "nullable_sha256_schema",
    "release_artifacts_schema",
    "release_artifacts_v2_schema",
    "release_defs",
    "release_provenance_schema",
    "release_v2_defs",
    "trust_tier_mirror_guards",
    "trust_tier_schema",
]
