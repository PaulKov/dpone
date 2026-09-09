from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract


from typing import Any

from dpone.gitops.schema_airflow_init_fetch_execution import (
    bounded_execution_token_schema,
    init_fetch_execution_schema,
)
from dpone.gitops.schema_contract_primitives import artifact_registry_ref_schema, documented_contract
from dpone.gitops.schema_runtime_artifact_delivery import (
    config_map_ref_schema,
    init_fetch_identity_schema,
    runtime_image_ref_schema,
)
from dpone.gitops.schema_runtime_connection_context import (
    exact_runtime_artifact_schema,
    runtime_connection_artifact_schema,
    runtime_pinned_cache_ref_schema,
)


def airflow_runtime_init_fetch_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        runtime_init_fetch_plan_contract(),
        runtime_init_fetch_plan_v2_contract(),
        runtime_init_fetch_plan_v3_contract(),
        runtime_fetch_ready_contract(),
    )


def runtime_init_fetch_plan_contract() -> GitOpsSchemaContract:
    return _runtime_init_fetch_plan_contract(version=1)


def runtime_init_fetch_plan_v2_contract() -> GitOpsSchemaContract:
    return _runtime_init_fetch_plan_contract(version=2)


def runtime_init_fetch_plan_v3_contract() -> GitOpsSchemaContract:
    return _runtime_init_fetch_plan_contract(version=3)


def _runtime_init_fetch_plan_contract(*, version: int) -> GitOpsSchemaContract:
    suffix = f"-v{version}" if version > 1 else ""
    kind = f"dpone.airflow-runtime-init-fetch-plan.v{version}"
    has_runtime_payloads = version >= 2
    runtime_payload_fields = ("runtime_payloads",) if has_runtime_payloads else ()
    result = documented_contract(
        name=f"airflow-runtime-init-fetch-plan{suffix}",
        kind=kind,
        title=f"dpone GitOps Airflow runtime init-fetch plan{f' v{version}' if version > 1 else ''}",
        required=(
            "schema",
            "environment",
            "trust_tier",
            "release_id",
            "deployment_id",
            "runtime_image",
            "registry",
            "trust_policy",
            "identity",
            "release",
            "deployment",
            "binding_set",
            "connection_registry",
            "credential_runtime",
            "workload_pack",
            "execution",
            "verify",
            *runtime_payload_fields,
        ),
        properties={
            "schema": {"const": kind},
            "environment": {
                "type": "string",
                "pattern": "^[a-z0-9][a-z0-9_-]{0,62}$",
            },
            "trust_tier": {"enum": ["production", "non_production"]},
            "release_id": {"$ref": "#/$defs/sha256"},
            "deployment_id": {"$ref": "#/$defs/sha256"},
            "runtime_image": runtime_image_schema(),
            "registry": registry_schema(),
            "trust_policy": {
                "anyOf": [
                    config_map_ref_schema(),
                    {"type": "null"},
                ]
            },
            "identity": init_fetch_identity_schema(strict=True),
            "release": {"$ref": "#/$defs/exactArtifact"},
            "deployment": {"$ref": "#/$defs/exactArtifact"},
            "binding_set": runtime_connection_artifact_schema("binding-set.json"),
            "connection_registry": runtime_connection_artifact_schema("connection-registry.json"),
            "credential_runtime": runtime_connection_artifact_schema("credential-runtime.json"),
            "workload_pack": {"$ref": "#/$defs/workloadPack"},
            "execution": init_fetch_execution_schema(explicit_hook_execution=version >= 3),
            "verify": verification_request_schema(),
            **(
                {
                    "runtime_payloads": {
                        "type": "array",
                        "minItems": 0 if version >= 3 else 1,
                        "maxItems": 16,
                        "items": {"$ref": "#/$defs/runtimePayload"},
                    }
                }
                if has_runtime_payloads
                else {}
            ),
        },
        defs=runtime_plan_defs(include_runtime_payloads=has_runtime_payloads),
        additional_properties=False,
    )
    result.schema["allOf"] = [
        {
            "if": {
                "properties": {"trust_tier": {"const": "production"}},
                "required": ["trust_tier"],
            },
            "then": {
                "properties": {
                    "trust_policy": {"not": {"type": "null"}},
                    "verify": {
                        "properties": {
                            "attestations": {"const": "required_for_prod"},
                        }
                    },
                }
            },
        }
    ]
    return result


def runtime_fetch_ready_contract() -> GitOpsSchemaContract:
    result = documented_contract(
        name="runtime-fetch-ready",
        kind="dpone.runtime-fetch-ready.v1",
        title="dpone GitOps verified runtime fetch ready manifest",
        required=(
            "schema",
            "plan_sha256",
            "release_id",
            "deployment_id",
            "runtime_image_digest",
            "trust_tier",
            "artifact_registry_ref",
            "registry_config_sha256",
            "trust_policy_sha256",
            "workload_id",
            "pack_fingerprint",
            "artifacts",
            "verification",
        ),
        properties={
            "schema": {"const": "dpone.runtime-fetch-ready.v1"},
            "plan_sha256": {"$ref": "#/$defs/sha256"},
            "release_id": {"$ref": "#/$defs/sha256"},
            "deployment_id": {"$ref": "#/$defs/sha256"},
            "runtime_image_digest": {"$ref": "#/$defs/sha256"},
            "trust_tier": {"enum": ["production", "non_production"]},
            "artifact_registry_ref": artifact_registry_ref_schema(),
            "registry_config_sha256": {"$ref": "#/$defs/sha256"},
            "trust_policy_sha256": {
                "anyOf": [
                    {"$ref": "#/$defs/sha256"},
                    {"type": "null"},
                ]
            },
            "workload_id": bounded_execution_token_schema(),
            "pack_fingerprint": {"$ref": "#/$defs/sha256"},
            "artifacts": ready_artifacts_schema(),
            "verification": ready_verification_schema(),
        },
        defs=runtime_ready_defs(),
        additional_properties=False,
    )
    result.schema["allOf"] = [
        {
            "if": {
                "properties": {"trust_tier": {"const": "production"}},
                "required": ["trust_tier"],
            },
            "then": {
                "properties": {
                    "trust_policy_sha256": {"$ref": "#/$defs/sha256"},
                    "verification": {
                        "properties": {
                            "effective_attestation_requirement": {"const": "required"},
                            "attestations": {"const": "passed"},
                        }
                    },
                }
            },
        }
    ]
    return result


def runtime_image_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["ref", "digest"],
        "additionalProperties": False,
        "properties": {
            "ref": runtime_image_ref_schema(),
            "digest": {"$ref": "#/$defs/sha256"},
        },
    }


def registry_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["logical_ref", "configuration"],
        "additionalProperties": False,
        "properties": {
            "logical_ref": artifact_registry_ref_schema(),
            "configuration": config_map_ref_schema(),
        },
    }


def verification_request_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["checksums", "attestations"],
        "additionalProperties": False,
        "properties": {
            "checksums": {"const": "required"},
            "attestations": {"enum": ["optional", "required_for_prod"]},
        },
    }


def runtime_plan_defs(*, include_runtime_payloads: bool = False) -> dict[str, Any]:
    exact_artifact = exact_artifact_schema()
    defs = {
        "sha256": canonical_sha256_schema(),
        "exactArtifact": exact_artifact,
        "workloadPack": {
            **exact_artifact,
            "required": [
                "id",
                "artifact_ref",
                "sha256",
                "bytes",
                "pack_fingerprint",
            ],
            "properties": {
                "id": bounded_execution_token_schema(),
                **exact_artifact["properties"],
                "pack_fingerprint": {"$ref": "#/$defs/sha256"},
            },
        },
    }
    if include_runtime_payloads:
        defs["runtimePayload"] = {
            "type": "object",
            "required": [
                "id",
                "kind",
                "artifact_ref",
                "sha256",
                "bytes",
                "media_type",
            ],
            "additionalProperties": False,
            "properties": {
                "id": bounded_execution_token_schema(),
                "kind": {
                    "enum": [
                        "dbt_project_bundle",
                        "dbt_manifest",
                        "dbt_selection_lock",
                    ]
                },
                "artifact_ref": runtime_pinned_cache_ref_schema(),
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
    return defs


def runtime_ready_defs() -> dict[str, Any]:
    return {
        "sha256": canonical_sha256_schema(),
        "readyArtifact": {
            "type": "object",
            "required": ["artifact_ref", "locator", "sha256", "bytes"],
            "additionalProperties": False,
            "properties": {
                "artifact_ref": runtime_pinned_cache_ref_schema(),
                "locator": {
                    "type": "string",
                    "pattern": r"^payload/[^\s\\]+$",
                    "not": {"pattern": r"(^|/)\.\.(/|$)"},
                },
                "sha256": {"$ref": "#/$defs/sha256"},
                "bytes": {"type": "integer", "minimum": 1},
            },
        },
    }


def exact_artifact_schema() -> dict[str, Any]:
    return exact_runtime_artifact_schema(
        artifact_ref_schema=runtime_pinned_cache_ref_schema(),
    )


def ready_artifacts_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["release", "deployment", "workload_pack"],
        "additionalProperties": False,
        "properties": {
            "release": {"$ref": "#/$defs/readyArtifact"},
            "deployment": {"$ref": "#/$defs/readyArtifact"},
            "workload_pack": {"$ref": "#/$defs/readyArtifact"},
        },
    }


def ready_verification_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "checksums",
            "effective_attestation_requirement",
            "attestations",
            "attestation_decision_sha256",
            "runtime_payload_sha256",
        ],
        "additionalProperties": False,
        "properties": {
            "checksums": {"const": "passed"},
            "effective_attestation_requirement": {
                "enum": ["optional", "required"],
            },
            "attestations": {"enum": ["not_required", "passed"]},
            "attestation_decision_sha256": {"$ref": "#/$defs/sha256"},
            "runtime_payload_sha256": {"$ref": "#/$defs/sha256"},
        },
        "allOf": [
            {
                "if": {
                    "properties": {
                        "effective_attestation_requirement": {
                            "const": "required",
                        }
                    },
                    "required": ["effective_attestation_requirement"],
                },
                "then": {
                    "properties": {"attestations": {"const": "passed"}},
                },
            },
            {
                "if": {
                    "properties": {
                        "effective_attestation_requirement": {
                            "const": "optional",
                        }
                    },
                    "required": ["effective_attestation_requirement"],
                },
                "then": {
                    "properties": {"attestations": {"const": "not_required"}},
                },
            },
        ],
    }


def canonical_sha256_schema() -> dict[str, str]:
    return {
        "type": "string",
        "pattern": "^sha256:[0-9a-f]{64}$",
    }


def bounded_token_schema() -> dict[str, Any]:
    return bounded_execution_token_schema()


__all__ = ["airflow_runtime_init_fetch_schema_contracts"]
