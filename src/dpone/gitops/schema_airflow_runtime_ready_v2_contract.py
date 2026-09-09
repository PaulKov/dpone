"""Runtime-ready schema with concrete artifact-attestation evidence."""

from __future__ import annotations

from dpone.gitops.schema_airflow_runtime_init_fetch_contracts import (
    bounded_token_schema,
    ready_artifacts_schema,
    runtime_ready_defs,
)
from dpone.gitops.schema_contract_primitives import (
    GitOpsSchemaContract,
    artifact_registry_ref_schema,
    documented_contract,
)


def runtime_fetch_ready_v2_contract() -> GitOpsSchemaContract:
    verification = {
        "type": "object",
        "required": [
            "checksums",
            "effective_attestation_requirement",
            "attestations",
            "attestation_decision_sha256",
            "runtime_payload_sha256",
            "artifact_attestation",
        ],
        "additionalProperties": False,
        "properties": {
            "checksums": {"const": "passed"},
            "effective_attestation_requirement": {"const": "required"},
            "attestations": {"const": "passed"},
            "attestation_decision_sha256": {"$ref": "#/$defs/sha256"},
            "runtime_payload_sha256": {"$ref": "#/$defs/sha256"},
            "artifact_attestation": {
                "type": "object",
                "required": [
                    "subject_kind",
                    "backend",
                    "attestation_id",
                    "verification_sha256",
                    "observed_claims",
                    "unobserved_claims",
                ],
                "additionalProperties": False,
                "properties": {
                    "subject_kind": {"const": "airflow_deployment"},
                    "backend": {"const": "cosign_public_key"},
                    "attestation_id": {"$ref": "#/$defs/sha256"},
                    "verification_sha256": {"$ref": "#/$defs/sha256"},
                    "observed_claims": _subject_claims_schema(),
                    "unobserved_claims": _subject_claims_schema(),
                },
            },
        },
    }
    contract = documented_contract(
        name="runtime-fetch-ready-v2",
        kind="dpone.runtime-fetch-ready.v2",
        title="dpone GitOps attested runtime fetch ready manifest",
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
            "schema": {"const": "dpone.runtime-fetch-ready.v2"},
            **{
                field: {"$ref": "#/$defs/sha256"}
                for field in (
                    "plan_sha256",
                    "release_id",
                    "deployment_id",
                    "runtime_image_digest",
                    "registry_config_sha256",
                    "pack_fingerprint",
                )
            },
            "trust_policy_sha256": {
                "anyOf": [
                    {"$ref": "#/$defs/sha256"},
                    {"type": "null"},
                ]
            },
            "trust_tier": {"enum": ["production", "non_production"]},
            "artifact_registry_ref": artifact_registry_ref_schema(),
            "workload_id": bounded_token_schema(),
            "artifacts": ready_artifacts_schema(),
            "verification": verification,
        },
        defs=runtime_ready_defs(),
        additional_properties=False,
    )
    contract.schema["allOf"] = [
        {
            "if": {
                "properties": {
                    "trust_tier": {"const": "production"},
                }
            },
            "then": {
                "properties": {
                    "trust_policy_sha256": {"$ref": "#/$defs/sha256"},
                }
            },
        }
    ]
    return contract


def _subject_claims_schema() -> dict[str, object]:
    return {
        "type": "array",
        "items": {
            "enum": [
                "release_id",
                "deployment_id",
                "environment",
                "artifact_registry_ref",
                "registry_scope_id",
                "release_set_sha256",
                "deployment_sha256",
                "airflow_index_sha256",
                "runtime_image_digest",
            ]
        },
        "uniqueItems": True,
    }


__all__ = ["runtime_fetch_ready_v2_contract"]
