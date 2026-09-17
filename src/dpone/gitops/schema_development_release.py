"""Closed schema for delivery-only development dbt workspace releases."""

from __future__ import annotations

from dpone.contracts.development_delivery_authority import (
    DEVELOPMENT_AUTHORITY_SCHEMA,
    DEVELOPMENT_COMPOSITION_PROFILE,
    DEVELOPMENT_RELEASE_SCHEMA,
)
from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract
from dpone.gitops.schema_release_deployment_definitions import release_artifacts_v2_schema, release_v2_defs
from dpone.gitops.schema_release_set_promotion import COMPACT_PROMOTION_SCHEMA


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


__all__ = ["development_release_set_contract"]
