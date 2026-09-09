"""Public JSON Schema for offline runtime-artifact trust policy v2."""

from __future__ import annotations

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract


def runtime_artifact_trust_policy_v2_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="runtime-artifact-trust-policy-v2",
        kind="dpone.runtime-artifact-trust-policy.v2",
        title="dpone GitOps runtime artifact trust policy v2",
        required=("schema", "trust_tier", "attestations", "verifier"),
        additional_properties=False,
        properties={
            "schema": {"const": "dpone.runtime-artifact-trust-policy.v2"},
            "trust_tier": {"enum": ["production", "non_production"]},
            "attestations": {"enum": ["optional", "required_for_prod"]},
            "verifier": github_artifact_attestation_verifier_schema(),
        },
    )


def runtime_artifact_attestation_verification_contract() -> GitOpsSchemaContract:
    result = documented_contract(
        name="runtime-artifact-attestation-verification",
        kind="dpone.runtime-artifact-attestation-verification.v1",
        title="dpone GitOps runtime artifact attestation verification",
        required=("schema", "passed", "changes", "errors", "status"),
        additional_properties=False,
        properties={
            "schema": {"const": "dpone.runtime-artifact-attestation-verification.v1"},
            "passed": {"type": "boolean"},
            "changes": {"type": "array", "maxItems": 0},
            "errors": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [
                        "schema",
                        "code",
                        "stage",
                        "severity",
                        "message",
                        "fixes",
                    ],
                    "additionalProperties": True,
                    "properties": {
                        "schema": {"const": "dpone.error.v1"},
                        "code": {
                            "type": "string",
                            "pattern": "^DPONE_(?:ARTIFACT_ATTESTATION|ARTIFACT_TRUST_POLICY)_[A-Z0-9_]+$",
                        },
                        "stage": {"const": "airflow_artifact_attestation_verify"},
                        "severity": {"const": "error"},
                        "message": {"type": "string", "minLength": 1},
                        "docs_url": {"type": "string", "minLength": 1},
                        "fixes": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "object",
                                "required": ["id", "safety"],
                                "additionalProperties": True,
                                "properties": {
                                    "id": {"type": "string", "minLength": 1},
                                    "safety": {"enum": ["safe", "manual", "destructive"]},
                                },
                            },
                        },
                    },
                },
            },
            "status": {"enum": ["passed", "failed"]},
            "subject_sha256": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
            "verifier": {"const": "github_artifact_attestation_v1"},
            "verifier_version": {"type": "string", "pattern": "^\\d+\\.\\d+\\.\\d+$"},
            "verified_attestations": {"type": "integer", "minimum": 1},
        },
    )
    result.schema["allOf"] = [
        {
            "if": {
                "properties": {"status": {"const": "passed"}},
                "required": ["status"],
            },
            "then": {
                "properties": {
                    "passed": {"const": True},
                    "errors": {"maxItems": 0},
                },
                "required": [
                    "subject_sha256",
                    "verifier",
                    "verifier_version",
                    "verified_attestations",
                ],
            },
        },
        {
            "if": {
                "properties": {"status": {"const": "failed"}},
                "required": ["status"],
            },
            "then": {
                "properties": {
                    "passed": {"const": False},
                    "errors": {"minItems": 1},
                },
            },
        },
    ]
    return result


def github_artifact_attestation_verifier_schema() -> dict[str, object]:
    return {
        "type": "object",
        "required": [
            "backend",
            "repository",
            "signer_workflow",
            "signer_digest",
            "predicate_type",
            "cert_oidc_issuer",
            "deny_self_hosted_runners",
            "trusted_root",
            "gh",
        ],
        "additionalProperties": False,
        "properties": {
            "backend": {"const": "github_artifact_attestation_v1"},
            "repository": {
                "type": "string",
                "pattern": "^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$",
            },
            "signer_workflow": {
                "type": "string",
                "pattern": "^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/\\.github/workflows/"
                "(?:[A-Za-z0-9_.-]+/)*[A-Za-z0-9_.-]+\\.ya?ml$",
                "maxLength": 300,
            },
            "signer_digest": {"type": "string", "pattern": "^[0-9a-f]{40}$"},
            "predicate_type": {"const": "https://slsa.dev/provenance/v1"},
            "cert_oidc_issuer": {"const": "https://token.actions.githubusercontent.com"},
            "deny_self_hosted_runners": {"const": True},
            "trusted_root": trusted_root_schema(),
            "gh": github_cli_policy_schema(),
        },
    }


def trusted_root_schema() -> dict[str, object]:
    return {
        "type": "object",
        "required": ["encoding", "content", "sha256", "generated_at", "refresh_after"],
        "additionalProperties": False,
        "properties": {
            "encoding": {"const": "base64"},
            "content": {
                "type": "string",
                "minLength": 1,
                "maxLength": 699052,
                "pattern": "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$",
            },
            "sha256": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
            "generated_at": {"type": "string", "format": "date-time"},
            "refresh_after": {"type": "string", "format": "date-time"},
        },
    }


def github_cli_policy_schema() -> dict[str, object]:
    return {
        "type": "object",
        "required": ["minimum_version", "maximum_version_exclusive", "timeout_seconds"],
        "additionalProperties": False,
        "properties": {
            "minimum_version": {"type": "string", "pattern": "^\\d+\\.\\d+\\.\\d+$"},
            "maximum_version_exclusive": {
                "type": "string",
                "pattern": "^\\d+\\.\\d+\\.\\d+$",
            },
            "timeout_seconds": {
                "type": "integer",
                "minimum": 1,
                "maximum": 60,
            },
        },
    }


__all__ = [
    "runtime_artifact_attestation_verification_contract",
    "runtime_artifact_trust_policy_v2_contract",
]
