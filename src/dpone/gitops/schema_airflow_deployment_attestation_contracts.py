"""Public schemas for signed Airflow deployment provenance."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract

_TRUE_END = "(?![\\s\\S])"
_CONTROL_FREE_TRIMMED = (
    f"^[^\\s\\u0000-\\u001F\\u007F](?:[^\\u0000-\\u001F\\u007F]*[^\\s\\u0000-\\u001F\\u007F])?{_TRUE_END}"
)
_DIGEST = {"type": "string", "pattern": f"^sha256:[0-9a-f]{{64}}{_TRUE_END}"}
_TEXT = {
    "type": "string",
    "minLength": 1,
    "maxLength": 512,
    "pattern": _CONTROL_FREE_TRIMMED,
}


def airflow_deployment_attestation_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        _attestation_contract(),
        _verification_contract(),
        _trust_policy_contract(),
        _trust_policy_render_contract(),
        _prepare_contract(),
        _marker_contract(),
        _publish_contract(),
        _error_contract(),
    )


def _attestation_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-artifact-attestation",
        kind="dpone.airflow-artifact-attestation.v1",
        title="dpone GitOps signed Airflow artifact statement",
        required=("schema", "attestation_id", "claims"),
        properties={
            "schema": {"const": "dpone.airflow-artifact-attestation.v1"},
            "attestation_id": _DIGEST,
            "claims": {
                "type": "object",
                "required": ["subject", "source", "publication", "issued_at"],
                "additionalProperties": False,
                "properties": {
                    "subject": _subject_schema(),
                    "source": _exact_object(
                        ("project", "ref", "git_sha"),
                        {
                            "project": _TEXT,
                            "ref": _TEXT,
                            "git_sha": {
                                "type": "string",
                                "pattern": f"^[0-9a-f]{{40,64}}{_TRUE_END}",
                            },
                        },
                    ),
                    "publication": _exact_object(
                        ("evidence_sha256", "verification_mode"),
                        {
                            "evidence_sha256": _DIGEST,
                            "verification_mode": {"const": "remote_readback_sha256"},
                        },
                    ),
                    "issued_at": _bounded_text(40),
                },
            },
        },
        additional_properties=False,
    )


def _verification_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-artifact-attestation-verification",
        kind="dpone.airflow-artifact-attestation-verification.v1",
        title="dpone GitOps Airflow artifact-attestation verification receipt",
        required=(
            "schema",
            "decision",
            "code",
            "message",
            "decision_sha256",
            "attestation_id",
            "policy_fingerprint",
            "public_key_id",
            "public_key_sha256",
            "verifier_version",
            "verified_at",
            "errors",
        ),
        properties=_verification_properties(),
        additional_properties=False,
    )


def _trust_policy_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-deployment-trust-policy",
        kind="dpone.airflow-deployment-trust-policy.v1",
        title="dpone GitOps production Airflow artifact trust policy",
        required=(
            "schema",
            "trust_tier",
            "attestations",
            "backend",
            "trusted_public_keys",
            "cosign",
            "allowed_environments",
            "allowed_artifact_registry_refs",
            "allowed_registry_scope_ids",
            "allowed_source_projects",
            "allowed_source_refs",
            "revoked_attestation_ids",
            "revoked_public_key_ids",
        ),
        properties={
            "schema": {"const": "dpone.airflow-deployment-trust-policy.v1"},
            "trust_tier": {"const": "production"},
            "attestations": {"const": "required_for_prod"},
            "backend": {"const": "cosign_public_key_v1"},
            "trusted_public_keys": {
                "type": "object",
                "minProperties": 1,
                "maxProperties": 8,
                "propertyNames": {
                    "pattern": f"^[A-Za-z0-9][A-Za-z0-9_.-]{{0,127}}{_TRUE_END}",
                },
                "additionalProperties": _exact_object(
                    ("file", "sha256"),
                    {
                        "file": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 253,
                            "pattern": f"^[A-Za-z0-9][A-Za-z0-9_.-]{{0,252}}{_TRUE_END}",
                        },
                        "sha256": _DIGEST,
                    },
                ),
            },
            "cosign": _exact_object(
                ("minimum_version", "maximum_version_exclusive", "timeout_seconds"),
                {
                    "minimum_version": _bounded_text(32),
                    "maximum_version_exclusive": _bounded_text(32),
                    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 60},
                },
            ),
            "allowed_environments": _bounded_array(_TEXT, minimum=1, maximum=64),
            "allowed_artifact_registry_refs": _bounded_array(_TEXT, minimum=1, maximum=64),
            "allowed_registry_scope_ids": _bounded_array(_DIGEST, minimum=1, maximum=256),
            "allowed_source_projects": _bounded_array(_TEXT, minimum=1, maximum=64),
            "allowed_source_refs": _bounded_array(_TEXT, minimum=1, maximum=64),
            "revoked_attestation_ids": _bounded_array(_DIGEST, minimum=0, maximum=256),
            "revoked_public_key_ids": _bounded_array(_TEXT, minimum=0, maximum=64),
        },
        additional_properties=False,
    )


def _trust_policy_render_contract() -> GitOpsSchemaContract:
    return _self_service_contract(
        name="airflow-deployment-trust-policy-render",
        kind="dpone.airflow-deployment-trust-policy-render.v1",
        title="dpone GitOps canonical Airflow deployment trust-policy render receipt",
        detail_fields={
            "status": {"const": "rendered"},
            "policy_fingerprint": _DIGEST,
            "policy_sha256": _DIGEST,
            "policy_bytes": {"type": "integer", "minimum": 1, "maximum": 256 * 1024},
            "output_path": _TEXT,
            "warnings": {"type": "array", "maxItems": 0},
        },
    )


def _prepare_contract() -> GitOpsSchemaContract:
    return _self_service_contract(
        name="airflow-artifact-attestation-prepare",
        kind="dpone.airflow-artifact-attestation-prepare.v1",
        title="dpone GitOps canonical Airflow artifact-attestation preparation receipt",
        detail_fields={
            "status": {"const": "prepared"},
            "attestation_id": _DIGEST,
            "release_id": _DIGEST,
            "deployment_id": _DIGEST,
            "environment": _TEXT,
            "statement_sha256": _DIGEST,
            "statement_bytes": {"type": "integer", "minimum": 1, "maximum": 256 * 1024},
            "output_path": _TEXT,
            "warnings": {"type": "array", "maxItems": 0},
        },
    )


def _marker_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-artifact-attestation-marker",
        kind="dpone.airflow-artifact-attestation-marker.v1",
        title="dpone GitOps immutable Airflow artifact-attestation completion marker",
        required=(
            "schema",
            "deployment_id",
            "attestation_id",
            "attestation_sha256",
            "attestation_bytes",
            "sigstore_bundle_sha256",
            "sigstore_bundle_bytes",
        ),
        properties={
            "schema": {"const": "dpone.airflow-artifact-attestation-marker.v1"},
            "deployment_id": _DIGEST,
            "attestation_id": _DIGEST,
            "attestation_sha256": _DIGEST,
            "attestation_bytes": {"type": "integer", "minimum": 1, "maximum": 256 * 1024},
            "sigstore_bundle_sha256": _DIGEST,
            "sigstore_bundle_bytes": {"type": "integer", "minimum": 1, "maximum": 2 * 1024 * 1024},
        },
        additional_properties=False,
    )


def _publish_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-artifact-attestation-publish",
        kind="dpone.airflow-artifact-attestation-publish.v1",
        title="dpone GitOps immutable Airflow artifact-attestation publication receipt",
        required=(
            "schema",
            "passed",
            "changes",
            "status",
            "attestation_id",
            "environment",
            "deployment_id",
            "created_objects",
            "existing_equal_objects",
            "verified_objects",
            "verification",
            "errors",
        ),
        properties={
            "schema": {"const": "dpone.airflow-artifact-attestation-publish.v1"},
            "passed": {"const": True},
            "changes": {"type": "array", "maxItems": 0},
            "status": {"enum": ["published", "already_published"]},
            "attestation_id": _DIGEST,
            "environment": _TEXT,
            "deployment_id": _DIGEST,
            "created_objects": {"type": "integer", "minimum": 0},
            "existing_equal_objects": {"type": "integer", "minimum": 0},
            "verified_objects": {"type": "integer", "minimum": 3},
            "verification": _exact_object(
                tuple(_verification_properties()),
                _verification_properties(),
            ),
            "errors": {"type": "array", "maxItems": 0},
        },
        additional_properties=False,
    )


def _verification_properties() -> dict[str, Any]:
    return {
        "schema": {"const": "dpone.airflow-artifact-attestation-verification.v1"},
        "decision": {"enum": ["verified", "invalid", "unverified"]},
        "code": _TEXT,
        "message": _TEXT,
        "decision_sha256": _DIGEST,
        "attestation_id": _nullable(_DIGEST),
        "policy_fingerprint": _DIGEST,
        "public_key_id": _nullable(_TEXT),
        "public_key_sha256": _nullable(_DIGEST),
        "verifier_version": _nullable(_TEXT),
        "verified_at": _TEXT,
        "errors": {"type": "array", "maxItems": 1, "items": {"type": "object"}},
    }


def _error_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-artifact-attestation-error",
        kind="dpone.airflow-artifact-attestation-error.v1",
        title="dpone GitOps Airflow artifact-attestation command failure",
        required=("schema", "passed", "changes", "status", "errors", "warnings"),
        properties={
            "schema": {"const": "dpone.airflow-artifact-attestation-error.v1"},
            "passed": {"const": False},
            "changes": {"type": "array", "maxItems": 0},
            "status": {"const": "failed"},
            "errors": {"type": "array", "minItems": 1, "items": {"type": "object"}},
            "warnings": {"type": "array"},
        },
        additional_properties=False,
    )


def _self_service_contract(
    *,
    name: str,
    kind: str,
    title: str,
    detail_fields: dict[str, Any],
) -> GitOpsSchemaContract:
    properties = {
        "schema": {"const": kind},
        "passed": {"const": True},
        "changes": {"type": "array", "maxItems": 0},
        "errors": {"type": "array", "maxItems": 0},
        **detail_fields,
    }
    return documented_contract(
        name=name,
        kind=kind,
        title=title,
        required=tuple(properties),
        properties=properties,
        additional_properties=False,
    )


def _subject_schema() -> dict[str, Any]:
    fields = (
        "release_id",
        "deployment_id",
        "environment",
        "artifact_registry_ref",
        "registry_scope_id",
        "release_set_sha256",
        "deployment_sha256",
        "airflow_index_sha256",
        "runtime_image_digest",
    )
    properties = {field: _DIGEST for field in fields}
    properties["environment"] = _bounded_text(63)
    properties["artifact_registry_ref"] = _bounded_text(128)
    return _exact_object(
        fields,
        properties,
    )


def _exact_object(required: tuple[str, ...], properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "required": list(required),
        "additionalProperties": False,
        "properties": properties,
    }


def _nullable(schema: dict[str, Any]) -> dict[str, Any]:
    return {"anyOf": [schema, {"type": "null"}]}


def _bounded_text(maximum: int) -> dict[str, Any]:
    return {
        "type": "string",
        "minLength": 1,
        "maxLength": maximum,
        "pattern": _CONTROL_FREE_TRIMMED,
    }


def _bounded_array(
    item: dict[str, Any],
    *,
    minimum: int,
    maximum: int,
) -> dict[str, Any]:
    return {
        "type": "array",
        "minItems": minimum,
        "maxItems": maximum,
        "items": item,
        "uniqueItems": True,
    }


__all__ = ["airflow_deployment_attestation_schema_contracts"]
