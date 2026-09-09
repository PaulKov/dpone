"""JSON Schema contracts for signed route authorization."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract

_SHA256 = "^sha256:[0-9a-f]{64}$"


def route_attestation_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        route_attestation_contract(),
        route_attestation_policy_contract(),
        route_attestation_verification_contract(),
    )


def route_attestation_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="route-attestation",
        kind="dpone.route-attestation.v1",
        title="dpone GitOps route attestation",
        required=("schema", "attestation_id", "claims"),
        properties={
            "schema": {"const": "dpone.route-attestation.v1"},
            "attestation_id": _digest(),
            "claims": {
                "type": "object",
                "required": ["route", "certification", "subject", "validity"],
                "additionalProperties": False,
                "properties": {
                    "route": _route(),
                    "certification": _certification(),
                    "subject": _subject(),
                    "validity": _validity(),
                },
            },
        },
        additional_properties=False,
    )


def route_attestation_policy_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="route-attestation-policy",
        kind="dpone.route-attestation-policy.v1",
        title="dpone GitOps route attestation policy",
        required=(
            "schema",
            "backend",
            "certificate_identity",
            "certificate_oidc_issuer",
            "trusted_root",
            "cosign",
            "allowed_environments",
            "allowed_authorization_profiles",
            "allowed_certification_profiles",
            "minimum_certification_level",
            "max_validity_seconds",
            "clock_skew_seconds",
            "revoked_attestation_ids",
            "revoked_certificate_identities",
        ),
        properties={
            "schema": {"const": "dpone.route-attestation-policy.v1"},
            "backend": {"const": "cosign_keyless_v1"},
            "certificate_identity": _text(),
            "certificate_oidc_issuer": _text(),
            "trusted_root": _object(
                ("path", "sha256"),
                {"path": _text(), "sha256": _digest()},
            ),
            "cosign": _object(
                ("minimum_version", "maximum_version_exclusive", "timeout_seconds"),
                {
                    "minimum_version": {"type": "string", "pattern": "^3\\.[0-9]+\\.[0-9]+$"},
                    "maximum_version_exclusive": {"const": "4.0.0"},
                    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 120},
                },
            ),
            "allowed_environments": _unique_texts(min_items=1),
            "allowed_authorization_profiles": _unique_texts(min_items=1),
            "allowed_certification_profiles": _unique_texts(min_items=1),
            "minimum_certification_level": {"const": "certified"},
            "max_validity_seconds": {"type": "integer", "minimum": 1, "maximum": 604800},
            "clock_skew_seconds": {"type": "integer", "minimum": 0, "maximum": 3600},
            "revoked_attestation_ids": {**_unique_texts(), "items": _digest()},
            "revoked_certificate_identities": _unique_texts(),
        },
        additional_properties=False,
    )


def route_attestation_verification_contract() -> GitOpsSchemaContract:
    nullable_text = {"type": ["string", "null"]}
    return documented_contract(
        name="route-attestation-verification",
        kind="dpone.route-attestation-verification.v1",
        title="dpone GitOps route attestation verification",
        required=(
            "schema",
            "decision",
            "code",
            "message",
            "attestation_id",
            "attestation_sha256",
            "certification_bundle_sha256",
            "policy_fingerprint",
            "route_id",
            "release_id",
            "deployment_id",
            "environment",
            "authorization_profile",
            "signer",
            "validity",
            "verified_at",
            "errors",
        ),
        properties={
            "schema": {"const": "dpone.route-attestation-verification.v1"},
            "decision": {"enum": ["verified", "invalid", "unverified"]},
            "code": _text(),
            "message": _text(),
            "attestation_id": {"anyOf": [_digest(), {"type": "null"}]},
            "attestation_sha256": _digest(),
            "certification_bundle_sha256": _digest(),
            "policy_fingerprint": _digest(),
            "route_id": nullable_text,
            "release_id": {"anyOf": [_digest(), {"type": "null"}]},
            "deployment_id": {"anyOf": [_digest(), {"type": "null"}]},
            "environment": nullable_text,
            "authorization_profile": nullable_text,
            "signer": _object(
                (),
                {
                    "backend": _text(),
                    "certificate_identity": _text(),
                    "certificate_oidc_issuer": _text(),
                    "verifier_version": nullable_text,
                },
            ),
            "validity": _object(
                (),
                {
                    "not_before": nullable_text,
                    "expires_at": nullable_text,
                },
            ),
            "verified_at": {"type": "string", "format": "date-time"},
            "errors": {
                "type": "array",
                "items": _object(
                    ("code", "message"),
                    {"code": _text(), "message": _text()},
                ),
            },
        },
        additional_properties=False,
    )


def _route() -> dict[str, Any]:
    fields = (
        "route_id",
        "source",
        "sink",
        "strategy",
        "transport",
        "schema_evolution",
        "airflow_runtime_mode",
        "sampling_mode",
    )
    return _object(fields, {field: _text() for field in fields})


def _certification() -> dict[str, Any]:
    return _object(
        ("bundle_sha256", "profile", "level"),
        {"bundle_sha256": _digest(), "profile": _text(), "level": {"const": "certified"}},
    )


def _subject() -> dict[str, Any]:
    return _object(
        ("release_id", "deployment_id", "environment", "runtime_image_digest", "authorization_profile"),
        {
            "release_id": _digest(),
            "deployment_id": _digest(),
            "environment": _text(),
            "runtime_image_digest": _digest(),
            "authorization_profile": _text(),
        },
    )


def _validity() -> dict[str, Any]:
    date_time = {"type": "string", "format": "date-time"}
    return _object(("issued_at", "not_before", "expires_at"), {field: date_time for field in date_time_fields()})


def date_time_fields() -> tuple[str, ...]:
    return ("issued_at", "not_before", "expires_at")


def _object(required: tuple[str, ...], properties: dict[str, Any]) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
    }
    if required:
        schema["required"] = list(required)
    return schema


def _digest() -> dict[str, str]:
    return {"type": "string", "pattern": _SHA256}


def _text() -> dict[str, Any]:
    return {"type": "string", "minLength": 1, "maxLength": 1024}


def _unique_texts(*, min_items: int = 0) -> dict[str, Any]:
    return {
        "type": "array",
        "items": _text(),
        "uniqueItems": True,
        "minItems": min_items,
        "maxItems": 1024,
    }


__all__ = ["route_attestation_schema_contracts"]
