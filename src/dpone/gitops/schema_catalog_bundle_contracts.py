"""Registered GitOps contracts for signed catalogs and extension evidence."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract


def catalog_bundle_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        _bundle_contract(),
        _trust_policy_contract(),
        _verification_contract(),
        _check_receipt_contract(),
        _conformance_request_contract(),
        _conformance_contract(),
    )


def _bundle_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="catalog-bundle",
        kind="dpone.catalog-bundle.v1",
        title="dpone GitOps catalog bundle",
        required=("schema", "bundle_id", "kind", "publisher_id", "environment", "entrypoint", "artifacts"),
        additional_properties=False,
        properties={
            "schema": {"const": "dpone.catalog-bundle.v1"},
            "bundle_id": _sha256(),
            "kind": {"enum": ["recipe_catalog", "connection_registry"]},
            "publisher_id": _text(),
            "environment": {"type": ["string", "null"]},
            "entrypoint": _text(),
            "artifacts": {"type": "array", "minItems": 1, "maxItems": 1001, "items": _artifact()},
        },
    )


def _trust_policy_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="catalog-trust-policy",
        kind="dpone.catalog-trust-policy.v1",
        title="dpone GitOps catalog trust policy",
        required=(
            "schema",
            "policy_id",
            "allowed_kinds",
            "allowed_publishers",
            "certificate_identity",
            "certificate_oidc_issuer",
            "trusted_root_sha256",
            "verifier",
        ),
        additional_properties=False,
        properties={
            "schema": {"const": "dpone.catalog-trust-policy.v1"},
            "policy_id": _text(),
            "allowed_kinds": {
                "type": "array",
                "minItems": 1,
                "uniqueItems": True,
                "items": {"enum": ["recipe_catalog", "connection_registry"]},
            },
            "allowed_publishers": _unique_text_array(),
            "allowed_environments": _unique_text_array(),
            "certificate_identity": _text(),
            "certificate_oidc_issuer": _text(),
            "trusted_root_sha256": _sha256(),
            "verifier": _object(
                ("minimum_version", "maximum_version_exclusive", "timeout_seconds"),
                {
                    "minimum_version": {"type": "string", "pattern": "^[0-9]+\\.[0-9]+\\.[0-9]+$"},
                    "maximum_version_exclusive": {
                        "type": "string",
                        "pattern": "^[0-9]+\\.[0-9]+\\.[0-9]+$",
                    },
                    "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 300},
                },
            ),
        },
    )


def _verification_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="catalog-bundle-verification",
        kind="dpone.catalog-bundle-verification.v1",
        title="dpone GitOps catalog bundle verification",
        required=(
            "schema",
            "decision",
            "code",
            "bundle_id",
            "bundle_manifest_sha256",
            "kind",
            "publisher_id",
            "environment",
            "policy_fingerprint",
            "verified_artifacts",
            "verifier_version",
            "verified_at",
            "errors",
        ),
        additional_properties=False,
        properties={
            "schema": {"const": "dpone.catalog-bundle-verification.v1"},
            "decision": {"enum": ["verified", "invalid", "unverified"]},
            "code": _text(),
            "bundle_id": {"oneOf": [_sha256(), {"type": "null"}]},
            "bundle_manifest_sha256": _sha256(),
            "kind": {"type": ["string", "null"]},
            "publisher_id": {"type": ["string", "null"]},
            "environment": {"type": ["string", "null"]},
            "policy_fingerprint": _sha256(),
            "verified_artifacts": {"type": "integer", "minimum": 0},
            "verifier_version": {"type": ["string", "null"]},
            "verified_at": _text(),
            "errors": {"type": "array", "items": _object(("code", "message"), {"code": _text(), "message": _text()})},
        },
    )


def _check_receipt_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="extension-check-receipt",
        kind="dpone.extension-check-receipt.v1",
        title="dpone GitOps extension check receipt",
        required=("schema", "check", "subject_digest", "status", "producer", "command"),
        additional_properties=False,
        properties={
            "schema": {"const": "dpone.extension-check-receipt.v1"},
            "check": _text(),
            "subject_digest": _sha256(),
            "status": {"enum": ["PASS", "FAIL", "UNVERIFIED", "SKIP", "N/A"]},
            "producer": _text(),
            "command": _text(),
            "code": {"type": "string"},
            "message": {"type": "string"},
        },
    )


def _conformance_request_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="extension-conformance-request",
        kind="dpone.extension-conformance-request.v1",
        title="dpone GitOps extension conformance request",
        required=("schema", "profile", "subject", "evidence"),
        additional_properties=False,
        properties={
            "schema": {"const": "dpone.extension-conformance-request.v1"},
            "profile": {"enum": ["airflow_provider", "recipe_catalog", "credential_resolver", "route"]},
            "subject": _subject(),
            "evidence": {
                "type": "array",
                "minItems": 1,
                "maxItems": 20,
                "items": _object(
                    ("check", "artifact_ref", "sha256", "expected_schema"),
                    {
                        "check": _text(),
                        "artifact_ref": _text(),
                        "sha256": _sha256(),
                        "expected_schema": {
                            "enum": ["dpone.extension-check-receipt.v1", "dpone.catalog-bundle-verification.v1"]
                        },
                    },
                ),
            },
        },
    )


def _conformance_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="extension-conformance",
        kind="dpone.extension-conformance.v1",
        title="dpone GitOps extension conformance",
        required=("schema", "conformance_id", "profile", "subject", "status", "checks", "evaluated_at"),
        additional_properties=False,
        properties={
            "schema": {"const": "dpone.extension-conformance.v1"},
            "conformance_id": _sha256(),
            "profile": {"enum": ["airflow_provider", "recipe_catalog", "credential_resolver", "route"]},
            "subject": _subject(),
            "status": {"enum": ["PASS", "FAIL", "UNVERIFIED"]},
            "checks": {"type": "array", "minItems": 1},
            "evaluated_at": _text(),
        },
    )


def _artifact() -> dict[str, Any]:
    return _object(
        ("logical_id", "path", "sha256", "size_bytes"),
        {
            "logical_id": _text(),
            "path": _text(),
            "sha256": _sha256(),
            "size_bytes": {"type": "integer", "minimum": 0, "maximum": 4194304},
        },
    )


def _subject() -> dict[str, Any]:
    return _object(("id", "version", "digest"), {"id": _text(), "version": _text(), "digest": _sha256()})


def _object(required: tuple[str, ...], properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "required": list(required), "properties": properties, "additionalProperties": False}


def _text() -> dict[str, Any]:
    return {"type": "string", "minLength": 1}


def _sha256() -> dict[str, Any]:
    return {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}


def _unique_text_array() -> dict[str, Any]:
    return {"type": "array", "minItems": 1, "uniqueItems": True, "items": _text()}


__all__ = ["catalog_bundle_schema_contracts"]
