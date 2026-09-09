"""JSON Schema contract for the route certification matrix projection."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract

_STATUS = ["experimental", "route-certified", "production-certified", "enterprise-certified"]
_EVIDENCE_STATUS = ["PASS", "FAIL", "UNVERIFIED"]


def route_certification_matrix_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="route-certification-matrix",
        kind="dpone.route-certification-matrix.v1",
        title="dpone GitOps route certification matrix",
        required=(
            "schema",
            "expected_commit",
            "evaluated_at",
            "has_input_failures",
            "counts",
            "rows",
            "errors",
        ),
        properties={
            "schema": {"const": "dpone.route-certification-matrix.v1"},
            "expected_commit": _text(),
            "evaluated_at": {"type": "string", "format": "date-time"},
            "has_input_failures": {"type": "boolean"},
            "counts": _counts(),
            "rows": {"type": "array", "items": {"$ref": "#/$defs/row"}, "maxItems": 10000},
            "errors": {"type": "array", "items": {"$ref": "#/$defs/error"}, "maxItems": 10000},
        },
        defs={
            "sha256": _sha256(),
            "nullable_sha256": {"anyOf": [{"$ref": "#/$defs/sha256"}, {"type": "null"}]},
            "nullable_text": {"type": ["string", "null"], "maxLength": 1024},
            "dimensions": _dimensions(),
            "proof": _proof(),
            "row": _row(),
            "error": _error(),
        },
        additional_properties=False,
    )


def route_matrix_claim_contract() -> GitOpsSchemaContract:
    route_fields = (
        "route_id",
        "source",
        "sink",
        "strategy",
        "transport",
        "schema_evolution",
        "airflow_runtime_mode",
        "sampling_mode",
    )
    return documented_contract(
        name="route-matrix-claim",
        kind="dpone.route-matrix-claim.v1",
        title="dpone GitOps content-bound route matrix claim",
        required=("schema", "release_id", "source_commit", "certified_at", "route"),
        properties={
            "schema": {"const": "dpone.route-matrix-claim.v1"},
            "release_id": _sha256(),
            "source_commit": {"type": "string", "pattern": "^(?:[0-9a-f]{40}|[0-9a-f]{64})$"},
            "certified_at": {"type": "string", "format": "date-time"},
            "route": _object(route_fields, {field: _text() for field in route_fields}),
        },
        additional_properties=False,
    )


def _counts() -> dict[str, Any]:
    return _object(
        tuple(_STATUS),
        {status: {"type": "integer", "minimum": 0} for status in _STATUS},
    )


def _dimensions() -> dict[str, Any]:
    fields = ("source", "sink", "strategy", "transport", "schema_evolution", "airflow_runtime_mode")
    return _object(fields, {field: _text() for field in fields})


def _proof() -> dict[str, Any]:
    required = (
        "evidence_set",
        "evidence_status",
        "certification_level",
        "release_id",
        "deployment_id",
        "environment",
        "signer_identity",
        "expires_at",
        "release_set_sha256",
        "certification_bundle_sha256",
        "attestation_sha256",
        "verification_sha256",
        "production_attempted",
        "blockers",
    )
    nullable_digest = {"$ref": "#/$defs/nullable_sha256"}
    nullable_text = {"$ref": "#/$defs/nullable_text"}
    return _object(
        required,
        {
            "evidence_set": _text(),
            "evidence_status": {"enum": ["PASS", "FAIL"]},
            "certification_level": {"enum": ["experimental", "route-certified", "production-certified"]},
            "release_id": nullable_digest,
            "deployment_id": nullable_digest,
            "environment": nullable_text,
            "signer_identity": nullable_text,
            "expires_at": nullable_text,
            "release_set_sha256": nullable_digest,
            "certification_bundle_sha256": nullable_digest,
            "attestation_sha256": nullable_digest,
            "verification_sha256": nullable_digest,
            "production_attempted": {"type": "boolean"},
            "blockers": _texts(),
        },
    )


def _row() -> dict[str, Any]:
    required = (
        "route_id",
        "dimensions",
        "sampling_mode",
        "status",
        "catalog_status",
        "capability_status",
        "contract_status",
        "live_status",
        "production_status",
        "docs_link",
        "blockers",
        "proofs",
    )
    return _object(
        required,
        {
            "route_id": _text(),
            "dimensions": {"$ref": "#/$defs/dimensions"},
            "sampling_mode": _text(),
            "status": {"enum": _STATUS},
            "catalog_status": {"enum": _STATUS},
            "capability_status": _text(),
            "contract_status": {"enum": _EVIDENCE_STATUS},
            "live_status": {"enum": _EVIDENCE_STATUS},
            "production_status": {"enum": _EVIDENCE_STATUS},
            "docs_link": _text(),
            "blockers": _texts(),
            "proofs": {"type": "array", "items": {"$ref": "#/$defs/proof"}, "maxItems": 1000},
        },
    )


def _error() -> dict[str, Any]:
    return _object(
        ("code", "evidence_set", "message"),
        {"code": _text(), "evidence_set": _text(), "message": _text()},
    )


def _object(required: tuple[str, ...], properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "required": list(required),
        "properties": properties,
        "additionalProperties": False,
    }


def _texts() -> dict[str, Any]:
    return {"type": "array", "items": _text(), "uniqueItems": True, "maxItems": 10000}


def _text() -> dict[str, Any]:
    return {"type": "string", "minLength": 1, "maxLength": 1024}


def _sha256() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}


__all__ = ["route_certification_matrix_contract", "route_matrix_claim_contract"]
