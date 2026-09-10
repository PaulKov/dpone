"""Bounded canonical UTF-8 documents for protected composition persistence.

SQL Server stores these bytes as VARBINARY rather than hashing NVARCHAR's
different UTF-16 encoding. A matching document is exact readback, not proof of
source authenticity or permission; callers still need a protected connection.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from dpone.contracts.composition_activation import (
    CompositionActivationRequest,
    CompositionAdmissionError,
    CompositionOccurrenceContext,
    CompositionPhysicalResource,
    CompositionWorkloadAdmission,
    require_digest,
)
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_proof import CompositionAttemptProof, CompositionProofAuthority
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

_MAX_DOCUMENT_BYTES = 8 * 1024 * 1024


def encode_activation_request(request: CompositionActivationRequest) -> bytes:
    """Return the canonical, detached bytes retained across catalog changes."""
    request.__post_init__()
    return _encode_document(request.to_dict())


def _encode_document(payload: dict[str, Any]) -> bytes:
    encoded = canonical_json_bytes(payload)
    if len(encoded) > _MAX_DOCUMENT_BYTES:
        raise CompositionAdmissionError("persistence_budget")
    return encoded


def decode_activation_request(document: bytes, expected_sha256: str) -> CompositionActivationRequest:
    """Reject changed, ambiguous, noncanonical or partial protected documents."""
    require_digest(expected_sha256)
    if type(document) is not bytes or len(document) > _MAX_DOCUMENT_BYTES:
        raise CompositionAdmissionError("persistence_document")
    try:
        body = strict_json_object(document)
        if not isinstance(body, dict) or set(body) != {
            "schema",
            "context",
            "source_subject_sha256",
            "workloads",
            "resources",
        }:
            raise CompositionAdmissionError("persistence_shape")
        if body["schema"] != "dpone.composition-activation-request.v1":
            raise CompositionAdmissionError("persistence_schema")
        if any(type(body[key]) is not list or len(body[key]) > 8192 for key in ("workloads", "resources")):
            raise CompositionAdmissionError("persistence_closure")
        request = CompositionActivationRequest(
            context=CompositionOccurrenceContext(**body["context"]),
            source_subject_sha256=body["source_subject_sha256"],
            workloads=tuple(
                CompositionWorkloadAdmission(**dict(row, write_subjects=tuple(row["write_subjects"])))
                for row in body["workloads"]
            ),
            resources=tuple(
                CompositionPhysicalResource(**dict(row, write_subjects=tuple(row["write_subjects"])))
                for row in body["resources"]
            ),
        )
        if request.request_sha256 != expected_sha256 or encode_activation_request(request) != document:
            raise CompositionAdmissionError("persistence_identity")
        return request
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        raise CompositionAdmissionError("persistence_readback") from None


def encode_attempt_identity(attempt: CompositionAttemptIdentity) -> bytes:
    """Persist existing attempt identity, including real scheduler coordinates."""
    attempt.__post_init__()
    return _encode_document({"schema": "dpone.composition-attempt.v1", **asdict(attempt)})


def decode_attempt_identity(document: bytes, expected_sha256: str) -> CompositionAttemptIdentity:
    """Reconstruct only canonical exact attempt bytes from protected storage."""
    require_digest(expected_sha256)
    try:
        body = _decode_document(document, "dpone.composition-attempt.v1")
        body["guard_epochs"] = tuple(tuple(pair) for pair in body["guard_epochs"])
        attempt = CompositionAttemptIdentity(**body)
        if attempt.attempt_sha256 != expected_sha256 or encode_attempt_identity(attempt) != document:
            raise CompositionAdmissionError("persistence_identity")
        return attempt
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        raise CompositionAdmissionError("attempt_persistence_readback") from None


def encode_attempt_proof(proof: CompositionAttemptProof) -> bytes:
    """Retain the canonical producer scope; this function grants no authority."""
    proof.__post_init__()
    return _encode_document(proof.to_dict())


def decode_attempt_proof(document: bytes, expected_sha256: str) -> CompositionAttemptProof:
    """Reconstruct proof scope before independent issuance-journal comparison."""
    require_digest(expected_sha256)
    try:
        body = _decode_document(document, "dpone.composition-attempt-proof.v1")
        body["authorities"] = tuple(CompositionProofAuthority(**value) for value in body["authorities"])
        proof = CompositionAttemptProof(**body)
        if proof.proof_sha256 != expected_sha256 or encode_attempt_proof(proof) != document:
            raise CompositionAdmissionError("persistence_identity")
        return proof
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        raise CompositionAdmissionError("proof_persistence_readback") from None


def _decode_document(document: bytes, schema: str) -> dict[str, Any]:
    if type(document) is not bytes or len(document) > _MAX_DOCUMENT_BYTES:
        raise CompositionAdmissionError("persistence_document")
    body = strict_json_object(document)
    if type(body) is not dict or body.pop("schema", None) != schema:
        raise CompositionAdmissionError("persistence_schema")
    return body
