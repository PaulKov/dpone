"""Closed version-one dispatcher RPC documents and exact Native payload framing.

These values confer no authority. The server authenticates the transport and its
application handler checks protected originals before acting. ACKNOWLEDGED means
an identity-bound response, never independent evidence of committed data effects.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from hashlib import sha256
from uuid import UUID

from dpone.contracts.composition_clickhouse_dispatch import (
    MAX_DISPATCH_PAYLOAD_BYTES,
    InsertGenerationDispatch,
    decode_clickhouse_dispatch,
)
from dpone.contracts.composition_identity import require_digest
from dpone.contracts.composition_persistence import decode_attempt_identity
from dpone.contracts.composition_snapshot_subjects import SnapshotTarget
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

RPC_SCHEMA = "dpone.composition-dispatch-rpc.v1"
RPC_ROUTE = "/v1/composition-dispatch"
RPC_CONTENT_TYPE = "application/vnd.dpone.composition-dispatch.v1"
MAX_METADATA_BYTES = 1024 * 1024
MAX_RESPONSE_BYTES = MAX_METADATA_BYTES
DEFAULT_PAYLOAD_BYTES = 64 * 1024 * 1024
_SUBJECT_FIELDS = {
    "READINESS": {"nonce", "configuration_sha256", "plan_sha256", "target_binding_ref"},
    "OPEN_GATE": {"attempt_document", "attempt_sha256", "purpose"},
    "CLOSE_GATE": {"attempt_document", "attempt_sha256", "purpose"},
    "DISPATCH": {"dispatch_document", "dispatch_sha256"},
    "OBSERVE_CATALOG": {"attempt_document", "attempt_sha256", "target", "generation_uuid"},
    "READ_STATUS": {"attempt_sha256", "reference_kind", "reference_sha256"},
}
_REQUEST_FIELDS = {"schema", "request_id", "dispatcher_id", "runtime_authority_sha256", "operation", "subject"}
_RESPONSE_FIELDS = _REQUEST_FIELDS - {"subject"} | {"subject_sha256", "status", "evidence_document", "evidence_sha256"}


class DispatchRpcError(ValueError):
    """Invalid closed RPC value; messages deliberately exclude supplied values."""


def digest(document: bytes) -> str:
    """Digest exact wire bytes, including their canonical representation."""
    return "sha256:" + sha256(document).hexdigest()


def require_payload_limit(value: int) -> None:
    """A local ceiling may reduce, but cannot enlarge, the global ceiling."""
    if type(value) is not int or not 0 < value <= MAX_DISPATCH_PAYLOAD_BYTES:
        raise DispatchRpcError("rpc_payload_limit")


def require_bearer_token(value: str) -> None:
    """Require a bounded URL-safe representation of at least 256 random bits.

    Provisioners must generate the value with a cryptographic random generator;
    syntax validation cannot establish entropy of a supplied credential.
    """
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9_-]{43,256}", value) is None:
        raise DispatchRpcError("rpc_bearer_configuration")


def _uuid(value: str, *, random: bool = False) -> None:
    parsed = UUID(value)
    if str(parsed) != value or not parsed.int or (random and parsed.version != 4):
        raise DispatchRpcError("rpc_uuid")


def _object(document: bytes, limit: int = MAX_METADATA_BYTES) -> dict:
    if type(document) is not bytes or not 0 < len(document) <= limit:
        raise DispatchRpcError("rpc_document_size")
    result = strict_json_object(document)
    if canonical_json_bytes(result) != document:
        raise DispatchRpcError("rpc_document_canonical")
    return result


def _subject(operation: str, document: bytes) -> tuple[int, str | None]:
    value = _object(document)
    if operation not in _SUBJECT_FIELDS or set(value) != _SUBJECT_FIELDS[operation]:
        raise DispatchRpcError("rpc_subject_fields")
    for name in ("configuration_sha256", "plan_sha256", "attempt_sha256", "reference_sha256"):
        if name in value:
            require_digest(value[name])
    if "attempt_document" in value:
        decode_attempt_identity(canonical_json_bytes(value["attempt_document"]), value["attempt_sha256"])
    if operation == "READINESS":
        _uuid(value["nonce"], random=True)
        if (
            not isinstance(value["target_binding_ref"], str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_./-]{0,254}", value["target_binding_ref"]) is None
            or ".." in value["target_binding_ref"]
        ):
            raise DispatchRpcError("rpc_binding_ref")
    elif operation in {"OPEN_GATE", "CLOSE_GATE"}:
        if value["purpose"] not in {"INGEST", "PUBLISHER"}:
            raise DispatchRpcError("rpc_gate_purpose")
    elif operation == "OBSERVE_CATALOG":
        SnapshotTarget(**value["target"])
        _uuid(value["generation_uuid"])
    elif operation == "READ_STATUS":
        if value["reference_kind"] not in {"dispatch", "publication"}:
            raise DispatchRpcError("rpc_reference_kind")
    elif operation == "DISPATCH":
        dispatch = decode_clickhouse_dispatch(
            canonical_json_bytes(value["dispatch_document"]), value["dispatch_sha256"]
        )
        if isinstance(dispatch, InsertGenerationDispatch):
            return dispatch.payload_bytes, dispatch.payload_sha256
    return 0, None


@dataclass(frozen=True, slots=True)
class DispatchRpcRequest:
    """Immutable canonical subject, pinned to one dispatcher and authority."""

    request_id: str
    dispatcher_id: str
    runtime_authority_sha256: str
    operation: str
    subject: bytes

    def __post_init__(self) -> None:
        try:
            _uuid(self.request_id, random=True)
            _uuid(self.dispatcher_id)
            require_digest(self.runtime_authority_sha256)
            _subject(self.operation, self.subject)
            if len(self.to_bytes()) > MAX_METADATA_BYTES:
                raise DispatchRpcError("rpc_metadata_size")
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
            raise DispatchRpcError("rpc_request_invalid") from None

    @property
    def subject_sha256(self) -> str:
        return digest(self.subject)

    @property
    def payload_spec(self) -> tuple[int, str | None]:
        """Exact byte count and optional digest established by the typed dispatch."""
        return _subject(self.operation, self.subject)

    def to_bytes(self) -> bytes:
        return canonical_json_bytes(
            {
                "schema": RPC_SCHEMA,
                "request_id": self.request_id,
                "dispatcher_id": self.dispatcher_id,
                "runtime_authority_sha256": self.runtime_authority_sha256,
                "operation": self.operation,
                "subject": strict_json_object(self.subject),
            }
        )


def decode_metadata(document: bytes) -> DispatchRpcRequest:
    """Reject unknown fields, duplicate keys and noncanonical exact documents."""
    try:
        value = _object(document)
        if set(value) != _REQUEST_FIELDS or value.pop("schema") != RPC_SCHEMA:
            raise DispatchRpcError("rpc_schema")
        value["subject"] = canonical_json_bytes(value["subject"])
        return DispatchRpcRequest(**value)
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise DispatchRpcError("rpc_metadata_invalid") from None


def validate_payload(
    request: DispatchRpcRequest, payload: bytes, max_payload_bytes: int = DEFAULT_PAYLOAD_BYTES
) -> None:
    """Validate length and hash before a handler may claim an operation."""
    require_payload_limit(max_payload_bytes)
    length, expected = request.payload_spec
    if type(payload) is not bytes or len(payload) != length or length > max_payload_bytes:
        raise DispatchRpcError("rpc_payload_length")
    if expected is not None and digest(payload) != expected:
        raise DispatchRpcError("rpc_payload_digest")


def encode_request(
    request: DispatchRpcRequest, payload: bytes = b"", *, max_payload_bytes: int = DEFAULT_PAYLOAD_BYTES
) -> bytes:
    """Frame canonical metadata followed by exactly the authorized Native bytes."""
    validate_payload(request, payload, max_payload_bytes)
    metadata = request.to_bytes()
    return len(metadata).to_bytes(4, "big") + metadata + payload


def decode_request(frame: bytes, *, max_payload_bytes: int = DEFAULT_PAYLOAD_BYTES) -> tuple[DispatchRpcRequest, bytes]:
    require_payload_limit(max_payload_bytes)
    if len(frame) < 4 or len(frame) > 4 + MAX_METADATA_BYTES + max_payload_bytes:
        raise DispatchRpcError("rpc_frame_size")
    size = int.from_bytes(frame[:4], "big")
    if not 0 < size <= MAX_METADATA_BYTES or 4 + size > len(frame):
        raise DispatchRpcError("rpc_metadata_size")
    request = decode_metadata(frame[4 : 4 + size])
    payload = frame[4 + size :]
    validate_payload(request, payload, max_payload_bytes)
    return request, payload


@dataclass(frozen=True, slots=True)
class DispatchRpcResponse:
    """Correlated evidence document; its interpretation belongs to the caller."""

    request_id: str
    dispatcher_id: str
    runtime_authority_sha256: str
    operation: str
    subject_sha256: str
    status: str
    evidence_document: bytes

    def __post_init__(self) -> None:
        try:
            _uuid(self.request_id, random=True)
            _uuid(self.dispatcher_id)
            require_digest(self.runtime_authority_sha256)
            require_digest(self.subject_sha256)
            if self.operation not in _SUBJECT_FIELDS or self.status not in {"ACKNOWLEDGED", "REJECTED", "UNKNOWN"}:
                raise DispatchRpcError("rpc_response_status")
            _object(self.evidence_document)
            if len(self.to_bytes()) > MAX_RESPONSE_BYTES:
                raise DispatchRpcError("rpc_response_size")
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
            raise DispatchRpcError("rpc_response_invalid") from None

    @classmethod
    def for_request(
        cls, request: DispatchRpcRequest, evidence_document: bytes, *, status: str = "ACKNOWLEDGED"
    ) -> DispatchRpcResponse:
        return cls(
            request.request_id,
            request.dispatcher_id,
            request.runtime_authority_sha256,
            request.operation,
            request.subject_sha256,
            status,
            evidence_document,
        )

    def to_bytes(self) -> bytes:
        return canonical_json_bytes(
            {
                "schema": RPC_SCHEMA,
                "request_id": self.request_id,
                "dispatcher_id": self.dispatcher_id,
                "runtime_authority_sha256": self.runtime_authority_sha256,
                "operation": self.operation,
                "subject_sha256": self.subject_sha256,
                "status": self.status,
                "evidence_document": strict_json_object(self.evidence_document),
                "evidence_sha256": digest(self.evidence_document),
            }
        )


def decode_response(document: bytes, request: DispatchRpcRequest) -> DispatchRpcResponse:
    """Check every identity echo and the exact evidence hash, including non-ACKs."""
    try:
        value = _object(document, MAX_RESPONSE_BYTES)
        if set(value) != _RESPONSE_FIELDS or value.pop("schema") != RPC_SCHEMA:
            raise DispatchRpcError("rpc_response_fields")
        evidence = canonical_json_bytes(value["evidence_document"])
        if value.pop("evidence_sha256") != digest(evidence):
            raise DispatchRpcError("rpc_evidence_digest")
        value["evidence_document"] = evidence
        result = DispatchRpcResponse(**value)
        for name in ("request_id", "dispatcher_id", "runtime_authority_sha256", "operation", "subject_sha256"):
            if getattr(result, name) != getattr(request, name):
                raise DispatchRpcError("rpc_response_identity")
        return result
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise DispatchRpcError("rpc_response_invalid") from None
