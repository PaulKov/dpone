"""Closed whole-cell metadata for dispatcher-owned capture; v1 remains separate.

Canonical equality authenticates correlation and internal consistency only.
The service must independently derive the candidate against current protected
SQL ownership before admitting any execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any
from uuid import UUID

from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.composition_persistence import CompositionAttemptIdentity, decode_attempt_identity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

RPC_SCHEMA = "dpone.composition-dispatch-rpc.v2"
RPC_PATH = "/v2/composition-dispatch"
MAX_METADATA_BYTES = 1024 * 1024
MAX_RESPONSE_BYTES = 1024 * 1024
_REQUEST = {"schema", "request_id", "dispatcher_id", "runtime_authority_sha256", "operation", "subject"}
_SUBJECT = {"attempt_document", "attempt_sha256", "airflow_run_identity", "airflow_attempt"}


def digest(document: bytes) -> str:
    return "sha256:" + sha256(document).hexdigest()


def _object(document: bytes) -> dict[str, Any]:
    if type(document) is not bytes or not 0 < len(document) <= MAX_METADATA_BYTES:
        raise CompositionAdmissionError("dispatch_v2_size")
    result = strict_json_object(document)
    if canonical_json_bytes(result) != document:
        raise CompositionAdmissionError("dispatch_v2_canonical")
    return result


def _uuid(value: str, *, random: bool = False) -> None:
    parsed = UUID(value)
    if str(parsed) != value or not parsed.int or (random and parsed.version != 4):
        raise ValueError


def _attempt(document: bytes) -> CompositionAttemptIdentity:
    body = _object(document)
    if set(body) != _SUBJECT:
        raise ValueError
    attempt = decode_attempt_identity(canonical_json_bytes(body["attempt_document"]), body["attempt_sha256"])
    run = AirflowRunIdentity.from_mapping(body["airflow_run_identity"])
    scheduler = AirflowAttemptCorrelation.from_mapping(body["airflow_attempt"])
    if (
        run.to_dict() != body["airflow_run_identity"]
        or scheduler.to_dict() != body["airflow_attempt"]
        or (run.dag_spec is not None and run.dag_spec.id != scheduler.dag_id)
        or (run.workload_pack.id, run.workload_pack.sha256) != (attempt.workload_id, attempt.pack_sha256)
        or (scheduler.run_id, scheduler.task_id, scheduler.try_number, scheduler.map_index)
        != (attempt.dag_run_id, attempt.task_id, attempt.try_number, attempt.map_index)
    ):
        raise ValueError
    return attempt


@dataclass(frozen=True, slots=True)
class DispatchV2Request:
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
            if self.operation not in {"EXECUTE_TRANSFER", "READ_STATUS"}:
                raise ValueError
            _attempt(self.subject)
            if len(self.to_bytes()) > MAX_METADATA_BYTES:
                raise ValueError
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
            raise CompositionAdmissionError("dispatch_v2_request") from None

    @property
    def attempt(self) -> CompositionAttemptIdentity:
        return _attempt(self.subject)

    @property
    def subject_sha256(self) -> str:
        return digest(self.subject)

    @property
    def payload_spec(self) -> tuple[int, None]:
        return 0, None

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


def decode_metadata(document: bytes) -> DispatchV2Request:
    try:
        body = _object(document)
        if set(body) != _REQUEST or body.pop("schema") != RPC_SCHEMA:
            raise ValueError
        body["subject"] = canonical_json_bytes(body["subject"])
        return DispatchV2Request(**body)
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise CompositionAdmissionError("dispatch_v2_request") from None


def encode_request(request: DispatchV2Request) -> bytes:
    document = request.to_bytes()
    return len(document).to_bytes(4, "big") + document


def decode_request(frame: bytes) -> DispatchV2Request:
    if type(frame) is not bytes or not 4 < len(frame) <= MAX_METADATA_BYTES + 4:
        raise CompositionAdmissionError("dispatch_v2_frame")
    if int.from_bytes(frame[:4], "big") != len(frame) - 4:
        raise CompositionAdmissionError("dispatch_v2_payload")
    return decode_metadata(frame[4:])


@dataclass(frozen=True, slots=True)
class DispatchV2Response:
    """Correlated result bytes; interpretation is checked against the request."""

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
            if self.operation not in {"EXECUTE_TRANSFER", "READ_STATUS"} or self.status not in {
                "SUCCEEDED",
                "FAILED",
                "IN_PROGRESS",
                "UNKNOWN",
            }:
                raise ValueError
            _object(self.evidence_document)
            if len(self.to_bytes()) > MAX_RESPONSE_BYTES:
                raise ValueError
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
            raise CompositionAdmissionError("dispatch_v2_response") from None

    @classmethod
    def for_request(cls, request: DispatchV2Request, evidence_document: bytes, *, status: str) -> DispatchV2Response:
        response = cls(
            request.request_id,
            request.dispatcher_id,
            request.runtime_authority_sha256,
            request.operation,
            request.subject_sha256,
            status,
            evidence_document,
        )
        return decode_response(response.to_bytes(), request)

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


def decode_response(document: bytes, request: DispatchV2Request) -> DispatchV2Response:
    from dpone.contracts.composition_remote_transfer_result import decode_result, decode_status

    try:
        body = _object(document)
        if (
            set(body)
            != {
                "schema",
                "request_id",
                "dispatcher_id",
                "runtime_authority_sha256",
                "operation",
                "subject_sha256",
                "status",
                "evidence_document",
                "evidence_sha256",
            }
            or body.pop("schema") != RPC_SCHEMA
        ):
            raise ValueError
        evidence = canonical_json_bytes(body["evidence_document"])
        if body.pop("evidence_sha256") != digest(evidence):
            raise ValueError
        body["evidence_document"] = evidence
        result = DispatchV2Response(**body)
        if (
            result.request_id,
            result.dispatcher_id,
            result.runtime_authority_sha256,
            result.operation,
            result.subject_sha256,
        ) != (
            request.request_id,
            request.dispatcher_id,
            request.runtime_authority_sha256,
            request.operation,
            request.subject_sha256,
        ):
            raise ValueError
        if result.status == "SUCCEEDED":
            decode_result(evidence, digest(evidence), attempt=request.attempt)
        elif (
            request.operation == "READ_STATUS"
            and result.status == "UNKNOWN"
            and strict_json_object(evidence).get("schema") == "dpone.composition-remote-transfer-absence.v1"
        ):
            if strict_json_object(evidence) != {
                "schema": "dpone.composition-remote-transfer-absence.v1",
                "attempt_sha256": request.attempt.attempt_sha256,
                "observation": "ABSENT",
            }:
                raise ValueError
        else:
            decode_status(evidence, status=result.status, attempt=request.attempt)
        return result
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise CompositionAdmissionError("dispatch_v2_response") from None
