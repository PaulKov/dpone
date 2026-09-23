"""Closed, credential-free permission evidence through the first READY hold.

These values prove only exact byte persistence. They do not prove process
provenance, SQL effects, message delivery, or continued remote-session custody.
"""

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from hashlib import sha256
from types import MappingProxyType

from dpone.contracts.mssql_sqlclient_permission_grant import (
    SqlClientPermissionGrantEvidence,
    decode_permission_grant_evidence,
    decode_permission_grant_request,
    encode_permission_grant_evidence,
    encode_permission_grant_request,
    validate_permission_binding,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import (
    PermissionBoundary,
    PermissionWireBinding,
    PermissionWireKind,
    decode_permission_message,
    encode_permission_message,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionProfile
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_codec import coordinator_identity_body
from dpone.contracts.mssql_tds_coordinator_ipc import encode_registration
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

ERROR = "mssql_native.sqlclient_permission_parent_evidence_invalid"
HELD_READY_SCHEMA = "dpone.sqlclient.permission-grant-held-ready-evidence.v1"
ADMISSION_SCHEMA = "dpone.tds.coordinator-build.v1"


class PermissionGrantParentEvidenceKind(StrEnum):
    REQUEST = "request"
    ADMISSION = "admission"
    REGISTRATION = "registration"
    REQUEST_ACCEPTED = "request_accepted"
    AUTHORITY = "authority"
    EXECUTION_INTENT = "execution_intent"
    RESULT = "result"
    HELD_READY = "held_ready"


K = PermissionGrantParentEvidenceKind
CAPS = MappingProxyType(
    {
        K.REQUEST: 131072,
        K.ADMISSION: 16384,
        K.REGISTRATION: 32768,
        K.REQUEST_ACCEPTED: 16384,
        K.AUTHORITY: 16384,
        K.EXECUTION_INTENT: 16384,
        K.RESULT: 1048572,
        K.HELD_READY: 65536,
    }
)


def _invalid() -> ValueError:
    return ValueError(ERROR)


def _binding(value: object) -> bytes:
    if type(value) is not PermissionWireBinding:
        raise _invalid()
    return value.snapshot()


@dataclass(frozen=True, slots=True)
class PermissionGrantEvidenceSubject:
    attempt_sha256: str
    operation_sha256: str

    def __post_init__(self) -> None:
        try:
            _hash(self.attempt_sha256)
            _hash(self.operation_sha256)
        except (ValueError, TypeError):
            raise _invalid() from None


def _subject(value: PermissionGrantEvidenceSubject) -> PermissionGrantEvidenceSubject:
    if type(value) is not PermissionGrantEvidenceSubject:
        raise _invalid()
    return PermissionGrantEvidenceSubject(value.attempt_sha256, value.operation_sha256)


def _name(subject: PermissionGrantEvidenceSubject, kind: K, digest: str) -> str:
    return f"tds-permission-v1-{subject.attempt_sha256}-{subject.operation_sha256}-{kind.value}-{digest}.json"


def _admission(payload: bytes) -> None:
    value = strict_json_object(payload)
    if set(value) != {"schema", "build", "profile"} or value["schema"] != ADMISSION_SCHEMA:
        raise _invalid()
    build = value["build"]
    if type(build) is not dict or set(build) != {"interpreter", "pyodbc", "driver", "driver_manager"}:
        raise _invalid()
    for pin in build.values():
        if type(pin) is not dict or set(pin) != {"path", "sha256"}:
            raise _invalid()
        path = pin["path"]
        if type(path) is not str or "\0" in path or len(path.encode("utf-8")) > 4096:
            raise _invalid()
        _hash(pin["sha256"])
    profile = value["profile"]
    if type(profile) is not str or not any(profile == item.value for item in TdsConnectionProfile):
        raise _invalid()


def _registration(payload: bytes, binding: PermissionWireBinding) -> str:
    value = strict_json_object(payload)
    if set(value) != {"schema", "startup", "admission_sha256"}:
        raise _invalid()
    digest = value["admission_sha256"]
    _hash(digest)
    if encode_registration(binding.startup, digest) != payload:
        raise _invalid()
    return digest


def _wire_request(binding: PermissionWireBinding) -> bytes:
    return encode_permission_message(
        binding,
        PermissionWireKind.REQUEST,
        1,
        {
            "operation": strict_json_object(canonical_json_bytes(coordinator_identity_body(binding.operation))),
            "execution_owner": asdict(binding.execution_owner),
            "request": strict_json_object(encode_permission_grant_request(binding.request)),
        },
    )


def _wire(payload: bytes, binding: PermissionWireBinding, kind: PermissionWireKind, ordinal: int) -> dict:
    message = decode_permission_message(payload, binding=binding, kind=kind, ordinal=ordinal)
    return strict_json_object(message.body)


@dataclass(frozen=True, slots=True)
class PermissionGrantHeldReadyEvidence:
    operation_sha256: str
    permission_evidence_sha256: str
    check_payload_sha256: str
    check_payload: bytes = field(repr=False)
    held_payload_sha256: str
    held_payload: bytes = field(repr=False)

    def __post_init__(self) -> None:
        try:
            for digest in (
                self.operation_sha256,
                self.permission_evidence_sha256,
                self.check_payload_sha256,
                self.held_payload_sha256,
            ):
                _hash(digest)
            if type(self.check_payload) is not bytes or type(self.held_payload) is not bytes:
                raise _invalid()
        except (ValueError, TypeError):
            raise _invalid() from None


def _held_ready(value: PermissionGrantHeldReadyEvidence, binding: PermissionWireBinding) -> tuple[dict, dict]:
    if type(value) is not PermissionGrantHeldReadyEvidence:
        raise _invalid()
    for digest in (
        value.operation_sha256,
        value.permission_evidence_sha256,
        value.check_payload_sha256,
        value.held_payload_sha256,
    ):
        _hash(digest)
    if value.operation_sha256 != coordinator_identity_digest(binding.operation):
        raise _invalid()
    if type(value.check_payload) is not bytes or sha256(value.check_payload).hexdigest() != value.check_payload_sha256:
        raise _invalid()
    if type(value.held_payload) is not bytes or sha256(value.held_payload).hexdigest() != value.held_payload_sha256:
        raise _invalid()
    check = _wire(value.check_payload, binding, PermissionWireKind.CHECK_HELD, 4)
    held = _wire(value.held_payload, binding, PermissionWireKind.HELD, 4)
    if (
        check["boundary"] != PermissionBoundary.READY.value
        or held["boundary"] != PermissionBoundary.READY.value
        or check["evidence_sha256"] != value.permission_evidence_sha256
        or held["evidence_sha256"] != value.permission_evidence_sha256
    ):
        raise _invalid()
    return check, held


def encode_permission_grant_held_ready_evidence(
    value: PermissionGrantHeldReadyEvidence, *, binding: PermissionWireBinding
) -> bytes:
    try:
        _binding(binding)
        check, held = _held_ready(value, binding)
        payload = canonical_json_bytes(
            {
                "schema": HELD_READY_SCHEMA,
                "operation_sha256": value.operation_sha256,
                "permission_evidence_sha256": value.permission_evidence_sha256,
                "check_payload_sha256": value.check_payload_sha256,
                "check_payload": strict_json_object(value.check_payload),
                "held_payload_sha256": value.held_payload_sha256,
                "held_payload": strict_json_object(value.held_payload),
            }
        )
        if check["evidence_sha256"] != held["evidence_sha256"] or len(payload) > CAPS[K.HELD_READY]:
            raise _invalid()
        return payload
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError, UnicodeError):
        raise _invalid() from None


def decode_permission_grant_held_ready_evidence(
    payload: bytes, *, binding: PermissionWireBinding
) -> PermissionGrantHeldReadyEvidence:
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= CAPS[K.HELD_READY]:
            raise _invalid()
        body = strict_json_object(payload)
        expected = {
            "schema",
            "operation_sha256",
            "permission_evidence_sha256",
            "check_payload_sha256",
            "check_payload",
            "held_payload_sha256",
            "held_payload",
        }
        if set(body) != expected or body.pop("schema") != HELD_READY_SCHEMA:
            raise _invalid()
        body["check_payload"] = canonical_json_bytes(body["check_payload"])
        body["held_payload"] = canonical_json_bytes(body["held_payload"])
        result = PermissionGrantHeldReadyEvidence(**body)
        if encode_permission_grant_held_ready_evidence(result, binding=binding) != payload:
            raise _invalid()
        return result
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError, UnicodeError):
        raise _invalid() from None


def _validate_payload(kind: K, payload: bytes, binding: PermissionWireBinding) -> None:
    if kind is K.REQUEST:
        if (
            decode_permission_grant_request(payload) != binding.request
            or encode_permission_grant_request(binding.request) != payload
        ):
            raise _invalid()
    elif kind is K.ADMISSION:
        _admission(payload)
    elif kind is K.REGISTRATION:
        _registration(payload, binding)
    elif kind is K.REQUEST_ACCEPTED:
        body = _wire(payload, binding, PermissionWireKind.REQUEST_ACCEPTED, 1)
        if body["request_payload_sha256"] != sha256(_wire_request(binding)).hexdigest():
            raise _invalid()
    elif kind is K.AUTHORITY:
        _wire(payload, binding, PermissionWireKind.AUTHORITY, 2)
    elif kind is K.EXECUTION_INTENT:
        _wire(payload, binding, PermissionWireKind.EXECUTE, 3)
    elif kind is K.RESULT:
        evidence = decode_permission_grant_evidence(payload)
        _validate_result(evidence, binding, payload)
    else:
        decode_permission_grant_held_ready_evidence(payload, binding=binding)
    if canonical_json_bytes(strict_json_object(payload)) != payload:
        raise _invalid()


def _validate_result(
    evidence: SqlClientPermissionGrantEvidence, binding: PermissionWireBinding, payload: bytes
) -> None:
    if (
        evidence.request != binding.request
        or evidence.operation != binding.operation
        or evidence.authority.execution_owner != binding.execution_owner
        or evidence.authority.process != binding.startup.process
        or evidence.authority.implementation_sha256 != binding.startup.implementation_sha256
    ):
        raise _invalid()
    validate_permission_binding(evidence.request, evidence.operation, evidence.grant, evidence.authority)
    if encode_permission_grant_evidence(evidence) != payload:
        raise _invalid()
