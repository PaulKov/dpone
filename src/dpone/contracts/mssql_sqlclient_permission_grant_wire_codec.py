"""Closed permission transcript: structural observations, never execution authority.

Public codecs use unprefixed JSON. Only frame_permission_payload adds framing.
Private credentials have no codec here; their actual size is accounted once.
Transport failures outside accept must explicitly call fail before propagation.
"""

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

from dpone.contracts.mssql_sqlclient_permission_grant import (
    EVIDENCE_LIMIT,
    REQUEST_LIMIT,
    SqlClientPermissionGrantRequest,
    _leaf,
    _uuid,
    decode_permission_grant_evidence,
    decode_permission_grant_request,
    encode_permission_grant_request,
    permission_grant_digest,
)
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorGrant,
    TdsCoordinatorIdentity,
    coordinator_identity_digest,
)
from dpone.contracts.mssql_tds_coordinator_authority import decode_authority, encode_authority
from dpone.contracts.mssql_tds_coordinator_codec import coordinator_identity_body, coordinator_identity_from_body
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, decode_startup, encode_startup
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.mssql_tds_frames import encode_message
from dpone.contracts.mssql_tds_session import decode_session_identity
from dpone.contracts.mssql_tds_validation import _hash, _integer
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsAttemptOwnership, TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, record_shape

ERROR = "mssql_native.sqlclient_permission_wire_invalid"
SCHEMA = "dpone.sqlclient.permission-grant-wire.v1"
CONTROL_LIMIT = 16384
CREDENTIAL_LIMIT = 196608
TOTAL_LIMIT = 2097152


class PermissionBoundary(StrEnum):
    READY = "READY"
    BEFORE_LAUNCH = "BEFORE_LAUNCH"
    BEFORE_JOB = "BEFORE_JOB"
    BEFORE_BULK = "BEFORE_BULK"
    AFTER_WRITER_EXCLUDED = "AFTER_WRITER_EXCLUDED"
    BEFORE_RELEASE = "BEFORE_RELEASE"


class PermissionWireKind(StrEnum):
    STARTUP = "STARTUP"
    REQUEST = "REQUEST"
    REQUEST_ACCEPTED = "REQUEST_ACCEPTED"
    CREDENTIALS = "CREDENTIALS"
    AUTHORITY = "AUTHORITY"
    EXECUTE = "EXECUTE"
    PERMISSION_HELD = "PERMISSION_HELD"
    CHECK_HELD = "CHECK_HELD"
    HELD = "HELD"
    RELEASE = "RELEASE"
    RELEASED = "RELEASED"


K = PermissionWireKind
_FIELDS = "schema kind ordinal operation_sha256 command_sha256 launch_nonce operation_deadline_ns body".split()


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError(ERROR)


@dataclass(frozen=True, slots=True)
class PermissionWireBinding:
    request: SqlClientPermissionGrantRequest
    operation: TdsCoordinatorIdentity
    startup: TdsCoordinatorStartup
    execution_owner: TdsAttemptOwnership
    operation_deadline_ns: int

    def snapshot(self) -> bytes:
        """Known original leaves are checked before serialization or hashing."""
        _leaf(self.request, SqlClientPermissionGrantRequest)
        _leaf(self.operation, TdsCoordinatorIdentity)
        _leaf(self.operation.parent, TdsAttemptIdentity)
        _uuid(self.operation.operation_id)
        _leaf(self.startup, TdsCoordinatorStartup)
        _leaf(self.startup.process, TdsProcessIdentity)
        _leaf(self.execution_owner, TdsAttemptOwnership)
        _integer(self.operation_deadline_ns, 1, 2**63 - 1)
        _require(
            self.operation.command is TdsCoordinatorCommand.GRANT
            and self.operation.parent == self.request.parent
            and self.operation.command_sha256 == permission_grant_digest(self.request)
            and self.operation.original_fence == self.execution_owner.fence
            and self.operation.implementation_sha256 == self.startup.implementation_sha256
        )
        return canonical_json_bytes(
            dict(
                request=strict_json_object(encode_permission_grant_request(self.request)),
                operation=coordinator_identity_body(self.operation),
                startup=strict_json_object(encode_startup(self.startup)),
                execution_owner=asdict(self.execution_owner),
                operation_deadline_ns=self.operation_deadline_ns,
            )
        )

    def __post_init__(self) -> None:
        self.snapshot()


@dataclass(frozen=True, slots=True)
class PermissionWireMessage:
    kind: PermissionWireKind
    ordinal: int
    body: bytes  # Canonical immutable JSON; no mutable caller dictionary retained.

    def __post_init__(self) -> None:
        _limit(self.kind)
        _integer(self.ordinal, 0, 10)
        _require(type(self.body) is bytes and 0 < len(self.body) <= EVIDENCE_LIMIT)
        _require(canonical_json_bytes(strict_json_object(self.body)) == self.body)


def _json_values(value: Any) -> None:
    """Reject Python aliases before JSON normalization can erase their types."""
    if type(value) is dict:
        for key, child in value.items():
            _require(type(key) is str)
            _json_values(child)
    elif type(value) is list:
        for child in value:
            _json_values(child)
    else:
        _require(value is None or type(value) in (str, int, bool))


def _limit(kind: PermissionWireKind) -> int:
    _require(type(kind) is K and kind is not K.CREDENTIALS)
    return REQUEST_LIMIT if kind is K.REQUEST else EVIDENCE_LIMIT if kind is K.PERMISSION_HELD else CONTROL_LIMIT


def checked_physical_total(current: int, payload_size: int) -> int:
    """Pure bounded counter; callers count each actual prefix exactly once."""
    _integer(current, 0, TOTAL_LIMIT)
    _integer(payload_size, 1, EVIDENCE_LIMIT)
    _require(current + payload_size + 4 <= TOTAL_LIMIT)
    return current + payload_size + 4


def _grant(body: dict) -> TdsCoordinatorGrant:
    values = dict(record_shape(TdsCoordinatorGrant, body))
    values["grant_id"] = canonical_uuid(values["grant_id"])
    values["session"] = decode_session_identity(canonical_json_bytes(values["session"]))
    for name, cls in (("ownership", TdsAttemptOwnership), ("process", TdsProcessIdentity)):
        values[name] = cls(**record_shape(cls, values[name]))
    return TdsCoordinatorGrant(**values)


def _authority(body: dict, binding: PermissionWireBinding) -> Any:
    value = decode_authority(canonical_json_bytes(body))
    stage = binding.request.stage
    _require(
        value.operation_sha256 == coordinator_identity_digest(binding.operation)
        and value.execution_owner == binding.execution_owner
        and value.process == binding.startup.process
        and value.implementation_sha256 == binding.startup.implementation_sha256
        and (
            value.database.database_id,
            value.database.name,
            value.database.database_guid,
            value.schema_observation.schema_id,
            value.schema_observation.name,
        )
        == (stage.database_id, stage.database_name, stage.database_guid, stage.schema_id, stage.schema_name)
    )
    return value


def _body(binding: PermissionWireBinding, kind: PermissionWireKind, ordinal: int, body: dict) -> None:
    _require(type(body) is dict)
    keys = {
        K.STARTUP: {"startup"},
        K.REQUEST: {"operation", "execution_owner", "request"},
        K.REQUEST_ACCEPTED: {"request_payload_sha256"},
        K.AUTHORITY: {"authority"},
        K.EXECUTE: {"grant"},
        K.PERMISSION_HELD: {"evidence"},
        K.CHECK_HELD: {"boundary", "evidence_sha256"},
        K.HELD: {"boundary", "evidence_sha256", "authority"},
        K.RELEASE: {"evidence_sha256"},
        K.RELEASED: {"evidence_sha256"},
    }
    _require(set(body) == keys[kind])
    if kind is K.STARTUP:
        _require(ordinal == 0 and decode_startup(canonical_json_bytes(body["startup"])) == binding.startup)
    elif kind is K.REQUEST:
        _require(
            ordinal == 1
            and coordinator_identity_from_body(body["operation"]) == binding.operation
            and TdsAttemptOwnership(**record_shape(TdsAttemptOwnership, body["execution_owner"]))
            == binding.execution_owner
            and decode_permission_grant_request(canonical_json_bytes(body["request"])) == binding.request
        )
    elif kind is K.REQUEST_ACCEPTED:
        _require(ordinal == 1)
        _hash(body["request_payload_sha256"])
    elif kind is K.EXECUTE:
        grant = _grant(body["grant"])
        _require(
            ordinal == 3
            and grant.operation_sha256 == coordinator_identity_digest(binding.operation)
            and grant.ownership == binding.execution_owner
            and grant.process == binding.startup.process
        )
    elif kind is K.PERMISSION_HELD:
        evidence = decode_permission_grant_evidence(canonical_json_bytes(body["evidence"]))
        _require(ordinal == 3 and evidence.request == binding.request and evidence.operation == binding.operation)
        _authority(strict_json_object(encode_authority(evidence.authority)), binding)
    if "authority" in body:
        _authority(body["authority"], binding)
        if kind is K.AUTHORITY:
            _require(ordinal == 2)
    if "evidence_sha256" in body:
        _hash(body["evidence_sha256"])
        _integer(ordinal, 4, 9 if "boundary" in body else 10)
    if "boundary" in body:
        _require(type(body["boundary"]) is str and body["boundary"] == list(PermissionBoundary)[ordinal - 4].value)


def encode_permission_message(
    binding: PermissionWireBinding, kind: PermissionWireKind, ordinal: int, body: dict
) -> bytes:
    """Encode a closed JSON-shaped body; no caller-selected serializer or framing."""
    try:
        _require(type(binding) is PermissionWireBinding)
        binding.snapshot()
        limit = _limit(kind)
        _integer(ordinal, 0, 10)
        _json_values(body)
        _body(binding, kind, ordinal, body)
        payload = canonical_json_bytes(
            dict(
                schema=SCHEMA,
                kind=kind.value,
                ordinal=ordinal,
                operation_sha256=coordinator_identity_digest(binding.operation),
                command_sha256=permission_grant_digest(binding.request),
                launch_nonce=binding.startup.launch_nonce.hex(),
                operation_deadline_ns=binding.operation_deadline_ns,
                body=body,
            )
        )
        _require(0 < len(payload) <= limit)
        return payload
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def decode_permission_message(
    payload: bytes, *, binding: PermissionWireBinding, kind: PermissionWireKind, ordinal: int
) -> PermissionWireMessage:
    try:
        _require(type(payload) is bytes and 0 < len(payload) <= _limit(kind))
        value = strict_json_object(payload)
        _require(set(value) == set(_FIELDS))
        _require(encode_permission_message(binding, kind, ordinal, value["body"]) == payload)
        return PermissionWireMessage(kind, ordinal, canonical_json_bytes(value["body"]))
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def frame_permission_payload(payload: bytes) -> bytes:
    """Frame canonical public bytes once; semantic admission belongs to codecs."""
    try:
        _require(type(payload) is bytes and 0 < len(payload) <= EVIDENCE_LIMIT)
        value = strict_json_object(payload)
        _require(set(value) == set(_FIELDS) and value["schema"] == SCHEMA and canonical_json_bytes(value) == payload)
        return encode_message(payload, max_payload=_limit(K(value["kind"])))
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None
