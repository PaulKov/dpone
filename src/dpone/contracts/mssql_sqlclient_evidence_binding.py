"""Common immutable worker bindings and explicit comparison with trusted originals."""

from dataclasses import asdict, dataclass

from dpone.contracts.mssql_sqlclient_evidence_types import ERROR, SqlClientEvidenceKind, require_payload
from dpone.contracts.mssql_sqlclient_launch import SqlClientLaunch, decode_launch, encode_launch, launch_digest
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash, _integer
from dpone.contracts.mssql_tds_worker import (
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsObjectIdentity,
    TdsProcessIdentity,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape


@dataclass(frozen=True, slots=True)
class SqlClientEvidenceBinding:
    """Internal consistency alone does not authenticate an observation or ACK."""

    launch_sha256: str
    attempt_sha256: str
    identity: TdsAttemptIdentity
    ownership: TdsAttemptOwnership
    process: TdsProcessIdentity
    object_identity: TdsObjectIdentity
    input_binding_sha256: str
    build_sha256: str
    operation_deadline_ns: int

    def __post_init__(self) -> None:
        for value in (self.launch_sha256, self.attempt_sha256, self.input_binding_sha256, self.build_sha256):
            _hash(value)
        _integer(self.operation_deadline_ns, 1)
        for nested, cls in (
            (self.identity, TdsAttemptIdentity),
            (self.ownership, TdsAttemptOwnership),
            (self.process, TdsProcessIdentity),
            (self.object_identity, TdsObjectIdentity),
        ):
            if type(nested) is not cls:
                raise ValueError(ERROR)
            construct_record(cls, asdict(nested))
        for text in (self.identity.database, self.identity.schema, self.identity.table):
            if len(text.encode("utf-16le")) > 256:
                raise ValueError(ERROR)
        if attempt_identity_digest(self.identity) != self.attempt_sha256:
            raise ValueError(ERROR)


def encode_evidence_binding(record: SqlClientEvidenceBinding) -> bytes:
    """Revalidate even frozen nested objects at the serialization boundary."""
    try:
        if type(record) is not SqlClientEvidenceBinding:
            raise ValueError(ERROR)
        record.__post_init__()
        return canonical_json_bytes(asdict(record))
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def decode_evidence_binding(payload: bytes) -> SqlClientEvidenceBinding:
    """Reconstruct each known nested record, never a generic object graph."""
    try:
        require_payload(payload, SqlClientEvidenceKind.REGISTRATION)
        data = record_shape(SqlClientEvidenceBinding, strict_json_object(payload))
        for name, cls in (
            ("identity", TdsAttemptIdentity),
            ("ownership", TdsAttemptOwnership),
            ("process", TdsProcessIdentity),
            ("object_identity", TdsObjectIdentity),
        ):
            data[name] = construct_record(cls, data[name])
        record = SqlClientEvidenceBinding(**data)
        if encode_evidence_binding(record) != payload:
            raise ValueError(ERROR)
        return record
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def validate_evidence_binding(
    record: SqlClientEvidenceBinding,
    *,
    launch: SqlClientLaunch,
    identity: TdsAttemptIdentity,
    ownership: TdsAttemptOwnership,
    object_identity: TdsObjectIdentity,
) -> None:
    """Compare with independently retained originals, not incoming-field copies."""
    try:
        checked = decode_evidence_binding(encode_evidence_binding(record))
        if type(launch) is not SqlClientLaunch:
            raise ValueError(ERROR)
        launch.__post_init__()
        original = decode_launch(encode_launch(launch))
        expected = SqlClientEvidenceBinding(
            launch_digest(original),
            original.attempt_sha256,
            identity,
            ownership,
            original.process,
            object_identity,
            original.input_binding_sha256,
            original.build_sha256,
            original.operation_deadline_ns,
        )
        if checked != expected:
            raise ValueError(ERROR)
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None
