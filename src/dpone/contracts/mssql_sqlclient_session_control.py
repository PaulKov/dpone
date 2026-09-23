"""Bounded SqlClient session announcement and one-shot bulk permission bodies.

Parsing establishes structure, not provenance or SQL authority. The supervisor
must persist independently observed evidence and fenced grant intent before
sending once over its private inherited channel. A recovered intent must never
be resent. Framing, actual EOF, clocks and SQL observations are injected by the
owning adapters; none are obtained here. These are not DDL coordinator grants.
"""

from dataclasses import asdict, dataclass
from hashlib import sha256

from dpone.contracts.mssql_sqlclient_launch import SqlClientLaunch, launch_digest
from dpone.contracts.mssql_tds_session import (
    TdsRemoteSessionIdentity,
    decode_session_identity,
    encode_session_identity,
    require_session_nonce,
)
from dpone.contracts.mssql_tds_validation import _hash, _integer, _text, _uuid
from dpone.contracts.mssql_tds_worker import TdsAttemptOwnership, TdsObjectIdentity, TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape

MAX_CONTROL_BYTES = 16384
_ERROR = "mssql_native.sqlclient_session_control_invalid"
_GRANT_DOMAIN = b"dpone.sqlclient.bulk-grant.v1\x00"


@dataclass(frozen=True)
class SqlClientSessionAnnouncement:
    """Untrusted child locator; only a separate observer establishes incarnation."""

    schema_version: int
    launch_sha256: str
    attempt_sha256: str
    session_id: int
    nonce: str

    def __post_init__(self) -> None:
        _integer(self.schema_version, 1, 1)
        _hash(self.launch_sha256)
        _hash(self.attempt_sha256)
        _integer(self.session_id, 1, 32767)
        _hash(self.nonce)
        require_session_nonce(bytes.fromhex(self.nonce))


@dataclass(frozen=True)
class SqlClientDatabasePrincipal:
    """Resolved catalog identity; does not itself prove a worker token or privileges."""

    principal_id: int
    name: str
    sid: str

    def __post_init__(self) -> None:
        _integer(self.principal_id, 1, 2**31 - 1)
        _text(self.name, 128)
        if len(self.name.encode("utf-16le")) > 256:
            raise ValueError(_ERROR)
        if (
            type(self.sid) is not str
            or not 2 <= len(self.sid) <= 170
            or len(self.sid) % 2
            or any(c not in "0123456789abcdef" for c in self.sid)
        ):
            raise ValueError(_ERROR)


@dataclass(frozen=True)
class SqlClientBulkGrant:
    """Original immutable bindings, validated against independently admitted facts.

    writer_observation_sha256 identifies immutable evidence bytes, not the child's
    announcement nor a semantic digest substituted for a stored artifact hash.
    Remote identity remains an observation, never proof that a writer has exited.
    """

    schema_version: int
    grant_id: str
    launch_sha256: str
    attempt_sha256: str
    ownership: TdsAttemptOwnership
    process: TdsProcessIdentity
    object_identity: TdsObjectIdentity
    input_binding_sha256: str
    build_sha256: str
    remote_session: TdsRemoteSessionIdentity
    writer_observation_sha256: str
    operation_deadline_ns: int
    resolved_database_principal: SqlClientDatabasePrincipal

    def __post_init__(self) -> None:
        _integer(self.schema_version, 1, 1)
        _uuid(self.grant_id)
        for value in (
            self.launch_sha256,
            self.attempt_sha256,
            self.input_binding_sha256,
            self.build_sha256,
            self.writer_observation_sha256,
        ):
            _hash(value)
        for nested, cls in (
            (self.ownership, TdsAttemptOwnership),
            (self.process, TdsProcessIdentity),
            (self.object_identity, TdsObjectIdentity),
            (self.remote_session, TdsRemoteSessionIdentity),
            (self.resolved_database_principal, SqlClientDatabasePrincipal),
        ):
            if type(nested) is not cls:
                raise ValueError(_ERROR)
        _integer(self.operation_deadline_ns, 1)


def _bounded(body: bytes) -> dict:
    if type(body) is not bytes or not 0 < len(body) <= MAX_CONTROL_BYTES:
        raise ValueError(_ERROR)
    return strict_json_object(body)


def _encode(record: SqlClientSessionAnnouncement | SqlClientBulkGrant) -> bytes:
    data = asdict(record)
    if type(record) is SqlClientBulkGrant:
        data["remote_session"] = strict_json_object(encode_session_identity(record.remote_session))
    body = canonical_json_bytes(data)
    if len(body) > MAX_CONTROL_BYTES:
        raise ValueError(_ERROR)
    return body


def encode_session_announcement(record: SqlClientSessionAnnouncement) -> bytes:
    """Produce an unframed body; the transport must send once then close its end."""
    if type(record) is not SqlClientSessionAnnouncement:
        raise ValueError(_ERROR)
    return _encode(record)


def decode_session_announcement(body: bytes) -> SqlClientSessionAnnouncement:
    """Reject ambiguous fields and scalar aliases without reflecting input bytes."""
    try:
        return construct_record(SqlClientSessionAnnouncement, _bounded(body))
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError(_ERROR) from None


def encode_bulk_grant(record: SqlClientBulkGrant) -> bytes:
    """Produce the closed original grant, without framing or authority effects."""
    if type(record) is not SqlClientBulkGrant:
        raise ValueError(_ERROR)
    return _encode(record)


def decode_bulk_grant(body: bytes) -> SqlClientBulkGrant:
    """Reuse strict nested identity codecs, including canonical SQL timestamps."""
    try:
        data = record_shape(SqlClientBulkGrant, _bounded(body))
        for field, cls in (
            ("ownership", TdsAttemptOwnership),
            ("process", TdsProcessIdentity),
            ("object_identity", TdsObjectIdentity),
            ("resolved_database_principal", SqlClientDatabasePrincipal),
        ):
            data[field] = construct_record(cls, data[field])
        data["remote_session"] = decode_session_identity(canonical_json_bytes(data["remote_session"]))
        return SqlClientBulkGrant(**data)
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError(_ERROR) from None


def bulk_grant_digest(record: SqlClientBulkGrant) -> str:
    """Semantic wire binding; distinct from immutable evidence artifact byte SHA."""
    return sha256(_GRANT_DOMAIN + encode_bulk_grant(record)).hexdigest()


def _current(launch: SqlClientLaunch, now_ns: int) -> None:
    if type(launch) is not SqlClientLaunch:
        raise ValueError(_ERROR)
    _integer(now_ns)
    if now_ns >= launch.operation_deadline_ns:
        raise ValueError(_ERROR)


def validate_session_announcement(
    record: SqlClientSessionAnnouncement, *, launch: SqlClientLaunch, nonce: bytes, now_ns: int
) -> None:
    """Bind a locator before independent observation; this permits no bulk copy."""
    _current(launch, now_ns)
    require_session_nonce(nonce)
    if (
        type(record) is not SqlClientSessionAnnouncement
        or record.launch_sha256 != launch_digest(launch)
        or record.attempt_sha256 != launch.attempt_sha256
        or record.nonce != nonce.hex()
    ):
        raise ValueError(_ERROR)


def validate_bulk_grant(
    record: SqlClientBulkGrant,
    *,
    launch: SqlClientLaunch,
    ownership: TdsAttemptOwnership,
    object_identity: TdsObjectIdentity,
    remote_session: TdsRemoteSessionIdentity,
    writer_observation_sha256: str,
    resolved_database_principal: SqlClientDatabasePrincipal,
    now_ns: int,
) -> None:
    """Require every original binding and the unrenewed operation deadline.

    Callers supply trusted expectations, never values copied from the grant being
    validated. They must separately reassert the fence and confirm continuity of
    the same open connection immediately before use. No reconnect is authorized.
    """
    _current(launch, now_ns)
    _hash(writer_observation_sha256)
    if type(resolved_database_principal) is not SqlClientDatabasePrincipal:
        raise ValueError(_ERROR)
    if type(ownership) is not TdsAttemptOwnership or type(object_identity) is not TdsObjectIdentity:
        raise ValueError(_ERROR)
    if type(remote_session) is not TdsRemoteSessionIdentity or type(record) is not SqlClientBulkGrant:
        raise ValueError(_ERROR)
    if (
        record.launch_sha256 != launch_digest(launch)
        or record.attempt_sha256 != launch.attempt_sha256
        or record.ownership != ownership
        or record.process != launch.process
        or record.object_identity != object_identity
        or record.input_binding_sha256 != launch.input_binding_sha256
        or record.build_sha256 != launch.build_sha256
        or record.remote_session != remote_session
        or record.writer_observation_sha256 != writer_observation_sha256
        or record.resolved_database_principal != resolved_database_principal
        or record.operation_deadline_ns != launch.operation_deadline_ns
    ):
        raise ValueError(_ERROR)
