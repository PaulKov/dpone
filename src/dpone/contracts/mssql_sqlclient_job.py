"""Closed memory-only SqlClient job; parsing is not permission to execute.

Never persist/log encoded jobs or hash their secret bytes. Evidence uses only the
explicit nonsecret projection below. Original file observations, trusted policy
admission, one-shot delivery and SQL authority belong to the owning composition.
"""

from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Literal

from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy
from dpone.contracts.mssql_sqlclient_credentials import SqlClientCredentials
from dpone.contracts.mssql_sqlclient_input import (
    SqlClientInputDescriptor,
    decode_input_descriptor,
    input_descriptor_digest,
)
from dpone.contracts.mssql_sqlclient_launch import SqlClientLaunch, launch_digest
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_session import require_session_nonce
from dpone.contracts.mssql_tds_validation import _hash, _integer
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsAttemptOwnership, TdsObjectIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape

MAX_JOB_BYTES = 1 << 20
_ERROR = "mssql_native.sqlclient_job_invalid"


@dataclass(frozen=True, repr=False)
class SqlClientJob:
    """Original attempt and path-free input; empty work carries no SQL credentials."""

    schema_version: int
    launch_sha256: str
    identity: TdsAttemptIdentity
    ownership: TdsAttemptOwnership
    object_identity: TdsObjectIdentity
    input: SqlClientInputDescriptor
    input_mode: Literal["rows", "arrow"]
    batch_rows: int
    max_input_batch_bytes: int
    credentials: SqlClientCredentials | None
    session_nonce: str | None

    def __post_init__(self) -> None:
        try:
            _integer(self.schema_version, 1, 1)
            _hash(self.launch_sha256)
            _integer(self.batch_rows, 1, 65536)
            _integer(self.max_input_batch_bytes, 1 << 20, 256 << 20)
            for value, cls in (
                (self.identity, TdsAttemptIdentity),
                (self.ownership, TdsAttemptOwnership),
                (self.object_identity, TdsObjectIdentity),
                (self.input, SqlClientInputDescriptor),
            ):
                if type(value) is not cls:
                    raise ValueError(_ERROR)
            if type(self.input_mode) is not str or self.input_mode not in ("rows", "arrow"):
                raise ValueError(_ERROR)
            for identifier in (self.identity.database, self.identity.schema, self.identity.table):
                if len(identifier.encode("utf-16le")) > 256:
                    raise ValueError(_ERROR)
            if self.identity.file_sha256 != self.input.expected.file_sha256:
                raise ValueError(_ERROR)
            if self.input.expected.rows == 0:
                if self.credentials is not None or self.session_nonce is not None:
                    raise ValueError(_ERROR)
            else:
                if type(self.credentials) is not SqlClientCredentials:
                    raise ValueError(_ERROR)
                if self.credentials.database != self.identity.database:
                    raise ValueError(_ERROR)
                if type(self.session_nonce) is not str:
                    raise ValueError(_ERROR)
                _hash(self.session_nonce)
                require_session_nonce(bytes.fromhex(self.session_nonce))
        except (ValueError, TypeError, UnicodeError):
            raise ValueError(_ERROR) from None


def _nonsecret_payload(record: SqlClientJob) -> dict:
    """Build explicitly; never serialize credentials then remove them."""
    if type(record) is not SqlClientJob:
        raise ValueError(_ERROR)
    return {
        "schema_version": record.schema_version,
        "launch_sha256": record.launch_sha256,
        "identity": asdict(record.identity),
        "ownership": asdict(record.ownership),
        "object_identity": asdict(record.object_identity),
        "input": asdict(record.input),
        "input_mode": record.input_mode,
        "batch_rows": record.batch_rows,
        "max_input_batch_bytes": record.max_input_batch_bytes,
        "session_nonce": record.session_nonce,
    }


def encode_job(record: SqlClientJob) -> bytes:
    """Secret body for one private inherited channel only; transport adds framing."""
    data = _nonsecret_payload(record)
    data["credentials"] = None if record.credentials is None else asdict(record.credentials)
    body = canonical_json_bytes(data)
    if len(body) > MAX_JOB_BYTES:
        raise ValueError(_ERROR)
    return body


def decode_job(body: bytes) -> SqlClientJob:
    """Reject all unknown/duplicate fields and scalar aliases without echoing data."""
    try:
        if type(body) is not bytes or not 0 < len(body) <= MAX_JOB_BYTES:
            raise ValueError(_ERROR)
        data = record_shape(SqlClientJob, strict_json_object(body))
        for field, cls in (
            ("identity", TdsAttemptIdentity),
            ("ownership", TdsAttemptOwnership),
            ("object_identity", TdsObjectIdentity),
        ):
            data[field] = construct_record(cls, data[field])
        data["input"] = decode_input_descriptor(canonical_json_bytes(data["input"]))
        if data["credentials"] is not None:
            data["credentials"] = construct_record(SqlClientCredentials, data["credentials"])
        return SqlClientJob(**data)
    except (ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError(_ERROR) from None


def job_binding_digest(record: SqlClientJob) -> str:
    """Secret-free semantic binding, not an immutable evidence artifact byte hash.

    TLS/principal authority is bound by separate trusted intent and observation;
    this projection deliberately makes no credential-identity assertion.
    """
    data = _nonsecret_payload(record)
    data["credentials_present"] = record.credentials is not None
    return sha256(b"dpone.sqlclient.job-binding.v1\0" + canonical_json_bytes(data)).hexdigest()


def validate_job(
    record: SqlClientJob,
    *,
    launch: SqlClientLaunch,
    ownership: TdsAttemptOwnership,
    object_identity: TdsObjectIdentity,
    policy: NativeBulkTransportPolicy,
    session_nonce: str | None,
    tls_profile: Literal["verified", "disposable_test"] | None,
    allow_disposable_test: bool,
    now_ns: int,
) -> None:
    """Compare with trusted originals, never expectations copied from this job.

    The caller binds the full admitted policy to the attempt before launch. Its
    identity may cover more than this transport DTO; no replacement policy hash
    is invented here. Disposable TLS permission must come from explicitly approved
    synthetic composition, never from hostname or child output. No SQL is opened.
    """
    if (
        type(record) is not SqlClientJob
        or type(launch) is not SqlClientLaunch
        or type(ownership) is not TdsAttemptOwnership
        or type(object_identity) is not TdsObjectIdentity
        or type(policy) is not NativeBulkTransportPolicy
        or type(allow_disposable_test) is not bool
    ):
        raise ValueError(_ERROR)
    try:
        _integer(now_ns)
    except ValueError:
        raise ValueError(_ERROR) from None
    actual_tls = None if record.credentials is None else record.credentials.tls_profile
    if (
        record.launch_sha256 != launch_digest(launch)
        or attempt_identity_digest(record.identity) != launch.attempt_sha256
        or input_descriptor_digest(record.input) != launch.input_binding_sha256
        or record.input.fd != launch.descriptors.input
        or record.ownership != ownership
        or record.object_identity != object_identity
        or policy.backend != "mssql_sqlclient"
        or record.input_mode != policy.input
        or record.batch_rows != policy.batch_rows
        or record.max_input_batch_bytes != policy.max_input_batch_bytes
        or launch.address_space_bytes != policy.max_worker_address_space_bytes
        or record.session_nonce != session_nonce
        or actual_tls != tls_profile
        or (actual_tls == "disposable_test" and not allow_disposable_test)
        or now_ns >= launch.operation_deadline_ns
    ):
        raise ValueError(_ERROR)
