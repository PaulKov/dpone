"""Closed one-shot wire contract for the contained P10d writer observer."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any, cast

from dpone.contracts.mssql_sqlclient_credentials import SqlClientCredentials
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientPrincipalResolution,
    SqlClientServerAuthority,
    SqlClientSessionAuthority,
    SqlClientTransportAuthority,
    SqlClientWriterObservation,
    encode_session_authority,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation import SqlClientObserverIncarnation
from dpone.contracts.mssql_sqlclient_observer_incarnation_codec import (
    decode_observer_incarnation,
    encode_observer_incarnation,
)
from dpone.contracts.mssql_tds_session import decode_session_identity, encode_session_identity, require_session_nonce
from dpone.contracts.mssql_tds_validation import _hash, _integer, deadline_nanoseconds, deadline_seconds
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape

ERROR = "mssql_native.sqlclient_writer_observer_wire_invalid"
MAX_CONTROL_BYTES = 64 * 1024
MAX_CREDENTIAL_BYTES = 32 * 1024


def validate_deadline(deadline: float) -> None:
    """Validate one protocol deadline without extending its clock budget."""
    deadline_nanoseconds(deadline)


def _admission(data: dict[str, Any]) -> SqlClientObserverAdmission:
    shaped = record_shape(SqlClientObserverAdmission, data)
    for name, cls in (
        ("server", SqlClientServerAuthority),
        ("database", SqlClientDatabaseAuthority),
        ("login", SqlClientLoginAuthority),
        ("transport", SqlClientTransportAuthority),
    ):
        shaped[name] = construct_record(cls, shaped[name])
    return SqlClientObserverAdmission(**shaped)


def _authority(data: dict[str, Any]) -> SqlClientSessionAuthority:
    shaped = record_shape(SqlClientSessionAuthority, data)
    for name, cls in (
        ("server", SqlClientServerAuthority),
        ("database", SqlClientDatabaseAuthority),
        ("login", SqlClientLoginAuthority),
        ("transport", SqlClientTransportAuthority),
        ("principal_resolution", SqlClientPrincipalResolution),
    ):
        shaped[name] = construct_record(cls, shaped[name])
    return SqlClientSessionAuthority(**shaped)


@dataclass(frozen=True, slots=True, kw_only=True)
class WriterObserverRequest:
    attempt_sha256: str
    launch_sha256: str
    observer_admission: SqlClientObserverAdmission
    target_admission: SqlClientObserverAdmission
    operation_deadline_ns: int
    schema: str = "dpone.sqlclient.writer-observer-request.v1"

    def __post_init__(self) -> None:
        _hash(self.attempt_sha256)
        _hash(self.launch_sha256)
        if self.schema != "dpone.sqlclient.writer-observer-request.v1":
            raise ValueError(ERROR)
        for value in (self.observer_admission, self.target_admission):
            if type(value) is not SqlClientObserverAdmission:
                raise ValueError(ERROR)
            value.__post_init__()
        if (
            type(self.operation_deadline_ns) is not int
            or self.observer_admission.server != self.target_admission.server
            or self.observer_admission.database != self.target_admission.database
            or self.observer_admission.login == self.target_admission.login
            or self.observer_admission.login.sid == self.target_admission.login.sid
        ):
            raise ValueError(ERROR)
        _integer(self.operation_deadline_ns, 1)
        deadline_seconds(self.operation_deadline_ns)

    @property
    def operation_deadline(self) -> float:
        """Project the authenticated integer ceiling downward to binary64 seconds."""
        return deadline_seconds(self.operation_deadline_ns)


@dataclass(frozen=True, slots=True, kw_only=True)
class WriterObserverCommand:
    request_sha256: str
    session_id: int
    nonce: str
    schema: str = "dpone.sqlclient.writer-observer-command.v1"

    def __post_init__(self) -> None:
        _hash(self.request_sha256)
        _integer(self.session_id, 1, 32767)
        require_session_nonce(bytes.fromhex(self.nonce))
        if self.nonce != self.nonce.lower() or self.schema != "dpone.sqlclient.writer-observer-command.v1":
            raise ValueError(ERROR)


def _encode(value: object, limit: int) -> bytes:
    try:
        value.__post_init__()  # type: ignore[attr-defined]
        payload = canonical_json_bytes(asdict(cast(Any, value)))
        if not 0 < len(payload) <= limit:
            raise ValueError
        return payload
    except (ValueError, TypeError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None


def encode_request(value: WriterObserverRequest) -> bytes:
    return _encode(value, MAX_CONTROL_BYTES)


def decode_request(payload: bytes) -> WriterObserverRequest:
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= MAX_CONTROL_BYTES:
            raise ValueError
        data = record_shape(WriterObserverRequest, strict_json_object(payload))
        data["observer_admission"] = _admission(data["observer_admission"])
        data["target_admission"] = _admission(data["target_admission"])
        value = WriterObserverRequest(**data)
        if encode_request(value) != payload:
            raise ValueError
        return value
    except (ValueError, TypeError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None


def request_digest(value: WriterObserverRequest) -> str:
    return sha256(encode_request(value)).hexdigest()


def encode_request_ack(request: WriterObserverRequest, launch_nonce: bytes) -> bytes:
    require_session_nonce(launch_nonce)
    return canonical_json_bytes(
        {
            "schema": "dpone.sqlclient.writer-observer-request-ack.v1",
            "request_sha256": request_digest(request),
            "launch_nonce": launch_nonce.hex(),
        }
    )


def validate_request_ack(payload: bytes, request: WriterObserverRequest, launch_nonce: bytes) -> None:
    if payload != encode_request_ack(request, launch_nonce):
        raise ValueError(ERROR)


def encode_credentials(credentials: SqlClientCredentials, request: WriterObserverRequest, launch_nonce: bytes) -> bytes:
    if type(credentials) is not SqlClientCredentials:
        raise ValueError(ERROR)
    credentials.__post_init__()
    require_session_nonce(launch_nonce)
    return _encode(
        _CredentialFrame(
            request_sha256=request_digest(request), launch_nonce=launch_nonce.hex(), credentials=credentials
        ),
        MAX_CREDENTIAL_BYTES,
    )


@dataclass(frozen=True, slots=True, repr=False)
class _CredentialFrame:
    request_sha256: str
    launch_nonce: str
    credentials: SqlClientCredentials
    schema: str = "dpone.sqlclient.writer-observer-credentials.v1"

    def __post_init__(self) -> None:
        _hash(self.request_sha256)
        require_session_nonce(bytes.fromhex(self.launch_nonce))
        if (
            type(self.credentials) is not SqlClientCredentials
            or self.schema != "dpone.sqlclient.writer-observer-credentials.v1"
        ):
            raise ValueError(ERROR)
        self.credentials.__post_init__()


def decode_credentials(payload: bytes, request: WriterObserverRequest, launch_nonce: bytes) -> SqlClientCredentials:
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= MAX_CREDENTIAL_BYTES:
            raise ValueError
        data = record_shape(_CredentialFrame, strict_json_object(payload))
        data["credentials"] = construct_record(SqlClientCredentials, data["credentials"])
        frame = _CredentialFrame(**data)
        if (
            _encode(frame, MAX_CREDENTIAL_BYTES) != payload
            or frame.request_sha256 != request_digest(request)
            or frame.launch_nonce != launch_nonce.hex()
        ):
            raise ValueError
        return frame.credentials
    except (ValueError, TypeError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None


def encode_ready(request: WriterObserverRequest, launch_nonce: bytes, value: SqlClientObserverIncarnation) -> bytes:
    return canonical_json_bytes(
        {
            "schema": "dpone.sqlclient.writer-observer-ready.v1",
            "request_sha256": request_digest(request),
            "launch_nonce": launch_nonce.hex(),
            "incarnation": strict_json_object(encode_observer_incarnation(value)),
        }
    )


def decode_ready(payload: bytes, request: WriterObserverRequest, launch_nonce: bytes) -> SqlClientObserverIncarnation:
    try:
        data = strict_json_object(payload)
        if (
            set(data) != {"schema", "request_sha256", "launch_nonce", "incarnation"}
            or data["schema"] != "dpone.sqlclient.writer-observer-ready.v1"
            or data["request_sha256"] != request_digest(request)
            or data["launch_nonce"] != launch_nonce.hex()
        ):
            raise ValueError
        value = decode_observer_incarnation(canonical_json_bytes(data["incarnation"]))
        if encode_ready(request, launch_nonce, value) != payload:
            raise ValueError
        return value
    except (ValueError, TypeError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None


def encode_command(value: WriterObserverCommand) -> bytes:
    return _encode(value, MAX_CONTROL_BYTES)


def decode_command(payload: bytes, request: WriterObserverRequest) -> WriterObserverCommand:
    try:
        value = WriterObserverCommand(**record_shape(WriterObserverCommand, strict_json_object(payload)))
        if encode_command(value) != payload or value.request_sha256 != request_digest(request):
            raise ValueError
        return value
    except (ValueError, TypeError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None


def encode_observation(
    request: WriterObserverRequest, command: WriterObserverCommand, value: SqlClientWriterObservation
) -> bytes:
    if type(value) is not SqlClientWriterObservation:
        raise ValueError(ERROR)
    return canonical_json_bytes(
        {
            "schema": "dpone.sqlclient.writer-observer-result.v1",
            "request_sha256": request_digest(request),
            "command_sha256": sha256(encode_command(command)).hexdigest(),
            "remote_session": strict_json_object(encode_session_identity(value.remote_session)),
            "authority": strict_json_object(encode_session_authority(value.authority)),
        }
    )


def decode_observation(
    payload: bytes, request: WriterObserverRequest, command: WriterObserverCommand
) -> SqlClientWriterObservation:
    try:
        data = strict_json_object(payload)
        if (
            set(data) != {"schema", "request_sha256", "command_sha256", "remote_session", "authority"}
            or data["schema"] != "dpone.sqlclient.writer-observer-result.v1"
            or data["request_sha256"] != request_digest(request)
            or data["command_sha256"] != sha256(encode_command(command)).hexdigest()
        ):
            raise ValueError
        value = SqlClientWriterObservation(
            decode_session_identity(canonical_json_bytes(data["remote_session"])), _authority(data["authority"])
        )
        if encode_observation(request, command, value) != payload:
            raise ValueError
        return value
    except (ValueError, TypeError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None


def terminal_ack(request: WriterObserverRequest, command: WriterObserverCommand) -> bytes:
    return canonical_json_bytes(
        {
            "schema": "dpone.sqlclient.writer-observer-terminal.v1",
            "request_sha256": request_digest(request),
            "command_sha256": sha256(encode_command(command)).hexdigest(),
        }
    )
