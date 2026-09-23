"""Closed, bounded facts for the restricted-writer VERIFY operation."""

from __future__ import annotations

from dataclasses import dataclass, replace
from hashlib import sha256
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_sqlclient_observation import SqlClientLoginAuthority, SqlClientPrincipalResolution
from dpone.contracts.mssql_sqlclient_stage_identity import SqlClientStageIdentity, encode_stage_identity
from dpone.contracts.mssql_tds_session import TdsRestrictedRemoteSessionIdentity
from dpone.contracts.mssql_tds_validation import _hash, _integer, _text
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity

ERROR = "mssql_native.sqlclient_restricted_writer_verify_invalid"
STAGE_PERMISSIONS = ("INSERT", "SELECT", "VIEW DEFINITION")
_FINGERPRINT_KINDS = frozenset(
    {"database", "login", "original-login", "permission-entity", "permission-subentity", "token", "user"}
)


def restricted_writer_fingerprint(kind: str, value: str) -> str:
    """Return a domain-separated, non-reversible fingerprint for one observed name."""
    if type(kind) is not str or kind not in _FINGERPRINT_KINDS:
        raise ValueError(ERROR)
    _text(value, 128)
    return sha256(b"dpone.sqlclient.restricted-writer-name.v1\0" + kind.encode() + b"\0" + value.encode()).hexdigest()


def restricted_writer_stage_fingerprint(stage: SqlClientStageIdentity) -> str:
    """Bind the observed target without retaining its catalog names in RESULT."""
    _exact(stage, SqlClientStageIdentity)
    return sha256(b"dpone.sqlclient.restricted-writer-stage.v1\0" + encode_stage_identity(stage)).hexdigest()


def _exact(value: Any, cls: type) -> None:
    if type(value) is not cls:
        raise ValueError(ERROR)
    replace(value)  # type: ignore[type-var]


def _sid(value: str) -> None:
    if type(value) is not str:
        raise ValueError(ERROR)
    try:
        raw = bytes.fromhex(value)
    except ValueError:
        raise ValueError(ERROR) from None
    if not 1 <= len(raw) <= 85 or raw.hex() != value:
        raise ValueError(ERROR)


@dataclass(frozen=True, slots=True)
class SqlClientEffectivePermission:
    entity_name: str | None
    subentity_name: str | None
    permission_name: str

    def __post_init__(self) -> None:
        for value in (self.entity_name, self.subentity_name):
            if value is not None:
                _text(value, 128)
        _text(self.permission_name, 128)


@dataclass(frozen=True, slots=True)
class SqlClientEffectivePermissionFingerprint:
    entity_name_sha256: str | None
    subentity_name_sha256: str | None
    permission_name: str

    def __post_init__(self) -> None:
        for value in (self.entity_name_sha256, self.subentity_name_sha256):
            if value is not None:
                _hash(value)
        _text(self.permission_name, 128)


@dataclass(frozen=True, slots=True)
class SqlClientTokenRow:
    principal_id: int
    sid: str
    name: str
    type: str
    usage: str

    def __post_init__(self) -> None:
        _integer(self.principal_id, 0, 2**31 - 1)
        _sid(self.sid)
        for value in (self.name, self.type, self.usage):
            _text(value, 128)


@dataclass(frozen=True, slots=True)
class SqlClientTokenFingerprint:
    principal_id: int
    sid: str
    name_sha256: str
    type: str
    usage: str

    def __post_init__(self) -> None:
        _integer(self.principal_id, 0, 2**31 - 1)
        _sid(self.sid)
        _hash(self.name_sha256)
        for value in (self.type, self.usage):
            _text(value, 128)


@dataclass(frozen=True, slots=True)
class RestrictedWriterSessionContext:
    session: TdsRestrictedRemoteSessionIdentity
    login_name_sha256: str
    login_sid: str
    original_login_name_sha256: str
    original_login_sid: str
    authenticating_database_id: int
    database_id: int
    database_name_sha256: str
    user_id: int
    user_name_sha256: str
    user_sid: str
    is_sysadmin: bool
    is_db_owner: bool
    is_dbo: bool
    request_count: int
    open_transaction_count: int
    transaction_count: int
    xact_state: int
    implicit_transactions: bool

    def __post_init__(self) -> None:
        _exact(self.session, TdsRestrictedRemoteSessionIdentity)
        for fingerprint in (
            self.login_name_sha256,
            self.original_login_name_sha256,
            self.database_name_sha256,
            self.user_name_sha256,
        ):
            _hash(fingerprint)
        for sid_value in (self.login_sid, self.original_login_sid, self.user_sid):
            _sid(sid_value)
        _integer(self.authenticating_database_id, 0, 1)
        for identity_value in (self.database_id, self.user_id):
            _integer(identity_value, 1, 2**31 - 1)
        for count_value in (self.request_count, self.open_transaction_count, self.transaction_count):
            _integer(count_value, 0, 2**31 - 1)
        if (
            any(
                type(value) is not bool
                for value in (self.is_sysadmin, self.is_db_owner, self.is_dbo, self.implicit_transactions)
            )
            or type(self.xact_state) is not int
            or self.xact_state not in (-1, 0, 1)
        ):
            raise ValueError(ERROR)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientRestrictedWriterVerifyRequest:
    parent: TdsAttemptIdentity
    stage: SqlClientStageIdentity
    writer: SqlClientPrincipalResolution
    writer_login: SqlClientLoginAuthority
    login_token: tuple[SqlClientTokenRow, ...]
    user_token: tuple[SqlClientTokenRow, ...]
    server_permissions: tuple[SqlClientEffectivePermission, ...]
    database_permissions: tuple[SqlClientEffectivePermission, ...]
    operation_id: UUID
    implementation_sha256: str
    schema: str = "dpone.sqlclient.restricted-writer-verify-request.v1"

    def __post_init__(self) -> None:
        _exact(self.parent, TdsAttemptIdentity)
        _exact(self.stage, SqlClientStageIdentity)
        encode_stage_identity(self.stage)
        _exact(self.writer, SqlClientPrincipalResolution)
        _exact(self.writer_login, SqlClientLoginAuthority)
        _hash(self.implementation_sha256)
        if (
            self.schema != "dpone.sqlclient.restricted-writer-verify-request.v1"
            or type(self.operation_id) is not UUID
            or not self.operation_id.int
            or self.writer.kind != "mapped_user"
            or self.writer_login.is_sysadmin is not False
            or self.writer.sid != self.writer_login.sid
            or (self.parent.database, self.parent.schema, self.parent.table, self.parent.owner_binding)
            != (self.stage.database_name, self.stage.schema_name, self.stage.table_name, self.stage.owner_binding)
        ):
            raise ValueError(ERROR)
        for values in (self.server_permissions, self.database_permissions):
            if type(values) is not tuple or len(values) > 128:
                raise ValueError(ERROR)
            for value in values:
                _exact(value, SqlClientEffectivePermission)
        for token_values in (self.login_token, self.user_token):
            if type(token_values) is not tuple or not 1 <= len(token_values) <= 8:
                raise ValueError(ERROR)
            for token_value in token_values:
                _exact(token_value, SqlClientTokenRow)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientRestrictedWriterVerifyResult:
    opening: RestrictedWriterSessionContext
    login_token: tuple[SqlClientTokenFingerprint, ...]
    user_token: tuple[SqlClientTokenFingerprint, ...]
    server_permissions: tuple[SqlClientEffectivePermissionFingerprint, ...]
    database_permissions: tuple[SqlClientEffectivePermissionFingerprint, ...]
    stage_permissions: tuple[str, ...]
    stage_sha256: str
    empty_count: int
    closing: RestrictedWriterSessionContext
    closing_stage_permissions: tuple[str, ...]
    closing_stage_sha256: str
    closing_empty_count: int
    schema: str = "dpone.sqlclient.restricted-writer-verify-result.v1"

    def __post_init__(self) -> None:
        for context_value in (self.opening, self.closing):
            _exact(context_value, RestrictedWriterSessionContext)
        for values, cls, limit in (
            (self.login_token, SqlClientTokenFingerprint, 8),
            (self.user_token, SqlClientTokenFingerprint, 8),
            (self.server_permissions, SqlClientEffectivePermissionFingerprint, 128),
            (self.database_permissions, SqlClientEffectivePermissionFingerprint, 128),
        ):
            if type(values) is not tuple or len(values) > limit:
                raise ValueError(ERROR)
            for item in values:
                _exact(item, cls)
        for stage_digest in (self.stage_sha256, self.closing_stage_sha256):
            _hash(stage_digest)
        if (
            self.schema != "dpone.sqlclient.restricted-writer-verify-result.v1"
            or self.stage_permissions != STAGE_PERMISSIONS
            or self.closing_stage_permissions != STAGE_PERMISSIONS
            or type(self.empty_count) is not int
            or type(self.closing_empty_count) is not int
            or self.empty_count != 0
            or self.closing_empty_count != 0
        ):
            raise ValueError(ERROR)


def _fingerprint_tokens(values: tuple[SqlClientTokenRow, ...]) -> tuple[SqlClientTokenFingerprint, ...]:
    return tuple(
        SqlClientTokenFingerprint(
            value.principal_id,
            value.sid,
            restricted_writer_fingerprint("token", value.name),
            value.type,
            value.usage,
        )
        for value in values
    )


def _fingerprint_permissions(
    values: tuple[SqlClientEffectivePermission, ...],
) -> tuple[SqlClientEffectivePermissionFingerprint, ...]:
    return tuple(
        SqlClientEffectivePermissionFingerprint(
            None
            if value.entity_name is None
            else restricted_writer_fingerprint("permission-entity", value.entity_name),
            None
            if value.subentity_name is None
            else restricted_writer_fingerprint("permission-subentity", value.subentity_name),
            value.permission_name,
        )
        for value in values
    )


def validate_verify_result(
    request: SqlClientRestrictedWriterVerifyRequest,
    result: SqlClientRestrictedWriterVerifyResult,
) -> None:
    """Require the complete admitted opening and immediate closing projection."""
    _exact(request, SqlClientRestrictedWriterVerifyRequest)
    _exact(result, SqlClientRestrictedWriterVerifyResult)
    request.__post_init__()
    result.__post_init__()
    opening, closing = result.opening, result.closing
    if (
        opening != closing
        or opening.session.nonce != closing.session.nonce
        or result.login_token != _fingerprint_tokens(request.login_token)
        or result.user_token != _fingerprint_tokens(request.user_token)
        or result.server_permissions != _fingerprint_permissions(request.server_permissions)
        or result.database_permissions != _fingerprint_permissions(request.database_permissions)
        or result.stage_sha256 != restricted_writer_stage_fingerprint(request.stage)
        or result.closing_stage_sha256 != restricted_writer_stage_fingerprint(request.stage)
    ):
        raise ValueError(ERROR)
    validate_verify_opening(request, opening)


def validate_verify_opening(
    request: SqlClientRestrictedWriterVerifyRequest,
    opening: RestrictedWriterSessionContext,
) -> None:
    _exact(request, SqlClientRestrictedWriterVerifyRequest)
    _exact(opening, RestrictedWriterSessionContext)
    if (
        opening.login_name_sha256 != restricted_writer_fingerprint("login", request.writer_login.name)
        or opening.login_sid != request.writer_login.sid
        or opening.original_login_name_sha256
        != restricted_writer_fingerprint("original-login", request.writer_login.name)
        or opening.original_login_sid != request.writer_login.sid
        or opening.authenticating_database_id != request.writer_login.authenticating_database_id
        or opening.user_id != request.writer.principal_id
        or opening.user_name_sha256 != restricted_writer_fingerprint("user", request.writer.name)
        or opening.user_sid != request.writer.sid
        or opening.database_id != request.stage.database_id
        or opening.database_name_sha256 != restricted_writer_fingerprint("database", request.stage.database_name)
        or opening.is_sysadmin
        or opening.is_db_owner
        or opening.is_dbo
        or opening.request_count != 1
        or opening.open_transaction_count != 0
        or opening.transaction_count != 0
        or opening.xact_state not in (0, 1)
        or opening.implicit_transactions
    ):
        raise ValueError(ERROR)
