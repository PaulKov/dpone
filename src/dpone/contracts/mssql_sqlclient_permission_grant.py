"""Finite direct-object permission evidence, never effective-token or parent authority.

The producer must own the actual live SQL session and original execution grant.
Canonical records bind observations; constructing them cannot authorize SQL.
"""

from dataclasses import asdict, dataclass, replace
from datetime import datetime
from hashlib import sha256
from typing import Any, cast
from uuid import UUID

from dpone.contracts.mssql_sqlclient_observation import SqlClientLoginAuthority, SqlClientPrincipalResolution
from dpone.contracts.mssql_sqlclient_stage_identity import (
    SqlClientStageIdentity,
    decode_stage_identity,
    encode_stage_identity,
)
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorGrant,
    TdsCoordinatorIdentity,
    coordinator_identity_digest,
)
from dpone.contracts.mssql_tds_coordinator_authority import (
    TdsCoordinatorAuthority,
    TdsDatabaseObservation,
    TdsLockObservation,
    TdsSchemaObservation,
    authority_digest,
    decode_authority,
    encode_authority,
)
from dpone.contracts.mssql_tds_create import TdsCreateObservedColumn, TdsCreateType
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity, decode_session_identity, encode_session_identity
from dpone.contracts.mssql_tds_validation import _hash, _integer, _text
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsAttemptOwnership, TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, record_shape, string_enum

ERROR = "mssql_native.sqlclient_permission_grant_invalid"
PERMISSIONS = ("INSERT", "SELECT", "VIEW DEFINITION")
REQUEST_LIMIT = 131072
EVIDENCE_LIMIT = 1048572


def _leaf(value: Any, cls: type) -> None:
    if type(value) is not cls:
        raise ValueError(ERROR)
    replace(value)  # type: ignore[type-var]


def _uuid(value: Any) -> None:
    if type(value) is not UUID or type(value.int) is not int or not 0 < value.int < 2**128:
        raise ValueError(ERROR)


def _stage_original(value: Any) -> None:
    if type(value) is not SqlClientStageIdentity:
        raise ValueError(ERROR)
    for identifier_value in (value.database_guid, value.object_nonce):
        _uuid(identifier_value)
    if (
        any(type(number) is not int for number in (value.database_id, value.schema_id, value.object_id))
        or type(value.create_date) is not datetime
        or type(value.columns) is not tuple
    ):
        raise ValueError(ERROR)
    for column in value.columns:
        if (
            type(column) is not TdsCreateObservedColumn
            or type(column.ordinal) is not int
            or type(column.name) is not str
            or type(column.type) is not TdsCreateType
            or type(column.nullable) is not bool
            or any(type(number) is not int for number in (column.max_length, column.precision, column.scale))
            or (column.collation is not None and type(column.collation) is not str)
        ):
            raise ValueError(ERROR)
        replace(column)
    replace(value)


def _session_original(value: Any) -> None:
    _leaf(value, TdsRemoteSessionIdentity)
    _uuid(value.connection_id)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPermissionGrantRequest:
    parent: TdsAttemptIdentity
    stage: SqlClientStageIdentity
    writer: SqlClientPrincipalResolution
    writer_login: SqlClientLoginAuthority
    management: SqlClientPrincipalResolution
    management_login: SqlClientLoginAuthority
    preparation_sha256: str
    permissions: tuple[str, ...] = PERMISSIONS
    schema: str = "dpone.sqlclient.permission-grant-request.v1"

    def __post_init__(self) -> None:
        _leaf(self.parent, TdsAttemptIdentity)
        _stage_original(self.stage)
        encode_stage_identity(self.stage)
        for principal, login in ((self.writer, self.writer_login), (self.management, self.management_login)):
            _leaf(principal, SqlClientPrincipalResolution)
            _leaf(login, SqlClientLoginAuthority)
        _hash(self.preparation_sha256)
        if (
            type(self.schema) is not str
            or self.schema != "dpone.sqlclient.permission-grant-request.v1"
            or type(self.permissions) is not tuple
            or any(type(p) is not str for p in self.permissions)
            or self.permissions != PERMISSIONS
            or self.writer.kind != "mapped_user"
            or self.writer_login.is_sysadmin is not False
            or self.management.kind != "sysadmin_dbo"
            or self.management_login.is_sysadmin is not True
            or self.writer.sid != self.writer_login.sid
            or self.writer.sid == self.management.sid
            or self.writer_login.sid == self.management_login.sid
            or (self.parent.database, self.parent.schema, self.parent.table, self.parent.owner_binding)
            != (self.stage.database_name, self.stage.schema_name, self.stage.table_name, self.stage.owner_binding)
        ):
            raise ValueError(ERROR)


def _bounded(payload: bytes, limit: int) -> dict:
    if type(payload) is not bytes or not 0 < len(payload) <= limit:
        raise ValueError(ERROR)
    return strict_json_object(payload)


def encode_permission_grant_request(value: SqlClientPermissionGrantRequest) -> bytes:
    try:
        _leaf(value, SqlClientPermissionGrantRequest)
        data = asdict(value)
        data["stage"] = strict_json_object(encode_stage_identity(value.stage))
        payload = canonical_json_bytes(data)
        _bounded(payload, REQUEST_LIMIT)
        return payload
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def decode_permission_grant_request(payload: bytes) -> SqlClientPermissionGrantRequest:
    try:
        data = record_shape(SqlClientPermissionGrantRequest, _bounded(payload, REQUEST_LIMIT))
        data["parent"] = TdsAttemptIdentity(**record_shape(TdsAttemptIdentity, data["parent"]))
        data["stage"] = decode_stage_identity(canonical_json_bytes(data["stage"]))
        for name, cls in (
            ("writer", SqlClientPrincipalResolution),
            ("management", SqlClientPrincipalResolution),
            ("writer_login", SqlClientLoginAuthority),
            ("management_login", SqlClientLoginAuthority),
        ):
            data[name] = cls(**record_shape(cls, data[name]))
        if type(data["permissions"]) is not list:
            raise ValueError(ERROR)
        data["permissions"] = tuple(data["permissions"])
        result = SqlClientPermissionGrantRequest(**data)
        if encode_permission_grant_request(result) != payload:
            raise ValueError(ERROR)
        return result
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def permission_grant_digest(value: SqlClientPermissionGrantRequest) -> str:
    return sha256(b"dpone.sqlclient.permission-grant.v1\0" + encode_permission_grant_request(value)).hexdigest()


def validate_permission_binding(
    request: SqlClientPermissionGrantRequest,
    operation: TdsCoordinatorIdentity,
    grant: TdsCoordinatorGrant,
    authority: TdsCoordinatorAuthority,
) -> None:
    """Validate known original leaves before equality/digest; structures are not authority."""
    _leaf(request, SqlClientPermissionGrantRequest)
    _leaf(operation, TdsCoordinatorIdentity)
    _leaf(operation.parent, TdsAttemptIdentity)
    if type(operation.command) is not TdsCoordinatorCommand:
        raise ValueError(ERROR)
    _leaf(grant, TdsCoordinatorGrant)
    _leaf(authority, TdsCoordinatorAuthority)
    for identifier_value in (operation.operation_id, grant.grant_id, authority.database.database_guid):
        _uuid(identifier_value)
    for record, cls in (
        (grant.ownership, TdsAttemptOwnership),
        (grant.process, TdsProcessIdentity),
        (authority.execution_owner, TdsAttemptOwnership),
        (authority.process, TdsProcessIdentity),
        (authority.database, TdsDatabaseObservation),
        (authority.schema_observation, TdsSchemaObservation),
        (authority.lock, TdsLockObservation),
    ):
        _leaf(record, cls)
    if any(
        type(text_value) is not str
        for text_value in (authority.lock.resource, authority.lock.principal, authority.lock.owner, authority.lock.mode)
    ):
        raise ValueError(ERROR)
    _session_original(grant.session)
    _session_original(authority.session)
    if (
        operation.command is not TdsCoordinatorCommand.GRANT
        or operation.parent != request.parent
        or operation.command_sha256 != permission_grant_digest(request)
        or authority.operation_sha256 != coordinator_identity_digest(operation)
        or grant.operation_sha256 != authority.operation_sha256
        or grant.authority_sha256 != authority_digest(authority)
        or grant.ownership != authority.execution_owner
        or grant.ownership.fence != operation.original_fence
        or grant.process != authority.process
        or grant.session != authority.session
        or authority.implementation_sha256 != operation.implementation_sha256
        or (
            authority.database.database_id,
            authority.database.name,
            authority.database.database_guid,
            authority.schema_observation.schema_id,
            authority.schema_observation.name,
        )
        != (
            request.stage.database_id,
            request.stage.database_name,
            request.stage.database_guid,
            request.stage.schema_id,
            request.stage.schema_name,
        )
    ):
        raise ValueError(ERROR)


@dataclass(frozen=True, slots=True)
class SqlClientDirectPermission:
    class_id: int
    major_id: int
    minor_id: int
    column_name: str | None
    state: str
    permission_name: str
    grantee_id: int
    grantee_name: str
    grantee_sid: str
    grantee_type: str
    grantor_id: int
    grantor_name: str
    grantor_sid: str
    grantor_type: str

    def __post_init__(self) -> None:
        for value in (self.class_id, self.major_id, self.grantee_id, self.grantor_id):
            _integer(value, 1, 2**31 - 1)
        _integer(self.minor_id, 0, 2**31 - 1)
        if self.class_id != 1 or (self.column_name is None) != (self.minor_id == 0):
            raise ValueError(ERROR)
        if self.column_name is not None:
            _text(self.column_name, 128)
        for text_value in (self.state, self.permission_name, self.grantee_type, self.grantor_type):
            _text(text_value, 128)
        for id_, name, sid in (
            (self.grantee_id, self.grantee_name, self.grantee_sid),
            (self.grantor_id, self.grantor_name, self.grantor_sid),
        ):
            SqlClientPrincipalResolution("mapped_user" if id_ > 4 else "sysadmin_dbo", id_, name, sid)


def parse_direct_permissions(rows: tuple[tuple, ...]) -> tuple[SqlClientDirectPermission, ...]:
    if type(rows) is not tuple or len(rows) > 4096:
        raise ValueError(ERROR)
    result = []
    for row in rows:
        if type(row) is not tuple or len(row) != 14:
            raise ValueError(ERROR)
        values = list(row)
        for index in (8, 12):
            if type(values[index]) is not bytes or not 1 <= len(values[index]) <= 85:
                raise ValueError(ERROR)
            values[index] = values[index].hex()
        result.append(SqlClientDirectPermission(*values))
    return tuple(result)


def validate_direct_permissions(
    request: SqlClientPermissionGrantRequest, rows: tuple[SqlClientDirectPermission, ...]
) -> None:
    _leaf(request, SqlClientPermissionGrantRequest)
    if type(rows) is not tuple or len(rows) != 3:
        raise ValueError(ERROR)
    for row in rows:
        _leaf(row, SqlClientDirectPermission)
    expected = tuple(
        SqlClientDirectPermission(
            1,
            request.stage.object_id,
            0,
            None,
            "G",
            permission,
            request.writer.principal_id,
            request.writer.name,
            request.writer.sid,
            "SQL_USER",
            request.management.principal_id,
            request.management.name,
            request.management.sid,
            "SQL_USER",
        )
        for permission in PERMISSIONS
    )
    if set(rows) != set(expected) or len(set(rows)) != 3:
        raise ValueError(ERROR)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPermissionGrantEvidence:
    request: SqlClientPermissionGrantRequest
    operation: TdsCoordinatorIdentity
    grant: TdsCoordinatorGrant
    authority: TdsCoordinatorAuthority
    direct_permissions: tuple[SqlClientDirectPermission, ...]
    schema: str = "dpone.sqlclient.direct-permission-grant-evidence.v1"

    def __post_init__(self) -> None:
        if type(self.schema) is not str or self.schema != "dpone.sqlclient.direct-permission-grant-evidence.v1":
            raise ValueError(ERROR)
        validate_permission_binding(self.request, self.operation, self.grant, self.authority)
        validate_direct_permissions(self.request, self.direct_permissions)


def encode_permission_grant_evidence(value: SqlClientPermissionGrantEvidence) -> bytes:
    try:
        _leaf(value, SqlClientPermissionGrantEvidence)
        data = asdict(value)
        data["request"] = strict_json_object(encode_permission_grant_request(value.request))
        data["operation"]["operation_id"] = str(value.operation.operation_id)
        data["grant"]["grant_id"] = str(value.grant.grant_id)
        data["grant"]["session"] = strict_json_object(
            encode_session_identity(cast(TdsRemoteSessionIdentity, value.grant.session))
        )
        data["authority"] = strict_json_object(encode_authority(value.authority))
        payload = canonical_json_bytes(data)
        _bounded(payload, EVIDENCE_LIMIT)
        return payload
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None


def decode_permission_grant_evidence(payload: bytes) -> SqlClientPermissionGrantEvidence:
    try:
        data = record_shape(SqlClientPermissionGrantEvidence, _bounded(payload, EVIDENCE_LIMIT))
        data["request"] = decode_permission_grant_request(canonical_json_bytes(data["request"]))
        op = record_shape(TdsCoordinatorIdentity, data["operation"])
        op["parent"] = TdsAttemptIdentity(**record_shape(TdsAttemptIdentity, op["parent"]))
        op["operation_id"] = canonical_uuid(op["operation_id"])
        op["command"] = string_enum(TdsCoordinatorCommand, op["command"])
        data["operation"] = TdsCoordinatorIdentity(**op)
        grant = record_shape(TdsCoordinatorGrant, data["grant"])
        grant["grant_id"] = canonical_uuid(grant["grant_id"])
        grant["session"] = decode_session_identity(canonical_json_bytes(grant["session"]))
        for name, cls in (("ownership", TdsAttemptOwnership), ("process", TdsProcessIdentity)):
            grant[name] = cls(**record_shape(cls, grant[name]))
        data["grant"] = TdsCoordinatorGrant(**grant)
        data["authority"] = decode_authority(canonical_json_bytes(data["authority"]))
        if type(data["direct_permissions"]) is not list or len(data["direct_permissions"]) != 3:
            raise ValueError(ERROR)
        data["direct_permissions"] = tuple(
            SqlClientDirectPermission(**record_shape(SqlClientDirectPermission, row))
            for row in data["direct_permissions"]
        )
        result = SqlClientPermissionGrantEvidence(**data)
        if encode_permission_grant_evidence(result) != payload:
            raise ValueError(ERROR)
        return result
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise ValueError(ERROR) from None
