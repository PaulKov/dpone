"""Strict canonical codecs for public P9a request and bounded result."""

from dataclasses import asdict
from hashlib import sha256
from typing import Any, cast

from dpone.contracts.mssql_sqlclient_observation import SqlClientLoginAuthority, SqlClientPrincipalResolution
from dpone.contracts.mssql_sqlclient_restricted_writer_verify import (
    ERROR,
    RestrictedWriterSessionContext,
    SqlClientEffectivePermission,
    SqlClientEffectivePermissionFingerprint,
    SqlClientRestrictedWriterVerifyRequest,
    SqlClientRestrictedWriterVerifyResult,
    SqlClientTokenFingerprint,
    SqlClientTokenRow,
)
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_handshake import (
    RestrictedWriterProbeAuthorization,
    RestrictedWriterVerifyOpening,
)
from dpone.contracts.mssql_sqlclient_stage_identity import decode_stage_identity, encode_stage_identity
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_session import (
    decode_restricted_session_identity,
    encode_restricted_session_identity,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, construct_record, record_shape

REQUEST_LIMIT = 524288
RESULT_LIMIT = 1048576
OPENING_LIMIT = 65536
AUTHORIZATION_LIMIT = 4096
OPENING_TYPE = RestrictedWriterVerifyOpening


def canonical_verify_json(value: object) -> bytes:
    """Encode one canonical P9 wire object."""
    return canonical_json_bytes(value)


def strict_verify_object(payload: bytes) -> dict[str, Any]:
    """Decode one exact canonical P9 wire object."""
    return strict_json_object(payload)


def decode_verify_process(value: object) -> TdsProcessIdentity:
    """Construct the nominal process identity accepted by P9 frames."""
    return construct_record(TdsProcessIdentity, value)


def decode_verify_material(value: object) -> TdsConnectionMaterial:
    """Construct the nominal connection material accepted by the private frame."""
    return TdsConnectionMaterial(**record_shape(TdsConnectionMaterial, value))


def _bounded(payload: bytes, limit: int) -> dict[str, Any]:
    if type(payload) is not bytes or not 0 < len(payload) <= limit:
        raise ValueError(ERROR)
    body = strict_json_object(payload)
    if canonical_json_bytes(body) != payload:
        raise ValueError(ERROR)
    return body


def _permissions(values: Any) -> tuple[SqlClientEffectivePermission, ...]:
    if type(values) is not list:
        raise ValueError(ERROR)
    return tuple(construct_record(SqlClientEffectivePermission, value) for value in values)


def _permission_fingerprints(values: Any) -> tuple[SqlClientEffectivePermissionFingerprint, ...]:
    if type(values) is not list:
        raise ValueError(ERROR)
    return tuple(construct_record(SqlClientEffectivePermissionFingerprint, value) for value in values)


def encode_verify_request(value: SqlClientRestrictedWriterVerifyRequest) -> bytes:
    try:
        if type(value) is not SqlClientRestrictedWriterVerifyRequest:
            raise ValueError
        value.__post_init__()
        body = asdict(value)
        body["operation_id"] = str(value.operation_id)
        body["stage"] = strict_json_object(encode_stage_identity(value.stage))
        payload = canonical_json_bytes(body)
        return payload if 0 < len(payload) <= REQUEST_LIMIT else (_ for _ in ()).throw(ValueError())
    except (ValueError, TypeError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None


def decode_verify_request(payload: bytes) -> SqlClientRestrictedWriterVerifyRequest:
    try:
        body = record_shape(SqlClientRestrictedWriterVerifyRequest, _bounded(payload, REQUEST_LIMIT))
        body["parent"] = construct_record(TdsAttemptIdentity, body["parent"])
        body["stage"] = decode_stage_identity(canonical_json_bytes(body["stage"]))
        body["writer"] = construct_record(SqlClientPrincipalResolution, body["writer"])
        body["writer_login"] = construct_record(SqlClientLoginAuthority, body["writer_login"])
        body["operation_id"] = canonical_uuid(body["operation_id"])
        for name in ("login_token", "user_token"):
            if type(body[name]) is not list:
                raise ValueError(ERROR)
            body[name] = tuple(construct_record(SqlClientTokenRow, value) for value in body[name])
        for name in ("server_permissions", "database_permissions"):
            body[name] = _permissions(body[name])
        result = SqlClientRestrictedWriterVerifyRequest(**body)
        if encode_verify_request(result) != payload:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None


def _session_body(value: RestrictedWriterSessionContext) -> dict[str, Any]:
    body = asdict(value)
    body["session"] = strict_json_object(encode_restricted_session_identity(value.session))
    return body


def _session(value: Any) -> RestrictedWriterSessionContext:
    body = record_shape(RestrictedWriterSessionContext, value)
    body["session"] = decode_restricted_session_identity(canonical_json_bytes(body["session"]))
    return RestrictedWriterSessionContext(**body)


def encode_verify_opening(value: RestrictedWriterVerifyOpening) -> bytes:
    """Encode the bounded writer-session authority emitted before the probe."""
    try:
        if type(value) is not RestrictedWriterVerifyOpening:
            raise ValueError
        value.__post_init__()
        payload = canonical_json_bytes(
            {
                "schema": value.schema,
                "request_sha256": value.request_sha256,
                "context": _session_body(cast(RestrictedWriterSessionContext, value.context)),
            }
        )
        if not 0 < len(payload) <= OPENING_LIMIT:
            raise ValueError
        return payload
    except (ValueError, TypeError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None


def decode_verify_opening(payload: bytes) -> RestrictedWriterVerifyOpening:
    """Decode one exact canonical writer-session authority frame."""
    try:
        body = record_shape(RestrictedWriterVerifyOpening, _bounded(payload, OPENING_LIMIT))
        body["context"] = _session(body["context"])
        result = RestrictedWriterVerifyOpening(**body)
        if encode_verify_opening(result) != payload:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None


def encode_probe_authorization(opening_payload: bytes) -> bytes:
    """Authorize only the exact canonical opening frame supplied by the child."""
    decode_verify_opening(opening_payload)
    value = RestrictedWriterProbeAuthorization(sha256(opening_payload).hexdigest())
    payload = canonical_json_bytes(asdict(value))
    if not 0 < len(payload) <= AUTHORIZATION_LIMIT:
        raise ValueError(ERROR)
    return payload


def decode_probe_authorization(payload: bytes, *, opening_payload: bytes) -> RestrictedWriterProbeAuthorization:
    """Require authorization for the exact opening frame retained by the child."""
    try:
        body = record_shape(RestrictedWriterProbeAuthorization, _bounded(payload, AUTHORIZATION_LIMIT))
        result = RestrictedWriterProbeAuthorization(**body)
        if result.opening_sha256 != sha256(opening_payload).hexdigest() or canonical_json_bytes(body) != payload:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None


def encode_verify_result(value: SqlClientRestrictedWriterVerifyResult) -> bytes:
    try:
        if type(value) is not SqlClientRestrictedWriterVerifyResult:
            raise ValueError
        value.__post_init__()
        body = asdict(value)
        body["opening"] = _session_body(value.opening)
        body["closing"] = _session_body(value.closing)
        payload = canonical_json_bytes(body)
        return payload if 0 < len(payload) <= RESULT_LIMIT else (_ for _ in ()).throw(ValueError())
    except (ValueError, TypeError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None


def decode_verify_result(payload: bytes) -> SqlClientRestrictedWriterVerifyResult:
    try:
        body = record_shape(SqlClientRestrictedWriterVerifyResult, _bounded(payload, RESULT_LIMIT))
        for name in ("opening", "closing"):
            body[name] = _session(body[name])
        for name in ("login_token", "user_token"):
            if type(body[name]) is not list:
                raise ValueError
            body[name] = tuple(construct_record(SqlClientTokenFingerprint, value) for value in body[name])
        for name in ("server_permissions", "database_permissions"):
            body[name] = _permission_fingerprints(body[name])
        for name in ("stage_permissions", "closing_stage_permissions"):
            if type(body[name]) is not list:
                raise ValueError
            body[name] = tuple(body[name])
        result = SqlClientRestrictedWriterVerifyResult(**body)
        if encode_verify_result(result) != payload:
            raise ValueError
        return result
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, UnicodeError, RecursionError):
        raise ValueError(ERROR) from None
