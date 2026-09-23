"""Canonical bounded codecs for public P10f helper frames and private credentials."""

from dataclasses import asdict
from hashlib import sha256
from typing import Any

from dpone.contracts.mssql_sqlclient_input import decode_input_descriptor, encode_input_descriptor
from dpone.contracts.mssql_sqlclient_stage_identity import decode_stage_identity, encode_stage_identity
from dpone.contracts.mssql_sqlclient_writer_evidence import (
    decode_writer_observation,
    encode_writer_observation,
)
from dpone.contracts.mssql_sqlclient_writer_session_departure_codec import (
    _admission,
    decode_writer_session_departure,
    encode_writer_session_departure,
)
from dpone.contracts.mssql_sqlclient_writer_settlement import (
    SqlClientStageContentExpectation,
    SqlClientWriterSettlementObservation,
)
from dpone.contracts.mssql_sqlclient_writer_settlement_ipc import (
    ERROR,
    MAX_FRAME_BYTES,
    SqlClientWriterSettlementCredentials,
    SqlClientWriterSettlementPlan,
    SqlClientWriterSettlementRequest,
    SqlClientWriterSettlementResult,
    _admission_from_observation,
    _exact,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_coordinator_ipc import decode_startup, encode_startup
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, construct_record, record_shape

_FAILURES = (ValueError, TypeError, KeyError, OverflowError, RecursionError, UnicodeError, AttributeError)


def _bounded(payload: bytes) -> dict[str, Any]:
    if type(payload) is not bytes or not 0 < len(payload) <= MAX_FRAME_BYTES:
        raise ValueError(ERROR)
    body = strict_json_object(payload)
    if canonical_json_bytes(body) != payload:
        raise ValueError(ERROR)
    return body


def _observation_body(value: SqlClientWriterSettlementObservation) -> dict[str, Any]:
    _exact(value, SqlClientWriterSettlementObservation)
    from dpone.contracts.mssql_sqlclient_observer_incarnation_codec import encode_observer_incarnation

    body = {
        "departure": strict_json_object(encode_writer_session_departure(value.departure)),
        "stage_before": strict_json_object(encode_stage_identity(value.stage_before)),
        "stage_after": strict_json_object(encode_stage_identity(value.stage_after)),
        "observer_before": strict_json_object(encode_observer_incarnation(value.observer_before)),
        "observer_after": strict_json_object(encode_observer_incarnation(value.observer_after)),
        "row_count": value.row_count,
        "typed_digest": value.typed_digest,
        "digest_profile": value.digest_profile,
    }
    if value.typed_sum is not None:
        body["typed_sum"] = value.typed_sum
    return body


def _observation(value: Any) -> SqlClientWriterSettlementObservation:
    from dpone.contracts.mssql_sqlclient_observer_incarnation_codec import decode_observer_incarnation

    if isinstance(value, dict) and "typed_sum" not in value:
        value = {**value, "typed_sum": None}
    body = record_shape(SqlClientWriterSettlementObservation, value)
    body["departure"] = decode_writer_session_departure(canonical_json_bytes(body["departure"]))
    for name in ("stage_before", "stage_after"):
        body[name] = decode_stage_identity(canonical_json_bytes(body[name]))
    for name in ("observer_before", "observer_after"):
        body[name] = decode_observer_incarnation(canonical_json_bytes(body[name]))
    return SqlClientWriterSettlementObservation(**body)


def encode_writer_settlement_plan(value: SqlClientWriterSettlementPlan) -> bytes:
    """Encode only nonsecret immutable helper intent."""
    try:
        _exact(value, SqlClientWriterSettlementPlan)
        payload = canonical_json_bytes(
            {
                "helper_id": str(value.helper_id),
                "attempt": asdict(value.attempt),
                "writer_observation": strict_json_object(encode_writer_observation(value.writer_observation)),
                "writer_admission": asdict(value.writer_admission),
                "management_admission": asdict(value.management_admission),
                "stage": strict_json_object(encode_stage_identity(value.stage)),
                "input_descriptor": strict_json_object(encode_input_descriptor(value.input_descriptor)),
                "expectation": asdict(value.expectation),
                "implementation_sha256": value.implementation_sha256,
                "package_root": value.package_root,
                "admission_sha256": value.admission_sha256,
                "startup_deadline": value.startup_deadline,
                "operation_deadline": value.operation_deadline,
                "max_address_space_bytes": value.max_address_space_bytes,
                "schema": value.schema,
            }
        )
        if not 0 < len(payload) <= MAX_FRAME_BYTES:
            raise ValueError
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_writer_settlement_plan(payload: bytes) -> SqlClientWriterSettlementPlan:
    try:
        body = record_shape(SqlClientWriterSettlementPlan, _bounded(payload))
        body["helper_id"] = canonical_uuid(body["helper_id"])
        body["attempt"] = construct_record(TdsAttemptIdentity, body["attempt"])
        body["writer_observation"] = decode_writer_observation(canonical_json_bytes(body["writer_observation"]))
        body["writer_admission"] = _admission(body["writer_admission"])
        body["management_admission"] = _admission(body["management_admission"])
        body["stage"] = decode_stage_identity(canonical_json_bytes(body["stage"]))
        body["input_descriptor"] = decode_input_descriptor(canonical_json_bytes(body["input_descriptor"]))
        body["expectation"] = construct_record(SqlClientStageContentExpectation, body["expectation"])
        result = SqlClientWriterSettlementPlan(**body)
        if encode_writer_settlement_plan(result) != payload:
            raise ValueError
        return result
    except _FAILURES:
        raise ValueError(ERROR) from None


def encode_writer_settlement_request(value: SqlClientWriterSettlementRequest) -> bytes:
    try:
        _exact(value, SqlClientWriterSettlementRequest)
        payload = canonical_json_bytes(
            {
                "plan": strict_json_object(encode_writer_settlement_plan(value.plan)),
                "startup": strict_json_object(encode_startup(value.startup)),
                "schema": value.schema,
            }
        )
        if not 0 < len(payload) <= MAX_FRAME_BYTES:
            raise ValueError
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_writer_settlement_request(payload: bytes) -> SqlClientWriterSettlementRequest:
    try:
        body = record_shape(SqlClientWriterSettlementRequest, _bounded(payload))
        body["plan"] = decode_writer_settlement_plan(canonical_json_bytes(body["plan"]))
        body["startup"] = decode_startup(canonical_json_bytes(body["startup"]))
        result = SqlClientWriterSettlementRequest(**body)
        if encode_writer_settlement_request(result) != payload:
            raise ValueError
        return result
    except _FAILURES:
        raise ValueError(ERROR) from None


def writer_settlement_request_digest(value: SqlClientWriterSettlementRequest) -> str:
    return sha256(encode_writer_settlement_request(value)).hexdigest()


def _require_request_binding(
    helper_id: object, request_sha256: object, request: SqlClientWriterSettlementRequest
) -> None:
    _exact(request, SqlClientWriterSettlementRequest)
    if helper_id != request.plan.helper_id or request_sha256 != writer_settlement_request_digest(request):
        raise ValueError(ERROR)


def encode_writer_settlement_credentials(value: SqlClientWriterSettlementCredentials) -> bytes:
    """Encode the sole private frame; callers must never persist these bytes."""
    try:
        _exact(value, SqlClientWriterSettlementCredentials)
        _require_request_binding(value.request.plan.helper_id, value.request_sha256, value.request)
        expected = value.request.plan.management_admission
        if value.material.database != expected.database.database_name or value.material.username != expected.login.name:
            raise ValueError
        payload = canonical_json_bytes(
            {
                "request": strict_json_object(encode_writer_settlement_request(value.request)),
                "request_sha256": value.request_sha256,
                "material": asdict(value.material),
                "schema": value.schema,
            }
        )
        if not 0 < len(payload) <= MAX_FRAME_BYTES:
            raise ValueError
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_writer_settlement_credentials(
    payload: bytes, *, request: SqlClientWriterSettlementRequest
) -> SqlClientWriterSettlementCredentials:
    try:
        body = record_shape(SqlClientWriterSettlementCredentials, _bounded(payload))
        body["request"] = decode_writer_settlement_request(canonical_json_bytes(body["request"]))
        body["material"] = construct_record(TdsConnectionMaterial, body["material"])
        result = SqlClientWriterSettlementCredentials(**body)
        if result.request != request:
            raise ValueError
        _require_request_binding(result.request.plan.helper_id, result.request_sha256, request)
        expected = request.plan.management_admission
        if (
            result.material.database != expected.database.database_name
            or result.material.username != expected.login.name
        ):
            raise ValueError
        if encode_writer_settlement_credentials(result) != payload:
            raise ValueError
        return result
    except _FAILURES:
        raise ValueError(ERROR) from None


def make_writer_settlement_result(
    request: SqlClientWriterSettlementRequest, observation: SqlClientWriterSettlementObservation
) -> SqlClientWriterSettlementResult:
    try:
        _exact(request, SqlClientWriterSettlementRequest)
        _exact(observation, SqlClientWriterSettlementObservation)
        plan = request.plan
        actual_management = _admission_from_observation(plan.writer_observation)
        observer = observation.departure.observer.authority
        actual_observer = type(plan.management_admission)(
            observer.server, observer.database, observer.login, observer.transport
        )
        if (
            observation.departure.original != plan.writer_observation.remote_session
            or observation.departure.writer_authority != plan.writer_observation.authority
            or observation.departure.writer_admission != plan.writer_admission
            or actual_management != plan.writer_admission
            or actual_observer != plan.management_admission
            or observation.stage_before != plan.stage
            or observation.stage_after != plan.stage
            or observation.row_count != plan.expectation.rows
            or observation.typed_digest != plan.expectation.typed_digest
            or observation.digest_profile != plan.expectation.digest_profile
        ):
            raise ValueError
        return SqlClientWriterSettlementResult(
            helper_id=plan.helper_id,
            request_sha256=writer_settlement_request_digest(request),
            observation=observation,
        )
    except _FAILURES:
        raise ValueError(ERROR) from None


def encode_writer_settlement_result(
    value: SqlClientWriterSettlementResult, *, request: SqlClientWriterSettlementRequest
) -> bytes:
    try:
        _exact(value, SqlClientWriterSettlementResult)
        if value != make_writer_settlement_result(request, value.observation):
            raise ValueError
        payload = canonical_json_bytes(
            {
                "helper_id": str(value.helper_id),
                "request_sha256": value.request_sha256,
                "observation": _observation_body(value.observation),
                "schema": value.schema,
            }
        )
        if not 0 < len(payload) <= MAX_FRAME_BYTES:
            raise ValueError
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_writer_settlement_result(
    payload: bytes, *, request: SqlClientWriterSettlementRequest
) -> SqlClientWriterSettlementResult:
    try:
        body = record_shape(SqlClientWriterSettlementResult, _bounded(payload))
        body["helper_id"] = canonical_uuid(body["helper_id"])
        body["observation"] = _observation(body["observation"])
        result = SqlClientWriterSettlementResult(**body)
        if encode_writer_settlement_result(result, request=request) != payload:
            raise ValueError
        return result
    except _FAILURES:
        raise ValueError(ERROR) from None


__all__ = (
    "decode_writer_settlement_credentials",
    "decode_writer_settlement_plan",
    "decode_writer_settlement_request",
    "decode_writer_settlement_result",
    "encode_writer_settlement_credentials",
    "encode_writer_settlement_plan",
    "encode_writer_settlement_request",
    "encode_writer_settlement_result",
    "make_writer_settlement_result",
    "writer_settlement_request_digest",
)
