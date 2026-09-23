"""Closed canonical helper IPC; exact request hash and supplied-original binding."""

from dataclasses import asdict
from hashlib import sha256

from dpone.contracts.mssql_sqlclient_create_departure import SqlClientCreateDeparture
from dpone.contracts.mssql_sqlclient_create_departure_codec import decode_create_departure, encode_create_departure
from dpone.contracts.mssql_sqlclient_departure_ipc import (
    ERROR,
    SqlClientDeparturePlan,
    SqlClientDepartureRequest,
    SqlClientDepartureResult,
    _typed,
)
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity
from dpone.contracts.mssql_tds_coordinator_ipc import decode_startup, encode_startup
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsAttemptOwnership, TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, construct_record, record_shape, string_enum

_PLAN_LIMIT = 65536
_RESULT_LIMIT = 32768
_FAILURES = (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError)


def _bounded(payload: bytes, limit: int) -> None:
    if type(payload) is not bytes or not 0 < len(payload) <= limit:
        raise ValueError(ERROR)


def encode_departure_plan(value: SqlClientDeparturePlan) -> bytes:
    """Validate original types before UUID/enum/session serialization."""
    try:
        _typed(value, SqlClientDeparturePlan)
        observed = strict_json_object(
            encode_create_departure(
                SqlClientCreateDeparture(
                    original=value.original,
                    database=value.database,
                    admission=value.creator_admission,
                    principal=value.principal,
                    counts=(0,) * 6,
                )
            )
        )
        data = asdict(value)
        data["helper_id"] = str(value.helper_id)
        data["create_operation"]["operation_id"] = str(value.create_operation.operation_id)
        data["original"] = observed["original"]
        data["database"] = observed["database"]
        payload = canonical_json_bytes(data)
        _bounded(payload, _PLAN_LIMIT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_departure_plan(payload: bytes) -> SqlClientDeparturePlan:
    """Reuse reviewed creator observation decoding for its four nested fact groups."""
    try:
        _bounded(payload, _PLAN_LIMIT)
        data = record_shape(SqlClientDeparturePlan, strict_json_object(payload))
        observed = decode_create_departure(
            canonical_json_bytes(
                dict(
                    schema="dpone.sqlclient.create-departure.v1",
                    original=data["original"],
                    database=data["database"],
                    admission=data["creator_admission"],
                    principal=data["principal"],
                    counts=[0] * 6,
                )
            )
        )
        data["helper_id"] = canonical_uuid(data["helper_id"])
        data["attempt"] = construct_record(TdsAttemptIdentity, data["attempt"])
        data["ownership"] = construct_record(TdsAttemptOwnership, data["ownership"])
        data["create_process"] = construct_record(TdsProcessIdentity, data["create_process"])
        operation = record_shape(TdsCoordinatorIdentity, data["create_operation"])
        operation["parent"] = construct_record(TdsAttemptIdentity, operation["parent"])
        operation["operation_id"] = canonical_uuid(operation["operation_id"])
        operation["command"] = string_enum(TdsCoordinatorCommand, operation["command"])
        data["create_operation"] = TdsCoordinatorIdentity(**operation)
        data.update(
            original=observed.original,
            database=observed.database,
            creator_admission=observed.admission,
            principal=observed.principal,
        )
        value = SqlClientDeparturePlan(**data)
        if encode_departure_plan(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None


def encode_departure_request(value: SqlClientDepartureRequest) -> bytes:
    """Canonical nonsecret bytes only; private delivery is a separate envelope."""
    try:
        _typed(value, SqlClientDepartureRequest)
        payload = canonical_json_bytes(
            dict(
                schema=value.schema,
                plan=strict_json_object(encode_departure_plan(value.plan)),
                startup=strict_json_object(encode_startup(value.startup)),
            )
        )
        _bounded(payload, _PLAN_LIMIT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_departure_request(payload: bytes) -> SqlClientDepartureRequest:
    """Require closed canonical request including existing startup representation."""
    try:
        _bounded(payload, _PLAN_LIMIT)
        data = record_shape(SqlClientDepartureRequest, strict_json_object(payload))
        data["plan"] = decode_departure_plan(canonical_json_bytes(data["plan"]))
        data["startup"] = decode_startup(canonical_json_bytes(data["startup"]))
        value = SqlClientDepartureRequest(**data)
        if encode_departure_request(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None


def departure_request_digest(request: SqlClientDepartureRequest) -> str:
    """Exact canonical request byte SHA256, without a new semantic hash domain."""
    return sha256(encode_departure_request(request)).hexdigest()


def make_departure_result(
    request: SqlClientDepartureRequest, departure: SqlClientCreateDeparture
) -> SqlClientDepartureResult:
    """Bind original creator facts and preserve the validated sequential counts."""
    try:
        digest = departure_request_digest(request)
        _typed(departure, SqlClientCreateDeparture)
        plan = request.plan
        if (departure.original, departure.database, departure.admission, departure.principal) != (
            plan.original,
            plan.database,
            plan.creator_admission,
            plan.principal,
        ):
            raise ValueError
        return SqlClientDepartureResult(request_sha256=digest, departure=departure)
    except _FAILURES:
        raise ValueError(ERROR) from None


def encode_departure_result(value: SqlClientDepartureResult, *, request: SqlClientDepartureRequest) -> bytes:
    """Reject self-selected request hashes and any changed original departure fact."""
    try:
        _typed(value, SqlClientDepartureResult)
        if value != make_departure_result(request, value.departure):
            raise ValueError
        payload = canonical_json_bytes(
            dict(
                schema=value.schema,
                request_sha256=value.request_sha256,
                departure=strict_json_object(encode_create_departure(value.departure)),
            )
        )
        _bounded(payload, _RESULT_LIMIT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_departure_result(payload: bytes, *, request: SqlClientDepartureRequest) -> SqlClientDepartureResult:
    """Original request is mandatory; decoding cannot infer helper exit or ACKs."""
    try:
        _bounded(payload, _RESULT_LIMIT)
        data = record_shape(SqlClientDepartureResult, strict_json_object(payload))
        data["departure"] = decode_create_departure(canonical_json_bytes(data["departure"]))
        value = SqlClientDepartureResult(**data)
        if encode_departure_result(value, request=request) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None
