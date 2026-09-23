"""Closed v2 IPC, preserving full observations and independently bound intent."""

from dataclasses import asdict, fields
from hashlib import sha256

from dpone.contracts.mssql_sqlclient_create_departure_v2 import SqlClientCreateDepartureV2
from dpone.contracts.mssql_sqlclient_create_departure_v2_codec import (
    decode_create_departure_v2,
    encode_create_departure_v2,
)
from dpone.contracts.mssql_sqlclient_departure_ipc_codec import (
    decode_departure_plan as _decode_common_plan,
)
from dpone.contracts.mssql_sqlclient_departure_ipc_codec import (
    encode_departure_plan as _encode_common_plan,
)
from dpone.contracts.mssql_sqlclient_departure_ipc_v2 import (
    ERROR,
    SqlClientDeparturePlanV2,
    SqlClientDepartureRequestV2,
    SqlClientDepartureResultV2,
    _common_plan,
    _typed,
)
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_tds_coordinator_ipc import decode_startup, encode_startup
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape

_PLAN_LIMIT = 65536
_RESULT_LIMIT = 32768
_FAILURES = (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError)


def _bounded(payload: bytes, limit: int) -> None:
    if type(payload) is not bytes or not 0 < len(payload) <= limit:
        raise ValueError(ERROR)


def encode_departure_plan_v2(value: SqlClientDeparturePlanV2) -> bytes:
    """Validate original v2 fields before privately projecting common plan fields."""
    try:
        _typed(value, SqlClientDeparturePlanV2)
        data = strict_json_object(_encode_common_plan(_common_plan(value)))
        data.update(schema=value.schema, observer_admission=asdict(value.observer_admission))
        payload = canonical_json_bytes(data)
        _bounded(payload, _PLAN_LIMIT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_departure_plan_v2(payload: bytes) -> SqlClientDeparturePlanV2:
    """Require closed v2 intent before decoding the unchanged common fields."""
    try:
        _bounded(payload, _PLAN_LIMIT)
        data = record_shape(SqlClientDeparturePlanV2, strict_json_object(payload))
        if data["schema"] != "dpone.sqlclient.departure-plan.v2":
            raise ValueError
        admission = record_shape(SqlClientObserverAdmission, data.pop("observer_admission"))
        for name, cls in (
            ("server", SqlClientServerAuthority),
            ("database", SqlClientDatabaseAuthority),
            ("login", SqlClientLoginAuthority),
            ("transport", SqlClientTransportAuthority),
        ):
            admission[name] = construct_record(cls, admission[name])
        observer = SqlClientObserverAdmission(**admission)
        data["schema"] = "dpone.sqlclient.departure-plan.v1"
        common = _decode_common_plan(canonical_json_bytes(data))
        value = SqlClientDeparturePlanV2(
            **{f.name: getattr(common, f.name) for f in fields(common) if f.name != "schema"},
            observer_admission=observer,
        )
        if encode_departure_plan_v2(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None


def encode_departure_request_v2(value: SqlClientDepartureRequestV2) -> bytes:
    """Canonical nonsecret bytes only; private delivery is a separate envelope."""
    try:
        _typed(value, SqlClientDepartureRequestV2)
        payload = canonical_json_bytes(
            dict(
                schema=value.schema,
                plan=strict_json_object(encode_departure_plan_v2(value.plan)),
                startup=strict_json_object(encode_startup(value.startup)),
            )
        )
        _bounded(payload, _PLAN_LIMIT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_departure_request_v2(payload: bytes) -> SqlClientDepartureRequestV2:
    """Require closed canonical request including existing startup representation."""
    try:
        _bounded(payload, _PLAN_LIMIT)
        data = record_shape(SqlClientDepartureRequestV2, strict_json_object(payload))
        data["plan"] = decode_departure_plan_v2(canonical_json_bytes(data["plan"]))
        data["startup"] = decode_startup(canonical_json_bytes(data["startup"]))
        value = SqlClientDepartureRequestV2(**data)
        if encode_departure_request_v2(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None


def departure_request_digest_v2(request: SqlClientDepartureRequestV2) -> str:
    """Exact canonical request byte SHA256, without a new semantic hash domain."""
    return sha256(encode_departure_request_v2(request)).hexdigest()


def make_departure_result_v2(
    request: SqlClientDepartureRequestV2, departure: SqlClientCreateDepartureV2
) -> SqlClientDepartureResultV2:
    """Bind all creator facts and actual observer authority to original intent."""
    try:
        _typed(request, SqlClientDepartureRequestV2)
        _typed(departure, SqlClientCreateDepartureV2)
        plan = request.plan
        if (departure.original, departure.database, departure.admission, departure.principal) != (
            plan.original,
            plan.database,
            plan.creator_admission,
            plan.principal,
        ):
            raise ValueError
        actual = departure.observer.authority
        expected = plan.observer_admission
        if (actual.server, actual.database, actual.login, actual.transport) != (
            expected.server,
            expected.database,
            expected.login,
            expected.transport,
        ):
            raise ValueError
        return SqlClientDepartureResultV2(request_sha256=departure_request_digest_v2(request), departure=departure)
    except _FAILURES:
        raise ValueError(ERROR) from None


def encode_departure_result_v2(value: SqlClientDepartureResultV2, *, request: SqlClientDepartureRequestV2) -> bytes:
    """Reject self-selected request hashes and any changed original departure fact."""
    try:
        _typed(value, SqlClientDepartureResultV2)
        if value != make_departure_result_v2(request, value.departure):
            raise ValueError
        payload = canonical_json_bytes(
            dict(
                schema=value.schema,
                request_sha256=value.request_sha256,
                departure=strict_json_object(encode_create_departure_v2(value.departure)),
            )
        )
        _bounded(payload, _RESULT_LIMIT)
        return payload
    except _FAILURES:
        raise ValueError(ERROR) from None


def decode_departure_result_v2(payload: bytes, *, request: SqlClientDepartureRequestV2) -> SqlClientDepartureResultV2:
    """Original request is mandatory; decoding cannot infer helper exit or ACKs."""
    try:
        _bounded(payload, _RESULT_LIMIT)
        data = record_shape(SqlClientDepartureResultV2, strict_json_object(payload))
        data["departure"] = decode_create_departure_v2(canonical_json_bytes(data["departure"]))
        value = SqlClientDepartureResultV2(**data)
        if encode_departure_result_v2(value, request=request) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(ERROR) from None
