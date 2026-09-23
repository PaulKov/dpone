"""Canonical 16 KiB v2 departure codec; v1 bytes and semantics stay independent."""

from dataclasses import asdict

from dpone.contracts.mssql_sqlclient_create_departure_v2 import (
    _ERROR,
    SqlClientCreateDepartureV2,
    SqlClientDepartureSampleKind,
    SqlClientDepartureSampleV2,
    SqlClientObserverRequestV2,
)
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation import _FAILURES, _record
from dpone.contracts.mssql_sqlclient_observer_incarnation_codec import (
    _decode_timestamp,
    decode_observer_incarnation,
    encode_observer_incarnation,
)
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation
from dpone.contracts.mssql_tds_session import decode_session_identity, encode_session_identity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import canonical_uuid, construct_record, record_shape, string_enum

_LIMIT = 16384
_SCHEMA = "dpone.sqlclient.create-departure.v2"


def _departure_body(value: SqlClientCreateDepartureV2) -> dict:
    """Validated census projection shared by explicit operation schemas."""
    _record(value, SqlClientCreateDepartureV2)
    database = asdict(value.database)
    database["database_guid"] = str(value.database.database_guid)
    samples = []
    for sample in value.samples:
        item = asdict(sample)
        item["kind"] = sample.kind.value
        if sample.request is not None:
            item["request"]["connection_id"] = str(sample.request.connection_id)
            item["request"]["start_time"] = sample.request.start_time.isoformat(timespec="microseconds")
        samples.append(item)
    return dict(
        original=strict_json_object(encode_session_identity(value.original)),
        database=database,
        admission=asdict(value.admission),
        principal=asdict(value.principal),
        observer=strict_json_object(encode_observer_incarnation(value.observer)),
        samples=samples,
    )


def _departure_from_body(data: dict) -> SqlClientCreateDepartureV2:
    """Decode the common closed census body, without choosing an outer schema."""
    data = record_shape(SqlClientCreateDepartureV2, data)
    data["original"] = decode_session_identity(canonical_json_bytes(data["original"]))
    database = record_shape(TdsDatabaseObservation, data["database"])
    database["database_guid"] = canonical_uuid(database["database_guid"])
    data["database"] = TdsDatabaseObservation(**database)
    admission = record_shape(SqlClientObserverAdmission, data["admission"])
    for name, cls in (
        ("server", SqlClientServerAuthority),
        ("database", SqlClientDatabaseAuthority),
        ("login", SqlClientLoginAuthority),
        ("transport", SqlClientTransportAuthority),
    ):
        admission[name] = construct_record(cls, admission[name])
    data["admission"] = SqlClientObserverAdmission(**admission)
    data["principal"] = construct_record(SqlClientDatabasePrincipal, data["principal"])
    data["observer"] = decode_observer_incarnation(canonical_json_bytes(data["observer"]))
    if type(data["samples"]) is not list or len(data["samples"]) != 6:
        raise ValueError
    samples = []
    for value in data["samples"]:
        item = record_shape(SqlClientDepartureSampleV2, value)
        item["kind"] = string_enum(SqlClientDepartureSampleKind, item["kind"])
        if item["request"] is not None:
            request = record_shape(SqlClientObserverRequestV2, item["request"])
            request["connection_id"] = canonical_uuid(request["connection_id"])
            request["start_time"] = _decode_timestamp(request["start_time"])
            item["request"] = SqlClientObserverRequestV2(**request)
        samples.append(SqlClientDepartureSampleV2(**item))
    data["samples"] = tuple(samples)
    value = SqlClientCreateDepartureV2(**data)
    return value


def encode_create_departure_v2(value: SqlClientCreateDepartureV2) -> bytes:
    """Preserve the exact existing CREATE v2 wrapper and 16 KiB budget."""
    try:
        payload = canonical_json_bytes(dict(schema=_SCHEMA, **_departure_body(value)))
        if not 0 < len(payload) <= _LIMIT:
            raise ValueError
        return payload
    except _FAILURES:
        raise ValueError(_ERROR) from None


def decode_create_departure_v2(payload: bytes) -> SqlClientCreateDepartureV2:
    """Require the unchanged closed canonical CREATE v2 wrapper."""
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= _LIMIT:
            raise ValueError
        data = strict_json_object(payload)
        if data.pop("schema", None) != _SCHEMA:
            raise ValueError
        value = _departure_from_body(data)
        if encode_create_departure_v2(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(_ERROR) from None
