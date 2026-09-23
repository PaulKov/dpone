"""Closed bounded observer-incarnation v2 wire codec, without trusted flags."""

import re
from datetime import datetime
from typing import Any

from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientPrincipalResolution,
    SqlClientServerAuthority,
    SqlClientSessionAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation import (
    _ERROR,
    _FAILURES,
    _LIMIT,
    _SCHEMA,
    SqlClientDepartureVisibilityV2,
    SqlClientObserverIncarnation,
    _observer_bytes,
)
from dpone.contracts.strict_json import strict_json_object
from dpone.contracts.strict_record import canonical_uuid, construct_record, record_shape


def _decode_timestamp(value: Any) -> datetime:
    """Admit the exact naive microsecond spelling before datetime construction."""
    if (
        type(value) is not str
        or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}", value) is None
    ):
        raise ValueError(_ERROR)
    return datetime.fromisoformat(value)


def encode_observer_incarnation(value: SqlClientObserverIncarnation) -> bytes:
    """Use the same validated canonical projection as the model's guard digest."""
    return _observer_bytes(value)


def decode_observer_incarnation(payload: bytes) -> SqlClientObserverIncarnation:
    """Bound exact bytes before strict parsing; reject every alternate encoding."""
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= _LIMIT:
            raise ValueError
        data = strict_json_object(payload)
        if data.pop("schema", None) != _SCHEMA:
            raise ValueError
        data = record_shape(SqlClientObserverIncarnation, data)
        data["connection_id"] = canonical_uuid(data["connection_id"])
        for key in ("connect_time", "login_time"):
            data[key] = _decode_timestamp(data[key])
        authority = record_shape(SqlClientSessionAuthority, data["authority"])
        for name, cls in (
            ("server", SqlClientServerAuthority),
            ("database", SqlClientDatabaseAuthority),
            ("login", SqlClientLoginAuthority),
            ("transport", SqlClientTransportAuthority),
            ("principal_resolution", SqlClientPrincipalResolution),
        ):
            authority[name] = construct_record(cls, authority[name])
        data["authority"] = SqlClientSessionAuthority(**authority)
        data["visibility"] = construct_record(SqlClientDepartureVisibilityV2, data["visibility"])
        value = SqlClientObserverIncarnation(**data)
        if encode_observer_incarnation(value) != payload:
            raise ValueError
        return value
    except _FAILURES:
        raise ValueError(_ERROR) from None
