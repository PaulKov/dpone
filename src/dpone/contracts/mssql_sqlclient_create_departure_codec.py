"""Bounded canonical departure observation IPC, without parent-proof authority.

This codec preserves supplied creator facts. It neither authenticates the sender
nor observes helper exit, CREATE success, or exclusion of another SQL writer.
The parent must bind and authenticate its separate process/control envelope.
"""

from dataclasses import asdict
from uuid import UUID

from dpone.contracts.mssql_sqlclient_create_departure import SqlClientCreateDeparture
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
)
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation
from dpone.contracts.mssql_tds_session import decode_session_identity, encode_session_identity
from dpone.contracts.mssql_tds_validation import _uuid
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.contracts.strict_record import construct_record, record_shape

_SCHEMA = "dpone.sqlclient.create-departure.v1"
_LIMIT = 16384
_ERROR = "mssql_native.sqlclient_create_departure_record_invalid"


def encode_create_departure(value: SqlClientCreateDeparture) -> bytes:
    """Revalidate typed originals before representation can normalize any aliases."""
    try:
        if type(value) is not SqlClientCreateDeparture:
            raise ValueError
        value.__post_init__()
        database = asdict(value.database)
        database["database_guid"] = str(value.database.database_guid)
        payload = canonical_json_bytes(
            {
                "schema": _SCHEMA,
                "original": strict_json_object(encode_session_identity(value.original)),
                "database": database,
                "admission": asdict(value.admission),
                "principal": asdict(value.principal),
                "counts": list(value.counts),
            }
        )
        if not 0 < len(payload) <= _LIMIT:
            raise ValueError
        return payload
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError):
        raise ValueError(_ERROR) from None


def decode_create_departure(payload: bytes) -> SqlClientCreateDeparture:
    """Bound before parsing, reconstruct closed nested types and require canonical bytes."""
    try:
        if type(payload) is not bytes or not 0 < len(payload) <= _LIMIT:
            raise ValueError
        data = strict_json_object(payload)
        if data.pop("schema", None) != _SCHEMA:
            raise ValueError
        data = record_shape(SqlClientCreateDeparture, data)
        data["original"] = decode_session_identity(canonical_json_bytes(data["original"]))
        database = record_shape(TdsDatabaseObservation, data["database"])
        _uuid(database["database_guid"])
        database["database_guid"] = UUID(database["database_guid"])
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
        if type(data["counts"]) is not list:
            raise ValueError
        data["counts"] = tuple(data["counts"])
        value = SqlClientCreateDeparture(**data)
        if encode_create_departure(value) != payload:
            raise ValueError
        return value
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError):
        raise ValueError(_ERROR) from None
