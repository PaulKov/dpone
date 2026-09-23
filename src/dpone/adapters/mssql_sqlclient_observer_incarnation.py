"""Decode one bounded actual-own row without inventing sleeping writer facts.

All source cardinalities and raw scalar types precede normalization. Expected
admission is independently supplied; no expected field becomes observed evidence.
The strict-zero xact_state is sampled by the fresh batch-entry declaration;
other transaction facts are sampled during metadata reading. Neither establishes
an atomic snapshot or excludes statement-internal autocommit work.
"""

from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import UUID

from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import OWN_INCARNATION_SQL
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientSessionAuthority,
    SqlClientTransportAuthority,
    resolve_database_principal,
    validate_catalog_admission,
)
from dpone.contracts.mssql_tds_api import (
    SqlClientDepartureVisibilityV2,
    SqlClientObserverIncarnation,
    _context_records,
    _integer,
)

_ERROR = "mssql_native.sqlclient_observer_incarnation_invalid"


def _raw_row(rows: list[Any]) -> tuple[Any, ...]:
    if len(rows) != 1 or len(rows[0]) != 68:
        raise ValueError(_ERROR)
    row = tuple(rows[0])
    for index in (19, 28, 39, 43, 45, 62):
        _integer(row[index], 1, 1)
    _integer(row[51], 1, 2)
    for index in (66, 67):
        _integer(row[index], 0, 0)
    # Count gates precede even inspection of MAX-projected payloads.
    if row[51] == 1 and any(value is not None for value in row[57:62]):
        raise ValueError(_ERROR)
    integers = {0, 1, 6, 8, 9, 10, 11, 12, 21, 29, 31, 32, 33, 38, 40, 46, 50, 52, 63}
    if row[51] == 2:
        integers.add(57)
    for index, value in enumerate(row):
        if index in (19, 28, 39, 43, 45, 51, 62, 66, 67):
            continue
        if index == 23 or (row[51] == 1 and 57 <= index <= 61):
            if value is not None:
                raise ValueError(_ERROR)
            continue
        if index in (13, 14):
            if value is not None:
                _integer(value, 0, 1)
            continue
        expected = (
            int
            if index in integers
            else UUID
            if index in (20, 44)
            else datetime
            if index in (22, 30)
            else bytes
            if index in (3, 5)
            else str
        )
        if type(value) is not expected:
            raise ValueError(_ERROR)
    return row


def _sid(value: str) -> bytes:
    # SQL style-2 emits uppercase hex; reject whitespace and malformed strings
    # before bytes.fromhex can normalize them. Final contracts own lowercase wire.
    if not 2 <= len(value) <= 170 or len(value) % 2 or any(c not in "0123456789ABCDEF" for c in value):
        raise ValueError(_ERROR)
    return bytes.fromhex(value)


def capture_observer_incarnation(
    query: Callable[..., list[Any]], *, admission: SqlClientObserverAdmission
) -> SqlClientObserverIncarnation:
    """Acquire fresh actual context using the inherited bounded query lifecycle."""
    if type(admission) is not SqlClientObserverAdmission:
        raise ValueError(_ERROR)
    _context_records(admission)
    return parse_observer_incarnation_rows(query(OWN_INCARNATION_SQL), admission=admission)


def require_own_incarnation_statement(statement: str) -> None:
    """Validate the legacy adapter argument without exposing SQL through a port."""
    if type(statement) is not str or statement != OWN_INCARNATION_SQL:
        raise ValueError(_ERROR)


def parse_observer_incarnation_rows(
    rows: list[Any], *, admission: SqlClientObserverAdmission
) -> SqlClientObserverIncarnation:
    """Decode the same strict 68-column contract from direct or finite reads."""
    if type(admission) is not SqlClientObserverAdmission:
        raise ValueError(_ERROR)
    _context_records(admission)
    r = _raw_row(rows)
    for index in (8, 9, 10, 32):
        _integer(r[index], 0, 0)
    _integer(r[31], 1, 1)
    _integer(r[50], 0, 1)
    for index in (3, 5):
        if not 1 <= len(r[index]) <= 85:
            raise ValueError(_ERROR)
    if (
        r[0] != r[21]
        or r[0] != r[29]
        or r[1] != r[33]
        or r[1] != r[40]
        or r[2] != r[4]
        or r[2] != r[34]
        or r[2] != r[36]
        or r[2] != r[47]
        or r[3] != r[5]
        or r[3] != _sid(r[35])
        or r[3] != _sid(r[37])
        or r[3] != _sid(r[48])
        or r[49] != "SQL_LOGIN"
    ):
        raise ValueError(_ERROR)
    catalog = (*r[15:19], r[40], r[41], r[44], _sid(r[42]), r[46], r[47], _sid(r[48]), r[50], r[49])
    validate_catalog_admission([catalog], admission)
    actual = SqlClientObserverAdmission(
        SqlClientServerAuthority(*r[15:19]),
        SqlClientDatabaseAuthority(r[40], r[41], str(r[44]), _sid(r[42]).hex()),
        SqlClientLoginAuthority(r[46], r[47], _sid(r[48]).hex(), r[4], r[5].hex(), r[38], bool(r[50])),
        SqlClientTransportAuthority(*r[24:28]),
    )
    if actual != admission:
        raise ValueError(_ERROR)
    principals = [(r[i], r[i + 1], _sid(r[i + 2]), r[i + 3], r[i + 4]) for i in (52, 57)[: r[51]]]
    resolution = resolve_database_principal(principals, admission=actual)
    if (r[6], r[7]) != (r[63], r[64]) or (r[63], r[64], _sid(r[65]).hex()) != (
        resolution.principal_id,
        resolution.name,
        resolution.sid,
    ):
        raise ValueError(_ERROR)
    return SqlClientObserverIncarnation(
        connection_id=r[20],
        session_id=r[0],
        connect_time=r[22],
        login_time=r[30],
        parent_connection_id=r[23],
        mars_child_count=r[66],
        transaction_count=r[8],
        xact_state=r[9],
        authority=SqlClientSessionAuthority(actual.server, actual.database, actual.login, actual.transport, resolution),
        visibility=SqlClientDepartureVisibilityV2(
            server_major_version=r[11],
            engine_edition=r[12],
            view_server_state=r[13],
            view_server_performance_state=r[14],
            database_id=r[1],
        ),
    )
