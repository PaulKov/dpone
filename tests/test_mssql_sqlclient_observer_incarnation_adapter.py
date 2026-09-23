"""Offline raw capture tests; no SQL execution or driver qualification."""

import pytest

from dpone.adapters.mssql_sqlclient_observer_incarnation import capture_observer_incarnation
from tests.mssql_sqlclient_departure_v2_fixtures import alias, sample


def own_row(own=None):
    own = own or sample().observer
    a, v = own.authority, own.visibility
    p = a.principal_resolution
    return [
        own.session_id,
        a.database.database_id,
        a.login.name,
        bytes.fromhex(a.login.sid),
        a.login.original_name,
        bytes.fromhex(a.login.original_sid),
        p.principal_id,
        p.name,
        0,
        0,
        0,
        v.server_major_version,
        v.engine_edition,
        v.view_server_state,
        v.view_server_performance_state,
        a.server.server_name,
        a.server.machine_name,
        a.server.instance_name,
        a.server.physical_machine_name,
        1,
        own.connection_id,
        own.session_id,
        own.connect_time,
        None,
        a.transport.net_transport,
        a.transport.protocol_type,
        a.transport.auth_scheme,
        a.transport.encrypt_option,
        1,
        own.session_id,
        own.login_time,
        1,
        0,
        a.database.database_id,
        a.login.name,
        a.login.sid.upper(),
        a.login.original_name,
        a.login.original_sid.upper(),
        a.login.authenticating_database_id,
        1,
        a.database.database_id,
        a.database.database_name,
        a.database.owner_sid.upper(),
        1,
        __import__("uuid").UUID(a.database.database_guid),
        1,
        a.login.principal_id,
        a.login.name,
        a.login.sid.upper(),
        "SQL_LOGIN",
        int(a.login.is_sysadmin),
        2,
        1,
        "dbo",
        a.database.owner_sid.upper(),
        "SQL_USER",
        "INSTANCE",
        p.principal_id,
        p.name,
        p.sid.upper(),
        "SQL_USER",
        "INSTANCE",
        1,
        p.principal_id,
        p.name,
        p.sid.upper(),
        0,
        0,
    ]


def capture(rows):
    calls = []

    def query(sql, *parameters):
        calls.append((sql, parameters))
        return rows

    result = capture_observer_incarnation(query, admission=sample().admission)
    assert len(calls) == 1 and calls[0][1] == ()
    return result


def test_complete_capture():
    assert len(own_row()) == 68
    assert capture([own_row()]) == sample().observer


@pytest.mark.parametrize("index", [19, 28, 39, 43, 45, 62])
@pytest.mark.parametrize("value", [0, 2, True, 1.0, None])
def test_raw_cardinality_before_payload(index, value):
    row = own_row()
    row[index] = value
    row[20] = object()
    with pytest.raises(ValueError):
        capture([row])


@pytest.mark.parametrize("index", range(68))
def test_equal_aliases_reject(index):
    row = own_row()
    if row[index] is None:
        return
    row[index] = alias(row[index])
    with pytest.raises(ValueError):
        capture([row])


@pytest.mark.parametrize("rows", [[], [own_row(), own_row()], [own_row()[:-1]], [own_row() + [0]]])
def test_bounded_row_shape(rows):
    with pytest.raises(ValueError):
        capture(rows)


@pytest.mark.parametrize(
    "index,value",
    [
        (51, 0),
        (51, 3),
        (51, True),
        (51, 1),
        (66, 1),
        (67, 1),
        (8, 1),
        (9, 1),
        (10, 2),
        (32, 1),
        (31, 0),
        (0, 71),
        (21, 71),
        (29, 71),
        (1, 8),
        (33, 8),
        (40, 8),
        (38, 1),
        (6, 6),
        (63, 6),
        (7, "other"),
        (64, "other"),
        (65, "BB"),
        (35, "AB"),
        (37, "AB"),
        (48, "AB"),
        (49, "WINDOWS_LOGIN"),
        (55, "WINDOWS_USER"),
        (60, "WINDOWS_USER"),
        (61, "DATABASE"),
        (54, " AA"),
        (59, "aa"),
        (13, 2),
        (14, 0),
        (11, 18),
        (12, 1),
        (24, "Named pipe"),
        (50, 2),
    ],
)
def test_independent_context_and_raw_gates(index, value):
    row = own_row()
    row[index] = value
    with pytest.raises(ValueError):
        capture([row])


def test_one_complete_candidate_slot_uses_existing_resolver():
    row = own_row()
    row[51] = 1
    row[52:57] = row[57:62]
    row[57:62] = [None] * 5
    assert capture([row]) == sample().observer


def test_bad_cardinality_rejects_before_sid_conversion(monkeypatch):
    import dpone.adapters.mssql_sqlclient_observer_incarnation as module

    def forbidden(value):
        pytest.fail("conversion reached before cardinality rejection")

    monkeypatch.setattr(module, "_sid", forbidden)
    row = own_row()
    row[19] = 2
    with pytest.raises(ValueError):
        capture([row])
