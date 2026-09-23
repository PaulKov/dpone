"""Synthetic catalog authority checks; these never certify a live SQL route."""

import hashlib
import json
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_observation import (
    AUTHORITY_DOMAIN,
    SqlClientDatabaseAuthority,
    SqlClientLoginAuthority,
    SqlClientObserverAdmission,
    SqlClientServerAuthority,
    SqlClientTransportAuthority,
    encode_session_authority,
    resolve_writer_observation,
    session_authority_digest,
    validate_catalog_admission,
    validate_visibility,
    validate_writer_rows,
)

NONCE = bytes(range(32))
GUID = UUID("01234567-89ab-cdef-0123-456789abcdef")
STAMP = datetime(2026, 9, 15, 12)


def admission(*, sysadmin=False, owner=False):
    return SqlClientObserverAdmission(
        SqlClientServerAuthority("server", "machine", "MSSQLSERVER", "physical"),
        SqlClientDatabaseAuthority(7, "target", str(GUID), "aa" if owner else "bb"),
        SqlClientLoginAuthority(300, "writer", "aa", "writer", "aa", 0, sysadmin),
        SqlClientTransportAuthority("TCP", "TSQL", "SQL", "TRUE"),
    )


def writer_row(*, sysadmin=False, owner=False):
    return [
        GUID,
        72,
        STAMP,
        STAMP + timedelta(seconds=1),
        None,
        "TCP",
        "TSQL",
        "SQL",
        0,
        "sleeping",
        1,
        0,
        0,
        0,
        NONCE,
        "server",
        "machine",
        "MSSQLSERVER",
        "physical",
        7,
        "target",
        GUID,
        bytes.fromhex("aa" if owner else "bb"),
        300,
        "writer",
        b"\xaa",
        "writer",
        b"\xaa",
        0,
        int(sysadmin),
        "SQL_LOGIN",
        "TRUE",
        "writer",
        b"\xaa",
        "writer",
    ]


def catalog_row(*, sysadmin=False, owner=False):
    r = writer_row(sysadmin=sysadmin, owner=owner)
    return r[15:26] + [r[29], r[30]]


def principal_rows():
    return [(1, "dbo", b"\xbb", "SQL_USER", "INSTANCE"), (5, "writer_user", b"\xaa", "SQL_USER", "INSTANCE")]


def observation(*, sysadmin=False, owner=False):
    admitted = admission(sysadmin=sysadmin, owner=owner)
    row = validate_writer_rows(
        [writer_row(sysadmin=sysadmin, owner=owner)], session_id=72, nonce=NONCE, admission=admitted
    )
    return resolve_writer_observation(row, principal_rows(), admission=admitted)


def test_explicit_master_authentication_profile_preserves_raw_authority():
    admitted = admission()
    master = replace(admitted, login=replace(admitted.login, authenticating_database_id=1))
    row = writer_row()
    row[28] = 1
    validate_catalog_admission([catalog_row()], master)
    result = resolve_writer_observation(tuple(row), principal_rows(), admission=master)
    assert result.authority.login.authenticating_database_id == 1
    assert result.remote_session.authority_sha256 != observation().remote_session.authority_sha256
    for expected, actual in ((admitted, row), (master, writer_row())):
        with pytest.raises(ValueError):
            validate_writer_rows([actual], session_id=72, nonce=NONCE, admission=expected)


@pytest.mark.parametrize("auth_database", [-1, 2, 7, None, True, False, 1.0, "1"])
def test_authentication_profile_rejects_unadmitted_database_and_aliases(auth_database):
    with pytest.raises(ValueError):
        replace(admission().login, authenticating_database_id=auth_database)


def test_authority_independent_golden_payload_and_domain():
    expected = {
        "schema_version": 1,
        "profile": "sql_login_initial_context_v1",
        "server": {
            "server_name": "server",
            "machine_name": "machine",
            "instance_name": "MSSQLSERVER",
            "physical_machine_name": "physical",
        },
        "database": {"database_id": 7, "database_name": "target", "database_guid": str(GUID), "owner_sid": "bb"},
        "login": {
            "principal_id": 300,
            "name": "writer",
            "sid": "aa",
            "original_name": "writer",
            "original_sid": "aa",
            "authenticating_database_id": 0,
            "is_sysadmin": False,
        },
        "principal_resolution": {"kind": "mapped_user", "principal_id": 5, "name": "writer_user", "sid": "aa"},
        "transport": {"net_transport": "TCP", "protocol_type": "TSQL", "auth_scheme": "SQL", "encrypt_option": "TRUE"},
    }
    payload = json.dumps(expected, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(b"dpone.sqlclient.session-authority.v1\0" + payload).digest()
    observed = observation()
    assert AUTHORITY_DOMAIN == b"dpone.sqlclient.session-authority.v1\0"
    assert encode_session_authority(observed.authority) == payload
    assert session_authority_digest(observed.authority) == digest
    assert observed.remote_session.authority_sha256 == digest
    assert observed.resolved_database_principal.name == "writer_user"
    with pytest.raises(FrozenInstanceError):
        observed.authority.login.name = "other"


@pytest.mark.parametrize(
    ("sysadmin", "owner", "kind"),
    [
        (True, False, "sysadmin_dbo"),
        (True, True, "sysadmin_dbo"),
        (False, True, "owner_dbo"),
        (False, False, "mapped_user"),
    ],
)
def test_catalog_mapping_precedence(sysadmin, owner, kind):
    result = observation(sysadmin=sysadmin, owner=owner)
    assert result.authority.principal_resolution.kind == kind
    # db_owner membership / CONTROL SERVER are not queried or used as dbo rules.
    assert result.resolved_database_principal.principal_id == (5 if kind == "mapped_user" else 1)


@pytest.mark.parametrize(
    ("index", "bad"),
    [
        (0, UUID(int=0)),
        (0, str(GUID)),
        (1, True),
        (1, 73),
        (2, datetime(1900, 1, 1)),
        (3, None),
        (4, GUID),
        (5, "Session"),
        (6, None),
        (7, "NTLM"),
        (8, 1),
        (8, False),
        (9, "running"),
        (10, 0),
        (10, True),
        (11, 1),
        (12, 1),
        (13, 1),
        (14, b"x" * 32),
        (14, NONCE + b"\0"),
        (14, bytearray(NONCE)),
        (15, "other"),
        (16, None),
        (17, ""),
        (18, "wrong"),
        (19, 8),
        (19, True),
        (20, "other"),
        (21, UUID(int=9)),
        (22, b"\xcc"),
        (23, 301),
        (24, "other"),
        (25, b"\xcc"),
        (26, "other"),
        (27, b"\xcc"),
        (28, 7),
        (29, None),
        (29, True),
        (29, 2),
        (29, 1),
        (30, "WINDOWS_LOGIN"),
        (31, "FALSE"),
        (31, None),
        (32, "other"),
        (33, b"\xcc"),
        (34, "other"),
    ],
)
def test_writer_rejects_bad_facts(index, bad):
    row = writer_row()
    row[index] = bad
    with pytest.raises(ValueError):
        validate_writer_rows([row], session_id=72, nonce=NONCE, admission=admission())


@pytest.mark.parametrize("rows", [[], [writer_row(), writer_row()], [writer_row()[:-1]], [writer_row() + [None]]])
def test_writer_cardinality(rows):
    with pytest.raises(ValueError):
        validate_writer_rows(rows, session_id=72, nonce=NONCE, admission=admission())


@pytest.mark.parametrize(
    "rows",
    [
        [],
        principal_rows() * 2,
        [(5, "guest", b"\xaa", "SQL_USER", "INSTANCE")],
        [(5, "writer_user", b"\xaa", "SQL_USER", "DATABASE")],
        [(5, "writer_user", b"\xaa", "WINDOWS_USER", "INSTANCE")],
        [(5, "writer_user", None, "SQL_USER", "INSTANCE")],
        [(5, "writer_user", b"\xaa", "SQL_USER", None)],
    ],
)
def test_reject_invisible_ambiguous_contained_or_guest_mapping(rows):
    with pytest.raises(ValueError):
        resolve_writer_observation(tuple(writer_row()), rows, admission=admission())


@pytest.mark.parametrize("row", [(13, 2, 1, None, 7), (15, 3, 1, None, 7), (16, 4, 0, 1, 7), (17, 3, None, 1, 7)])
def test_version_scoped_permission(row):
    validate_visibility([row], admission())


@pytest.mark.parametrize(
    "row",
    [
        (12, 3, 1, 1, 7),
        (18, 3, 1, 1, 7),
        (16, 5, 1, 1, 7),
        (16, 8, 1, 1, 7),
        (15, 3, 0, 1, 7),
        (16, 3, 1, 0, 7),
        (16, 3, 1, None, 7),
        (16, 3, 1, True, 7),
        (16, 3, 1, 1, 8),
    ],
)
def test_visibility_fails_closed(row):
    with pytest.raises(ValueError):
        validate_visibility([row], admission())


def test_catalog_preflight_matches_independent_admission():
    validate_catalog_admission([catalog_row()], admission())
    for index in range(13):
        row = catalog_row()
        row[index] = None
        with pytest.raises((ValueError, TypeError)):
            validate_catalog_admission([row], admission())


@pytest.mark.parametrize("sid", ["", "A0", "a", "gg", "aa" * 86])
def test_strict_sid(sid):
    with pytest.raises(ValueError):
        replace(admission().login, sid=sid)


def test_unicode_authority_canonical_utf8_and_sql_utf16_bounds():
    from dpone.contracts.mssql_sqlclient_observation import SqlClientSessionAuthority

    base = observation().authority
    changed = replace(base.server, server_name="сервер😀")
    authority = SqlClientSessionAuthority(changed, base.database, base.login, base.transport, base.principal_resolution)
    expected = json.loads(encode_session_authority(base))
    expected["server"]["server_name"] = "сервер😀"
    encoded = json.dumps(expected, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert encode_session_authority(authority) == encoded
    assert (
        session_authority_digest(authority)
        == hashlib.sha256(b"dpone.sqlclient.session-authority.v1\0" + encoded).digest()
    )
    with pytest.raises(ValueError):
        replace(base.server, server_name="😀" * 65)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", True),
        ("schema_version", 2),
        ("profile", "other"),
        ("principal_resolution", None),
        ("server", {}),
        ("login", None),
        ("database", {}),
    ],
)
def test_authority_closed_strict_types(field, value):
    with pytest.raises(ValueError):
        replace(observation().authority, **{field: value})


def test_resolution_does_not_accept_unvalidated_writer_row():
    row = writer_row()
    row[11] = 1
    with pytest.raises(ValueError):
        resolve_writer_observation(tuple(row), principal_rows(), admission=admission())


@pytest.mark.parametrize("rows", [[], [catalog_row(), catalog_row()], [catalog_row()[:-1]]])
def test_catalog_cardinality(rows):
    with pytest.raises(ValueError):
        validate_catalog_admission(rows, admission())


@pytest.mark.parametrize("rows", [[], [(16, 3, 1, 1, 7)] * 2, [(16, 3, 1, 1)]])
def test_visibility_cardinality(rows):
    with pytest.raises(ValueError):
        validate_visibility(rows, admission())
