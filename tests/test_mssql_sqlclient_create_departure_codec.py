"""Synthetic departure IPC only; matching content never authenticates exclusion."""

import json
from dataclasses import replace
from datetime import datetime
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_create_departure import SqlClientCreateDeparture
from dpone.contracts.mssql_sqlclient_create_departure_codec import decode_create_departure, encode_create_departure
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_tds_coordinator_authority import TdsDatabaseObservation
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity, coordinator_authority_digest
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_mssql_sqlclient_observation import admission


def sample():
    a = admission()
    p = SqlClientDatabasePrincipal(5, "writer_user", "aa")
    s, d, login = a.server, a.database, a.login
    digest = coordinator_authority_digest(
        [
            s.server_name,
            s.machine_name,
            s.instance_name,
            s.physical_machine_name,
            d.database_name,
            d.database_id,
            UUID(d.database_guid),
            login.name,
            bytes.fromhex(login.sid),
            login.original_name,
            bytes.fromhex(login.original_sid),
            p.name,
            p.principal_id,
            bytes.fromhex(p.sid),
        ]
    )
    original = TdsRemoteSessionIdentity(
        UUID(d.database_guid), 72, datetime(2026, 9, 15, 12), datetime(2026, 9, 15, 12, 0, 1), bytes(range(32)), digest
    )
    return SqlClientCreateDeparture(
        original=original,
        database=TdsDatabaseObservation(d.database_name, d.database_id, UUID(d.database_guid)),
        admission=a,
        principal=p,
        counts=(0,) * 6,
    )


def test_roundtrip():
    value = sample()
    assert decode_create_departure(encode_create_departure(value)) == value


GOLDEN = (
    b'{"admission":{"database":{"database_guid":"01234567-89ab-cdef-0123-456789abcdef",'
    b'"database_id":7,"database_name":"target","owner_sid":"bb"},"login":{'
    b'"authenticating_database_id":0,"is_sysadmin":false,"name":"writer","original_name":"writer",'
    b'"original_sid":"aa","principal_id":300,"sid":"aa"},"server":{"instance_name":"MSSQLSERVER",'
    b'"machine_name":"machine","physical_machine_name":"physical","server_name":"server"},'
    b'"transport":{"auth_scheme":"SQL","encrypt_option":"TRUE","net_transport":"TCP","protocol_type":"TSQL"}},'
    b'"counts":[0,0,0,0,0,0],"database":{"database_guid":"01234567-89ab-cdef-0123-456789abcdef",'
    b'"database_id":7,"name":"target"},"original":{'
    b'"authority_sha256":"e1d5d7665d319ec23ed73d2073189d7006e16bff95e73e8d05b6041b7d243cf1",'
    b'"connect_time":"2026-09-15T12:00:00.000000","connection_id":"01234567-89ab-cdef-0123-456789abcdef",'
    b'"login_time":"2026-09-15T12:00:01.000000",'
    b'"nonce":"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",'
    b'"schema":"dpone.tds.remote-session.v1","session_id":72},'
    b'"principal":{"name":"writer_user","principal_id":5,"sid":"aa"},'
    b'"schema":"dpone.sqlclient.create-departure.v1"}'
)


def test_literal_golden_bytes_and_hash():
    from hashlib import sha256

    assert encode_create_departure(sample()) == GOLDEN
    assert sha256(GOLDEN).hexdigest() == "801d4936cb4084b047f073c42196b788b1ed1ad787b3da34483b05f625cfecd2"
    assert decode_create_departure(GOLDEN) == sample()


def maximum(character):
    import json
    from hashlib import sha256

    value = sample()
    name = character * (128 // (len(character.encode("utf-16le")) // 2))
    a = value.admission
    a = replace(
        a,
        server=replace(a.server, server_name=name, machine_name=name, instance_name=name, physical_machine_name=name),
        database=replace(a.database, database_name=name, database_id=2**31 - 1, owner_sid="ff" * 85),
        login=replace(
            a.login,
            name=name,
            original_name=name,
            sid="aa" * 85,
            original_sid="aa" * 85,
            principal_id=2**31 - 1,
            authenticating_database_id=1,
        ),
    )
    principal = replace(value.principal, name=name, principal_id=2**31 - 1, sid="aa" * 85)
    # Independent producer for the established CREATE digest, rather than the
    # function used by departure validation.
    facts = [
        "dpone.tds.session-authority.v1",
        name,
        name,
        name,
        name,
        name,
        2**31 - 1,
        a.database.database_guid,
        name,
        "aa" * 85,
        name,
        "aa" * 85,
        name,
        2**31 - 1,
        "aa" * 85,
    ]
    digest = sha256(json.dumps(facts, ensure_ascii=True, separators=(",", ":")).encode("ascii")).digest()
    original = replace(
        value.original, session_id=32767, connect_time=datetime.max, login_time=datetime.max, authority_sha256=digest
    )
    return replace(
        value,
        original=original,
        database=replace(value.database, name=name, database_id=2**31 - 1),
        admission=a,
        principal=principal,
    )


@pytest.mark.parametrize("character", ['"', "\\", "\uffff", "\U0010ffff"])
def test_joint_maximum_fits_real_encoding(character):
    value = maximum(character)
    raw = encode_create_departure(value)
    assert 0 < len(raw) < 16384
    assert decode_create_departure(raw) == value
    assert len(value.principal.name.encode("utf-16le")) == 256
    assert len(value.principal.sid) == 170


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("original",),
        ("database",),
        ("admission",),
        ("principal",),
        ("admission", "server"),
        ("admission", "database"),
        ("admission", "login"),
        ("admission", "transport"),
    ],
)
@pytest.mark.parametrize("mutation", ["extra", "missing"])
def test_every_nested_shape_closed_and_private(path, mutation):
    data = json.loads(GOLDEN)
    nested = data
    for key in path:
        nested = nested[key]
    if mutation == "extra":
        nested["private-canary"] = "private-canary"
    else:
        del nested[next(iter(nested))]
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_create_departure_record_invalid$"):
        decode_create_departure(canonical_json_bytes(data))


@pytest.mark.parametrize(
    "path,bad",
    [
        (("original", "session_id"), True),
        (("original", "session_id"), 72.0),
        (("original", "connection_id"), "01234567-89AB-CDEF-0123-456789ABCDEF"),
        (("original", "connect_time"), "2026-09-15T12:00:00"),
        (("original", "nonce"), "00" * 32),
        (("database", "database_id"), 7.0),
        (("database", "database_guid"), "0123456789abcdef0123456789abcdef"),
        (("admission", "login", "is_sysadmin"), 0),
        (("admission", "login", "authenticating_database_id"), False),
        (("admission", "database", "database_id"), True),
        (("principal", "principal_id"), 5.0),
        (("counts",), [False] * 6),
        (("counts",), [0.0] * 6),
        (("counts",), [0] * 5),
        (("counts",), [0, 0, 0, 0, 0, 1]),
    ],
)
def test_scalar_and_count_aliases(path, bad):
    data = json.loads(GOLDEN)
    nested = data
    for key in path[:-1]:
        nested = nested[key]
    nested[path[-1]] = bad
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_create_departure_record_invalid$"):
        decode_create_departure(canonical_json_bytes(data))


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b" " * 16385,
        bytearray(GOLDEN),
        GOLDEN + b" ",
        b" " + GOLDEN,
        GOLDEN + b"{}",
        GOLDEN.replace(b'"counts":[', b'"counts":[],"counts":['),
        GOLDEN.replace(b'"session_id":72', b'"session_id":72,"session_id":72'),
        b'"private-canary"',
        b"\xff",
    ],
)
def test_invalid_wire_constant(payload):
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_create_departure_record_invalid$"):
        decode_create_departure(payload)


@pytest.mark.parametrize(
    "path,field,alias",
    [
        (("original",), "connection_id", "01234567-89ab-cdef-0123-456789abcdef"),
        (("original",), "session_id", 72.0),
        (("original",), "nonce", bytearray(range(32))),
        (("database",), "database_guid", "01234567-89ab-cdef-0123-456789abcdef"),
        (("database",), "database_id", 7.0),
        (("principal",), "principal_id", 5.0),
        (("admission", "login"), "is_sysadmin", 0),
        (("admission", "login"), "authenticating_database_id", False),
        ((), "counts", [0] * 6),
        ((), "counts", (False,) * 6),
    ],
)
def test_encode_rejects_forged_nested_without_normalizing(path, field, alias):
    value = sample()
    nested = value
    for key in path:
        nested = getattr(nested, key)
    object.__setattr__(nested, field, alias)
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_create_departure_record_invalid$"):
        encode_create_departure(value)


def test_bound_checked_before_parse_and_type_before_encode(monkeypatch):
    from dpone.contracts import mssql_sqlclient_create_departure_codec as module

    called = []
    monkeypatch.setattr(module, "strict_json_object", lambda *args: called.append(args))
    for payload in (b"x" * 16385, bytearray(GOLDEN)):
        with pytest.raises(ValueError):
            decode_create_departure(payload)
    with pytest.raises(ValueError):
        encode_create_departure({"private-canary": 1})
    assert not called
