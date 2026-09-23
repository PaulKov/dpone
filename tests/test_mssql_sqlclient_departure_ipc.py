"""Synthetic helper IPC: content consistency is not actual launch/ACK authentication."""

import json
import math
from dataclasses import replace
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_departure_ipc import (
    SqlClientDeparturePlan,
    SqlClientDepartureRequest,
    validate_departure_request_binding,
)
from dpone.contracts.mssql_sqlclient_departure_ipc_codec import (
    decode_departure_plan,
    decode_departure_request,
    decode_departure_result,
    departure_request_digest,
    encode_departure_plan,
    encode_departure_request,
    encode_departure_result,
    make_departure_result,
)
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_mssql_sqlclient_create_departure_codec import maximum, sample
from tests.test_mssql_sqlclient_launch_contract import launch
from tests.test_mssql_sqlclient_session_control import OWNER
from tests.test_mssql_tds_lifecycle import identity


def request(departure=None):
    d = sample() if departure is None else departure
    attempt = identity(database=d.database.name)
    operation = TdsCoordinatorIdentity(
        attempt, 0, UUID("33333333-3333-4333-8333-333333333333"), TdsCoordinatorCommand.CREATE, "a" * 64, 1, "b" * 64
    )
    process = launch().process
    plan = SqlClientDeparturePlan(
        helper_id=UUID("44444444-4444-4444-8444-444444444444"),
        attempt=attempt,
        ownership=replace(OWNER),
        create_operation=operation,
        create_process=process,
        create_result_sha256="c" * 64,
        create_local_exit_sha256="d" * 64,
        original=d.original,
        database=d.database,
        creator_admission=d.admission,
        principal=d.principal,
        implementation_sha256="e" * 64,
        package_root="/synthetic",
        admission_sha256="f" * 64,
        startup_deadline=10.0,
        operation_deadline=20.0,
        max_address_space_bytes=1 << 30,
    )
    startup = TdsCoordinatorStartup(
        replace(process, pid=124), plan.implementation_sha256, plan.package_root, bytes(range(32))
    )
    return SqlClientDepartureRequest(plan=plan, startup=startup)


def bind(r, **changes):
    expected = dict(
        startup=r.startup,
        admission_sha256=r.plan.admission_sha256,
        startup_deadline=r.plan.startup_deadline,
        operation_deadline=r.plan.operation_deadline,
        max_address_space_bytes=r.plan.max_address_space_bytes,
    )
    expected.update(changes)
    validate_departure_request_binding(r, **expected)


def test_roundtrip_and_historical_builds_remain_distinct():
    r = request()
    assert (
        r.plan.attempt.implementation_sha256
        != r.plan.create_operation.implementation_sha256
        != r.plan.implementation_sha256
    )
    assert decode_departure_plan(encode_departure_plan(r.plan)) == r.plan
    assert decode_departure_request(encode_departure_request(r)) == r
    bind(r)
    result = make_departure_result(r, sample())
    assert result.request_sha256 == sha256(encode_departure_request(r)).hexdigest()
    assert decode_departure_result(encode_departure_result(result, request=r), request=r) == result


@pytest.mark.parametrize(
    "field,value",
    [
        ("admission_sha256", "0" * 64),
        ("startup_deadline", 9.0),
        ("operation_deadline", 21.0),
        ("max_address_space_bytes", 2 << 30),
        ("startup_deadline", 10),
        ("operation_deadline", True),
        ("max_address_space_bytes", float(1 << 30)),
    ],
)
def test_original_expectation_mismatch_and_alias(field, value):
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_departure_ipc_invalid$"):
        bind(request(), **{field: value})


@pytest.mark.parametrize(
    "field,value",
    [
        ("pid", 125),
        ("start_ticks", 457),
        ("host_sha256", "1" * 64),
        ("boot_id", "55555555-5555-4555-8555-555555555555"),
    ],
)
def test_actual_startup_process_independent_binding(field, value):
    r = request()
    with pytest.raises(ValueError):
        bind(r, startup=replace(r.startup, process=replace(r.startup.process, **{field: value})))


@pytest.mark.parametrize(
    "field,value", [("launch_nonce", b"a" * 32), ("package_root", "/different"), ("implementation_sha256", "1" * 64)]
)
def test_actual_startup_source_root_nonce(field, value):
    r = request()
    with pytest.raises(ValueError):
        bind(r, startup=replace(r.startup, **{field: value}))


@pytest.mark.parametrize("field", ["startup_deadline", "operation_deadline"])
@pytest.mark.parametrize("value", [0.0, -1.0, math.inf, math.nan, 1e-12, 1e20, 10, True])
def test_invalid_deadlines(field, value):
    with pytest.raises(ValueError):
        replace(request().plan, **{field: value})


@pytest.mark.parametrize(
    "mutation", ["parent", "fence", "command", "database", "deadlines", "source", "root", "host", "boot"]
)
def test_internal_crossbinding(mutation):
    r = request()
    with pytest.raises(ValueError):
        if mutation == "parent":
            replace(
                r.plan, create_operation=replace(r.plan.create_operation, parent=replace(r.plan.attempt, ordinal=1))
            )
        elif mutation == "fence":
            replace(r.plan, ownership=replace(r.plan.ownership, fence=2))
        elif mutation == "command":
            replace(
                r.plan,
                create_operation=replace(
                    r.plan.create_operation,
                    command=next(c for c in TdsCoordinatorCommand if c is not TdsCoordinatorCommand.CREATE),
                ),
            )
        elif mutation == "database":
            altered = replace(r.plan.attempt, database="other")
            replace(r.plan, attempt=altered, create_operation=replace(r.plan.create_operation, parent=altered))
        elif mutation == "deadlines":
            replace(r.plan, startup_deadline=21.0)
        else:
            changes = {
                "source": {"implementation_sha256": "1" * 64},
                "root": {"package_root": "/other"},
                "host": {"process": replace(r.startup.process, host_sha256="1" * 64)},
                "boot": {"process": replace(r.startup.process, boot_id="55555555-5555-4555-8555-555555555555")},
            }
            replace(r, startup=replace(r.startup, **changes[mutation]))


@pytest.mark.parametrize(
    "field",
    [
        "helper_id",
        "admission_sha256",
        "create_result_sha256",
        "create_local_exit_sha256",
        "implementation_sha256",
        "package_root",
        "operation_deadline",
        "max_address_space_bytes",
    ],
)
def test_result_exact_request_digest(field):
    r = request()
    result = make_departure_result(r, sample())
    values = dict(
        helper_id=UUID("55555555-5555-4555-8555-555555555555"),
        admission_sha256="1" * 64,
        create_result_sha256="1" * 64,
        create_local_exit_sha256="1" * 64,
        implementation_sha256="1" * 64,
        package_root="/other",
        operation_deadline=21.0,
        max_address_space_bytes=123,
    )
    plan = replace(r.plan, **{field: values[field]})
    altered = replace(
        r,
        plan=plan,
        startup=replace(r.startup, implementation_sha256=plan.implementation_sha256, package_root=plan.package_root),
    )
    with pytest.raises(ValueError):
        encode_departure_result(result, request=altered)
    with pytest.raises(ValueError):
        decode_departure_result(encode_departure_result(result, request=r), request=altered)


def test_creator_digest_cannot_be_sqlclient_writer_digest():
    from dpone.contracts.mssql_sqlclient_observation import session_authority_digest
    from tests.test_mssql_sqlclient_observation import observation

    r = request()
    with pytest.raises(ValueError):
        replace(
            r.plan,
            original=replace(r.plan.original, authority_sha256=session_authority_digest(observation().authority)),
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("package_root", "relative"),
        ("package_root", "/\x7f"),
        ("package_root", "/" + "x" * 4096),
        ("helper_id", UUID(int=0)),
        ("max_address_space_bytes", 0),
        ("max_address_space_bytes", 2**63),
    ],
)
def test_plan_bounds(field, value):
    with pytest.raises(ValueError):
        replace(request().plan, **{field: value})


@pytest.mark.parametrize("character", ['"', "\\", "\uffff", "\U0010ffff"])
def test_joint_maximum_wire_caps(character):
    departure = maximum(character)
    r = request(departure)
    attempt = replace(
        r.plan.attempt,
        target_key=character * 256,
        run_id=character * 256,
        schema=departure.database.name,
        table=departure.database.name,
        ordinal=2**63 - 1,
        attempt=2,
    )
    owner = replace(r.plan.ownership, owner=character * 256, fence=2**63 - 1)
    operation = replace(r.plan.create_operation, parent=attempt, original_fence=owner.fence, slot_index=2**63 - 1)
    # Exactly4096 UTF8 bytes, without padding the serialized JSON.
    width = len(character.encode("utf8"))
    root = "/" + character * (4095 // width) + "x" * (4095 % width)
    deadline = math.nextafter((2**63 - 1) / 1e9, 0.0)
    process = replace(r.plan.create_process, pid=2**31 - 1, start_ticks=2**63 - 1)
    plan = replace(
        r.plan,
        attempt=attempt,
        ownership=owner,
        create_operation=operation,
        create_process=process,
        package_root=root,
        startup_deadline=deadline,
        operation_deadline=deadline,
        max_address_space_bytes=2**63 - 1,
    )
    r = replace(r, plan=plan, startup=replace(r.startup, package_root=root, process=replace(process, pid=2**31 - 2)))
    p = encode_departure_plan(plan)
    raw = encode_departure_request(r)
    result = make_departure_result(r, departure)
    body = encode_departure_result(result, request=r)
    assert len(root.encode("utf8")) == 4096
    assert len(p) <= 65536 and len(raw) <= 65536 and len(body) <= 32768
    assert decode_departure_plan(p) == plan and decode_departure_request(raw) == r
    assert decode_departure_result(body, request=r) == result
    bind(r)


@pytest.mark.parametrize("kind", ["plan", "request", "result"])
@pytest.mark.parametrize(
    "mutation", ["missing", "extra", "duplicate", "whitespace", "trailing", "oversize", "bytes_type"]
)
def test_closed_canonical_private_wire(kind, mutation):
    r = request()
    encoders = {
        "plan": lambda: encode_departure_plan(r.plan),
        "request": lambda: encode_departure_request(r),
        "result": lambda: encode_departure_result(make_departure_result(r, sample()), request=r),
    }
    decoders = {
        "plan": decode_departure_plan,
        "request": decode_departure_request,
        "result": lambda body: decode_departure_result(body, request=r),
    }
    body = encoders[kind]()
    data = json.loads(body)
    if mutation == "extra":
        data["private-canary"] = "private-canary"
        body = canonical_json_bytes(data)
    elif mutation == "missing":
        del data["schema"]
        body = canonical_json_bytes(data)
    elif mutation == "duplicate":
        body = body[:-1] + b',"schema":' + json.dumps(data["schema"]).encode() + b"}"
    elif mutation == "whitespace":
        body += b" "
    elif mutation == "trailing":
        body += b"{}"
    elif mutation == "oversize":
        body = b"x" * (32769 if kind == "result" else 65537)
    else:
        body = bytearray(body)
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_departure_ipc_invalid$"):
        decoders[kind](body)


@pytest.mark.parametrize(
    "path,field,alias",
    [
        (("plan",), "helper_id", "44444444-4444-4444-8444-444444444444"),
        (("plan", "attempt"), "ordinal", 0.0),
        (("plan", "ownership"), "fence", True),
        (("plan", "create_operation"), "command", "create"),
        (("plan", "create_operation"), "operation_id", "33333333-3333-4333-8333-333333333333"),
        (("plan", "create_operation", "parent"), "attempt", False),
        (("startup", "process"), "pid", 124.0),
        (("startup",), "launch_nonce", bytearray(range(32))),
        (("plan", "original"), "session_id", 72.0),
        (("plan", "creator_admission", "login"), "is_sysadmin", 0),
        (("plan", "principal"), "principal_id", 5.0),
        (("plan",), "startup_deadline", 10),
    ],
)
def test_no_forged_nested_normalization(path, field, alias):
    r = request()
    nested = r
    for key in path:
        nested = getattr(nested, key)
    object.__setattr__(nested, field, alias)
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_departure_ipc_invalid$"):
        encode_departure_request(r)


@pytest.mark.parametrize(
    "path",
    [
        ("attempt",),
        ("ownership",),
        ("create_operation",),
        ("create_operation", "parent"),
        ("create_process",),
        ("original",),
        ("database",),
        ("creator_admission", "server"),
        ("creator_admission", "database"),
        ("creator_admission", "login"),
        ("creator_admission", "transport"),
        ("principal",),
    ],
)
def test_nested_shape_closed(path):
    data = json.loads(encode_departure_plan(request().plan))
    nested = data
    for key in path:
        nested = nested[key]
    nested["private-canary"] = "private-canary"
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_departure_ipc_invalid$"):
        decode_departure_plan(canonical_json_bytes(data))


def test_result_rejects_changed_observation_even_with_valid_create_digest():
    from tests.test_mssql_sqlclient_create_departure_codec import maximum

    with pytest.raises(ValueError):
        make_departure_result(request(), maximum("x"))


def test_invalid_request_not_hashed(monkeypatch):
    from dpone.contracts import mssql_sqlclient_departure_ipc_codec as module

    r = request()
    object.__setattr__(r.startup.process, "pid", 124.0)
    calls = []
    monkeypatch.setattr(module, "sha256", lambda *args: calls.append(args))
    with pytest.raises(ValueError):
        departure_request_digest(r)
    assert not calls


GOLDEN_REQUEST = (
    b'{"plan":{"admission_sha256":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","attempt":{"att'
    b'empt":0,"database":"target","file_sha256":"4444444444444444444444444444444444444444444444444444444444444444","'
    b'implementation_sha256":"3333333333333333333333333333333333333333333333333333333333333333","ordinal":0,"owner_b'
    b'inding":"5555555555555555555555555555555555555555555555555555555555555555","plan_sha256":"11111111111111111111'
    b'11111111111111111111111111111111111111111111","policy_sha256":"22222222222222222222222222222222222222222222222'
    b'22222222222222222","run_id":"run","schema":"dbo","table":"tds_owned","target_key":"target"},"create_local_exit'
    b'_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","create_operation":{"command":"cre'
    b'ate","command_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","implementation_sha25'
    b'6":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","operation_id":"33333333-3333-4333-8333-'
    b'333333333333","original_fence":1,"parent":{"attempt":0,"database":"target","file_sha256":"44444444444444444444'
    b'44444444444444444444444444444444444444444444","implementation_sha256":"333333333333333333333333333333333333333'
    b'3333333333333333333333333","ordinal":0,"owner_binding":"555555555555555555555555555555555555555555555555555555'
    b'5555555555","plan_sha256":"1111111111111111111111111111111111111111111111111111111111111111","policy_sha256":"'
    b'2222222222222222222222222222222222222222222222222222222222222222","run_id":"run","schema":"dbo","table":"tds_o'
    b'wned","target_key":"target"},"slot_index":0},"create_process":{"boot_id":"22222222-2222-4222-8222-222222222222'
    b'","host_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","pid":123,"start_ticks":456'
    b'},"create_result_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","creator_admission'
    b'":{"database":{"database_guid":"01234567-89ab-cdef-0123-456789abcdef","database_id":7,"database_name":"target"'
    b',"owner_sid":"bb"},"login":{"authenticating_database_id":0,"is_sysadmin":false,"name":"writer","original_name"'
    b':"writer","original_sid":"aa","principal_id":300,"sid":"aa"},"server":{"instance_name":"MSSQLSERVER","machine_'
    b'name":"machine","physical_machine_name":"physical","server_name":"server"},"transport":{"auth_scheme":"SQL","e'
    b'ncrypt_option":"TRUE","net_transport":"TCP","protocol_type":"TSQL"}},"database":{"database_guid":"01234567-89a'
    b'b-cdef-0123-456789abcdef","database_id":7,"name":"target"},"helper_id":"44444444-4444-4444-8444-444444444444",'
    b'"implementation_sha256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","max_address_space_'
    b'bytes":1073741824,"operation_deadline":20.0,"original":{"authority_sha256":"e1d5d7665d319ec23ed73d2073189d7006'
    b'e16bff95e73e8d05b6041b7d243cf1","connect_time":"2026-09-15T12:00:00.000000","connection_id":"01234567-89ab-cde'
    b'f-0123-456789abcdef","login_time":"2026-09-15T12:00:01.000000","nonce":"000102030405060708090a0b0c0d0e0f101112'
    b'131415161718191a1b1c1d1e1f","schema":"dpone.tds.remote-session.v1","session_id":72},"ownership":{"fence":1,"ow'
    b'ner":"owner","supervisor_id":"33333333-3333-4333-8333-333333333333"},"package_root":"/synthetic","principal":{'
    b'"name":"writer_user","principal_id":5,"sid":"aa"},"schema":"dpone.sqlclient.departure-plan.v1","startup_deadli'
    b'ne":10.0},"schema":"dpone.sqlclient.departure-request.v1","startup":{"implementation_sha256":"eeeeeeeeeeeeeeee'
    b'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","launch_nonce":"000102030405060708090a0b0c0d0e0f101112131415'
    b'161718191a1b1c1d1e1f","package_root":"/synthetic","process":{"boot_id":"22222222-2222-4222-8222-222222222222",'
    b'"host_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","pid":124,"start_ticks":456},'
    b'"schema":"dpone.tds.coordinator-startup.v1"}}'
)


def test_literal_request_bytes_and_hash():
    assert encode_departure_request(request()) == GOLDEN_REQUEST
    assert decode_departure_request(GOLDEN_REQUEST) == request()
    assert departure_request_digest(request()) == "7192b281f24c2d31f9c4783d4c50b975670293a938c86bdeaeeaca130345325c"
    assert sha256(GOLDEN_REQUEST).hexdigest() == departure_request_digest(request())
