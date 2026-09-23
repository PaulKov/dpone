"""Synthetic private bytes remain separate from durable nonsecret request intent."""

import json
import math
from dataclasses import replace

import pytest

from dpone.adapters.mssql_tds_coordinator_connection import TdsConnectionMaterial
from dpone.app.mssql_sqlclient_departure_request import (
    SqlClientDepartureCredentials,
    decode_departure_credentials,
    encode_departure_credentials,
)
from dpone.contracts.mssql_sqlclient_departure_ipc_codec import departure_request_digest
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_mssql_sqlclient_create_departure_codec import maximum
from tests.test_mssql_sqlclient_departure_ipc import GOLDEN_REQUEST, request


def credentials(r=None):
    r = request() if r is None else r
    return SqlClientDepartureCredentials(
        request=r,
        connection_material=TdsConnectionMaterial(
            "synthetic.invalid", 1433, r.plan.database.name, "separate_observer", "secret-canary"
        ),
    )


def decode(payload, r=None, **changes):
    r = request() if r is None else r
    expected = dict(
        startup=r.startup,
        admission_sha256=r.plan.admission_sha256,
        startup_deadline=r.plan.startup_deadline,
        operation_deadline=r.plan.operation_deadline,
        max_address_space_bytes=r.plan.max_address_space_bytes,
    )
    expected.update(changes)
    return decode_departure_credentials(payload, **expected)


def test_literal_private_bytes_roundtrip_and_foreign_observer():
    value = credentials()
    raw = (
        b'{"connection_material":{"database":"target","host":"synthetic.invalid","password":"secret-canary","port":1433,"username":"separate_observer"},"request":'
        + GOLDEN_REQUEST
        + b',"schema":"dpone.sqlclient.departure-credentials.v1"}'
    )
    assert encode_departure_credentials(value) == raw
    assert decode(raw) == value
    assert value.connection_material.username != value.request.plan.creator_admission.login.name
    assert "secret-canary" not in repr(value) and "synthetic.invalid" not in repr(value)
    other = replace(value, connection_material=replace(value.connection_material, password="different-canary"))
    assert departure_request_digest(other.request) == departure_request_digest(value.request)
    assert encode_departure_credentials(other) != raw


@pytest.mark.parametrize(
    "field,bad",
    [
        ("admission_sha256", "0" * 64),
        ("startup_deadline", 9.0),
        ("operation_deadline", 21.0),
        ("max_address_space_bytes", 1),
        ("startup_deadline", 10),
        ("operation_deadline", True),
        ("max_address_space_bytes", float(1 << 30)),
    ],
)
def test_each_independent_launch_value(field, bad):
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_departure_credentials_invalid$"):
        decode(encode_departure_credentials(credentials()), **{field: bad})


@pytest.mark.parametrize(
    "field,bad",
    [
        ("pid", 125),
        ("start_ticks", 457),
        ("host_sha256", "0" * 64),
        ("boot_id", "55555555-5555-4555-8555-555555555555"),
    ],
)
def test_independent_startup_process(field, bad):
    r = request()
    with pytest.raises(ValueError):
        decode(
            encode_departure_credentials(credentials(r)),
            startup=replace(r.startup, process=replace(r.startup.process, **{field: bad})),
        )


@pytest.mark.parametrize(
    "field,bad", [("launch_nonce", b"a" * 32), ("implementation_sha256", "0" * 64), ("package_root", "/different")]
)
def test_independent_startup_fields(field, bad):
    r = request()
    with pytest.raises(ValueError):
        decode(encode_departure_credentials(credentials(r)), startup=replace(r.startup, **{field: bad}))


@pytest.mark.parametrize(
    "field,bad",
    [
        ("database", "different"),
        ("port", True),
        ("port", 1433.0),
        ("host", "host;bad"),
        ("password", ""),
        ("password", "a\0b"),
        ("password", "x" * 16385),
        ("username", None),
    ],
)
def test_material_drift_invalid_and_forged_revalidated(field, bad):
    value = credentials()
    object.__setattr__(value.connection_material, field, bad)
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_departure_credentials_invalid$"):
        encode_departure_credentials(value)
    data = json.loads(encode_departure_credentials(credentials()))
    data["connection_material"][field] = bad
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_departure_credentials_invalid$"):
        decode(canonical_json_bytes(data))


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("connection_material",),
        ("request",),
        ("request", "plan"),
        ("request", "startup"),
        ("request", "startup", "process"),
    ],
)
@pytest.mark.parametrize("mutation", ["extra", "missing"])
def test_closed_nested_private_shape(path, mutation):
    data = json.loads(encode_departure_credentials(credentials()))
    nested = data
    for key in path:
        nested = nested[key]
    if mutation == "extra":
        nested["secret-canary"] = "secret-canary"
    else:
        del nested[next(iter(nested))]
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_departure_credentials_invalid$"):
        decode(canonical_json_bytes(data))


@pytest.mark.parametrize(
    "mutation",
    ["bytes_type", "oversize", "empty", "whitespace", "reordered", "duplicate", "nested_duplicate", "trailing"],
)
def test_noncanonical_and_limits(mutation):
    raw = encode_departure_credentials(credentials())
    if mutation == "bytes_type":
        raw = bytearray(raw)
    elif mutation == "oversize":
        raw = b"x" * 196609
    elif mutation == "empty":
        raw = b""
    elif mutation == "whitespace":
        raw += b" "
    elif mutation == "reordered":
        raw = json.dumps(json.loads(raw), sort_keys=False, indent=1).encode()
    elif mutation == "duplicate":
        raw = raw[:-1] + b',"schema":"dpone.sqlclient.departure-credentials.v1"}'
    elif mutation == "nested_duplicate":
        raw = raw.replace(b'"port":1433', b'"port":1433,"port":1433')
    else:
        raw += b"{}"
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_departure_credentials_invalid$"):
        decode(raw)


def test_forged_nested_request_and_independent_scalar_alias():
    value = credentials()
    object.__setattr__(value.request.startup.process, "pid", 124.0)
    with pytest.raises(ValueError):
        encode_departure_credentials(value)
    r = request()
    raw = encode_departure_credentials(credentials(r))
    object.__setattr__(r.startup.process, "pid", 124.0)
    with pytest.raises(ValueError):
        decode(raw, startup=r.startup)


@pytest.mark.parametrize("character", ['"', "\\", "\uffff", "\U0010ffff"])
def test_actual_joint_maximum_request_and_password(character):
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
    # U+0001 is allowed password content, expands one UTF8 byte to six JSON bytes.
    material = TdsConnectionMaterial(
        "x" * 255, 65535, departure.database.name, departure.principal.name, "\x01" * 16384
    )
    value = SqlClientDepartureCredentials(request=r, connection_material=material)
    body = encode_departure_credentials(value)
    assert body.count(b"\\u0001") == 16384 and 98304 < len(body) <= 196608
    assert decode(body, r) == value


def test_size_rejection_precedes_parser(monkeypatch):
    from dpone.app import mssql_sqlclient_departure_request as module

    called = []
    monkeypatch.setattr(module, "strict_json_object", lambda *args: called.append(args))
    with pytest.raises(ValueError):
        decode(b"x" * 196609)
    assert not called


def v2_credentials(r=None):
    from dpone.app.mssql_sqlclient_departure_request import SqlClientDepartureCredentialsV2
    from tests.test_mssql_sqlclient_departure_ipc_v2 import request as v2_request

    r = v2_request() if r is None else r
    return SqlClientDepartureCredentialsV2(
        request=r,
        connection_material=TdsConnectionMaterial(
            "synthetic.invalid", 1433, r.plan.database.name, r.plan.observer_admission.login.name, "synthetic-canary"
        ),
    )


def decode_v2(payload, r=None, **changes):
    from dpone.app.mssql_sqlclient_departure_request import decode_departure_credentials_v2
    from tests.test_mssql_sqlclient_departure_ipc_v2 import request as v2_request

    r = v2_request() if r is None else r
    expected = dict(
        startup=r.startup,
        admission_sha256=r.plan.admission_sha256,
        startup_deadline=r.plan.startup_deadline,
        operation_deadline=r.plan.operation_deadline,
        max_address_space_bytes=r.plan.max_address_space_bytes,
    )
    expected.update(changes)
    return decode_departure_credentials_v2(payload, **expected)


def test_v2_credentials_exact_versions_no_fallback():
    from dpone.app.mssql_sqlclient_departure_request import encode_departure_credentials_v2

    old, new = credentials(), v2_credentials()
    assert decode_v2(encode_departure_credentials_v2(new)) == new
    for action in (
        lambda: encode_departure_credentials(new),
        lambda: encode_departure_credentials_v2(old),
        lambda: decode(encode_departure_credentials_v2(new)),
        lambda: decode_v2(encode_departure_credentials(old)),
    ):
        with pytest.raises(ValueError):
            action()
    assert "synthetic-canary" not in repr(new)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("database", "different"),
        ("username", "other"),
        ("port", True),
        ("port", 1433.0),
        ("password", ""),
        ("password", "x" * 16385),
    ],
)
def test_v2_original_material_and_wire_mismatch(field, bad):
    from dpone.app.mssql_sqlclient_departure_request import encode_departure_credentials_v2

    value = v2_credentials()
    data = json.loads(encode_departure_credentials_v2(value))
    object.__setattr__(value.connection_material, field, bad)
    with pytest.raises(ValueError):
        encode_departure_credentials_v2(value)
    data["connection_material"][field] = bad
    with pytest.raises(ValueError):
        decode_v2(canonical_json_bytes(data))


@pytest.mark.parametrize(
    "field,bad",
    [
        ("admission_sha256", "0" * 64),
        ("startup_deadline", 9.0),
        ("operation_deadline", 21.0),
        ("max_address_space_bytes", 1),
    ],
)
def test_v2_independent_launch_binding(field, bad):
    from dpone.app.mssql_sqlclient_departure_request import encode_departure_credentials_v2

    with pytest.raises(ValueError):
        decode_v2(encode_departure_credentials_v2(v2_credentials()), **{field: bad})


@pytest.mark.parametrize(
    "field,bad",
    [
        ("pid", 125),
        ("start_ticks", 457),
        ("host_sha256", "0" * 64),
        ("boot_id", "55555555-5555-4555-8555-555555555555"),
    ],
)
def test_v2_independent_process_binding(field, bad):
    from dpone.app.mssql_sqlclient_departure_request import encode_departure_credentials_v2

    v = v2_credentials()
    with pytest.raises(ValueError):
        decode_v2(
            encode_departure_credentials_v2(v),
            startup=replace(v.request.startup, process=replace(v.request.startup.process, **{field: bad})),
        )


@pytest.mark.parametrize(
    "field,bad", [("launch_nonce", b"a" * 32), ("implementation_sha256", "0" * 64), ("package_root", "/different")]
)
def test_v2_independent_source_binding(field, bad):
    from dpone.app.mssql_sqlclient_departure_request import encode_departure_credentials_v2

    v = v2_credentials()
    with pytest.raises(ValueError):
        decode_v2(encode_departure_credentials_v2(v), startup=replace(v.request.startup, **{field: bad}))


def test_v2_size_rejection_precedes_parser(monkeypatch):
    from dpone.app import mssql_sqlclient_departure_request as module

    called = []
    monkeypatch.setattr(module, "strict_json_object", lambda *args: called.append(args))
    with pytest.raises(ValueError):
        decode_v2(b"x" * 196609)
    assert not called


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("connection_material",),
        ("request",),
        ("request", "plan"),
        ("request", "startup"),
        ("request", "startup", "process"),
        ("request", "plan", "observer_admission"),
    ],
)
@pytest.mark.parametrize("mutation", ["extra", "missing"])
def test_v2_closed_private_nested_shapes(path, mutation):
    from dpone.app.mssql_sqlclient_departure_request import encode_departure_credentials_v2

    data = json.loads(encode_departure_credentials_v2(v2_credentials()))
    nested = data
    for key in path:
        nested = nested[key]
    if mutation == "extra":
        nested["unexpected"] = 1
    else:
        del nested[next(iter(nested))]
    with pytest.raises(ValueError):
        decode_v2(canonical_json_bytes(data))


@pytest.mark.parametrize("mutation", ["duplicate", "nested_duplicate", "trailing", "whitespace", "subclass"])
def test_v2_noncanonical_private_envelope(mutation):
    from dpone.app.mssql_sqlclient_departure_request import encode_departure_credentials_v2

    raw = encode_departure_credentials_v2(v2_credentials())
    if mutation == "duplicate":
        raw = raw[:-1] + b',"schema":"dpone.sqlclient.departure-credentials.v2"}'
    elif mutation == "nested_duplicate":
        raw = raw.replace(b'"port":1433', b'"port":1433,"port":1433')
    elif mutation == "subclass":
        raw = type("Binary", (bytes,), {})(raw)
    else:
        raw += b"{}" if mutation == "trailing" else b" "
    with pytest.raises(ValueError):
        decode_v2(raw)


def observe_credentials():
    from dpone.app.mssql_sqlclient_departure_request import SqlClientObserveDepartureCredentials
    from tests.test_mssql_sqlclient_observe_departure_contract import request as observe_request

    r = observe_request()
    return SqlClientObserveDepartureCredentials(
        request=r,
        connection_material=replace(credentials().connection_material, username=r.plan.observer_admission.login.name),
    )


def decode_observe(payload, r=None, **changes):
    from dpone.app.mssql_sqlclient_departure_request import decode_observe_departure_credentials

    r = observe_credentials().request if r is None else r
    expected = dict(
        startup=r.startup,
        admission_sha256=r.plan.admission_sha256,
        startup_deadline=r.plan.startup_deadline,
        operation_deadline=r.plan.operation_deadline,
        max_address_space_bytes=r.plan.max_address_space_bytes,
        observer_admission=r.plan.observer_admission,
    )
    expected.update(changes)
    return decode_observe_departure_credentials(payload, **expected)


def test_observe_exact_private_roundtrip():
    from dpone.app.mssql_sqlclient_departure_request import encode_observe_departure_credentials
    from dpone.contracts.mssql_sqlclient_observe_departure_codec import encode_observe_departure_request

    value = observe_credentials()
    raw = encode_observe_departure_credentials(value)
    expected = (
        b'{"connection_material":{"database":"target","host":"synthetic.invalid","password":"secret-canary","port":1433,"username":"writer"},"request":'
        + encode_observe_departure_request(value.request)
        + b',"schema":"dpone.sqlclient.observe-departure-credentials.v1"}'
    )
    assert raw == expected and decode_observe(raw) == value
    assert "secret-canary" not in repr(value) and "synthetic.invalid" not in repr(value)
    for old_decode in (decode, decode_v2):
        with pytest.raises(ValueError):
            old_decode(raw)


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("connection_material",),
        ("request",),
        ("request", "plan"),
        ("request", "startup"),
        ("request", "startup", "process"),
        ("request", "plan", "observer_admission"),
    ],
)
@pytest.mark.parametrize("mutation", ["extra", "missing", "wrong_type"])
def test_observe_closed_shapes(path, mutation):
    from dpone.app.mssql_sqlclient_departure_request import encode_observe_departure_credentials

    data = json.loads(encode_observe_departure_credentials(observe_credentials()))
    nested = data
    for key in path:
        nested = nested[key]
    if mutation == "missing":
        del nested[next(iter(nested))]
    else:
        nested["secret-canary" if mutation == "extra" else next(iter(nested))] = ["secret-canary"]
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_departure_credentials_invalid$"):
        decode_observe(canonical_json_bytes(data))


@pytest.mark.parametrize(
    "field,bad",
    [
        ("database", "other"),
        ("username", "creator"),
        ("host", "host;bad"),
        ("host", "x" * 256),
        ("port", True),
        ("port", 1433.0),
        ("port", 0),
        ("port", 65536),
        ("password", ""),
        ("password", "x\0y"),
        ("password", "x" * 16385),
        ("password", "😀" * 4097),
    ],
)
def test_observe_material_raw_and_wire_revalidated(field, bad):
    from dpone.app.mssql_sqlclient_departure_request import encode_observe_departure_credentials

    v = observe_credentials()
    data = json.loads(encode_observe_departure_credentials(v))
    data["connection_material"][field] = bad
    object.__setattr__(v.connection_material, field, bad)
    for action in (lambda: encode_observe_departure_credentials(v), lambda: decode_observe(canonical_json_bytes(data))):
        with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_departure_credentials_invalid$") as exc:
            action()
        assert exc.value.__cause__ is None


def test_observe_all_original_scalar_aliases_and_independent_bindings():
    from dpone.app.mssql_sqlclient_departure_request import encode_observe_departure_credentials
    from tests.mssql_sqlclient_departure_v2_fixtures import alias, corrupt, leaves

    v = observe_credentials()
    raw = encode_observe_departure_credentials(v)
    for path, leaf in leaves(v.request):
        forged = observe_credentials()
        object.__setattr__(forged, "request", corrupt(forged.request, path, alias(leaf)))
        with pytest.raises(ValueError):
            encode_observe_departure_credentials(forged)
    expected = dict(startup=v.request.startup, observer_admission=v.request.plan.observer_admission)
    for key, value in expected.items():
        for path, leaf in leaves(value):
            with pytest.raises(ValueError):
                decode_observe(raw, **{key: corrupt(value, path, alias(leaf))})
    for key, value in [
        ("admission_sha256", "0" * 64),
        ("startup_deadline", 9.0),
        ("operation_deadline", 21.0),
        ("max_address_space_bytes", 1),
    ]:
        with pytest.raises(ValueError):
            decode_observe(raw, **{key: value})
    a = v.request.plan.observer_admission
    with pytest.raises(ValueError):
        decode_observe(raw, observer_admission=replace(a, login=replace(a.login, name="independently-configured")))


@pytest.mark.parametrize("mutation", ["empty", "duplicate", "nested_duplicate", "space", "trailing", "alias", "schema"])
def test_observe_canonical_private_bytes(mutation):
    from dpone.app.mssql_sqlclient_departure_request import encode_observe_departure_credentials

    raw = encode_observe_departure_credentials(observe_credentials())
    if mutation == "empty":
        raw = b""
    elif mutation == "duplicate":
        raw = raw[:-1] + b',"schema":"secret-canary"}'
    elif mutation == "nested_duplicate":
        raw = raw.replace(b'"port":1433', b'"port":1433,"port":1433')
    elif mutation == "alias":
        raw = type("Binary", (bytes,), {})(raw)
    elif mutation == "schema":
        raw = raw.replace(b"observe-departure-credentials.v1", b"departure-credentials.v2")
    else:
        raw += b" " if mutation == "space" else b"{}"
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_departure_credentials_invalid$"):
        decode_observe(raw)


def test_observe_real_escaped_password_boundary_and_no_private_hash(monkeypatch):
    from dpone.app.mssql_sqlclient_departure_request import encode_observe_departure_credentials
    from dpone.contracts import mssql_sqlclient_observe_departure_codec as codec

    def no_hash(*args):
        pytest.fail("private encoding must not invoke evidence/hash producers")

    monkeypatch.setattr(codec, "sha256", no_hash)
    v = observe_credentials()
    v = replace(
        v, connection_material=replace(v.connection_material, host="x" * 255, port=65535, password="\x01" * 16384)
    )
    raw = encode_observe_departure_credentials(v)
    assert raw.count(b"\\u0001") == 16384 and 98304 < len(raw) <= 196608
    assert decode_observe(raw) == v


@pytest.mark.parametrize("size", [196608, 196609])
def test_observe_envelope_cap_before_parser(monkeypatch, size):
    from dpone.app import mssql_sqlclient_departure_request as module

    calls = []

    def parser(raw):
        calls.append(len(raw))
        raise ValueError

    monkeypatch.setattr(module, "strict_json_object", parser)
    with pytest.raises(ValueError):
        decode_observe(b" " * size)
    assert calls == ([size] if size == 196608 else [])


@pytest.mark.parametrize(
    "field,bad", [("launch_nonce", b"a" * 32), ("implementation_sha256", "0" * 64), ("package_root", "/different")]
)
def test_observe_independent_startup_values(field, bad):
    from dpone.app.mssql_sqlclient_departure_request import encode_observe_departure_credentials

    v = observe_credentials()
    with pytest.raises(ValueError):
        decode_observe(encode_observe_departure_credentials(v), startup=replace(v.request.startup, **{field: bad}))


@pytest.mark.parametrize(
    "field,bad",
    [
        ("pid", 125),
        ("start_ticks", 457),
        ("host_sha256", "0" * 64),
        ("boot_id", "55555555-5555-4555-8555-555555555555"),
    ],
)
def test_observe_independent_process_values(field, bad):
    from dpone.app.mssql_sqlclient_departure_request import encode_observe_departure_credentials

    v = observe_credentials()
    with pytest.raises(ValueError):
        decode_observe(
            encode_observe_departure_credentials(v),
            startup=replace(v.request.startup, process=replace(v.request.startup.process, **{field: bad})),
        )


@pytest.mark.parametrize("size", [65536, 65537])
def test_observe_nested_request_cap_precedes_request_parser(monkeypatch, size):
    from dpone.app.mssql_sqlclient_departure_request import encode_observe_departure_credentials
    from dpone.contracts import mssql_sqlclient_observe_departure_codec as codec

    body = json.loads(encode_observe_departure_credentials(observe_credentials()))
    body["request"]["padding"] = ""
    body["request"]["padding"] = "x" * (size - len(canonical_json_bytes(body["request"])))
    assert len(canonical_json_bytes(body["request"])) == size
    calls = []

    def parser(raw):
        calls.append(len(raw))
        raise ValueError

    monkeypatch.setattr(codec, "strict_json_object", parser)
    with pytest.raises(ValueError):
        decode_observe(canonical_json_bytes(body))
    assert calls == ([size] if size == 65536 else [])
