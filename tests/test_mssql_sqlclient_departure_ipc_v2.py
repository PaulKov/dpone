"""Pure v2 IPC consistency; synthetic records never certify SQL or process origin."""

# Every scalar is attacked independently, including aliases whose deepcopy
# returns a plain string. Validation must precede request hashing/projection.
import json
import math
from dataclasses import fields, replace
from hashlib import sha256

import pytest

from dpone.contracts import mssql_sqlclient_departure_ipc_codec as v1
from dpone.contracts import mssql_sqlclient_departure_ipc_v2_codec as v2
from dpone.contracts.mssql_sqlclient_departure_ipc_v2 import (
    SqlClientDeparturePlanV2,
    SqlClientDepartureRequestV2,
    validate_departure_observer_binding_v2,
    validate_departure_request_binding_v2,
)
from dpone.contracts.mssql_sqlclient_departure_ipc_v2_codec import (
    decode_departure_request_v2,
    decode_departure_result_v2,
    encode_departure_request_v2,
    encode_departure_result_v2,
    make_departure_result_v2,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation import observer_incarnation_digest
from dpone.contracts.strict_json import canonical_json_bytes
from tests.mssql_sqlclient_departure_v2_fixtures import alias, corrupt, leaves, sample
from tests.test_mssql_sqlclient_departure_ipc import GOLDEN_REQUEST
from tests.test_mssql_sqlclient_departure_ipc import request as legacy_request


def request(departure=None):
    departure = sample() if departure is None else departure
    old = legacy_request(departure)
    plan = SqlClientDeparturePlanV2(
        **{f.name: getattr(old.plan, f.name) for f in fields(old.plan) if f.name != "schema"},
        observer_admission=departure.admission,
    )
    return SqlClientDepartureRequestV2(plan=plan, startup=old.startup)


def test_exact_v2_roundtrip_and_request_digest():
    r = request()
    raw = encode_departure_request_v2(r)
    assert decode_departure_request_v2(raw) == r
    result = make_departure_result_v2(r, sample())
    assert result.request_sha256 == sha256(raw).hexdigest()
    assert decode_departure_result_v2(encode_departure_result_v2(result, request=r), request=r) == result


@pytest.mark.parametrize("path,value", list(leaves(request())))
def test_every_original_request_leaf_before_hash(monkeypatch, path, value):
    malformed = corrupt(request(), path, alias(value))
    calls = []
    monkeypatch.setattr(v2, "sha256", lambda *args: calls.append(args))
    with pytest.raises(ValueError):
        v2.departure_request_digest_v2(malformed)
    assert calls == []


@pytest.mark.parametrize("path,value", list(leaves(sample())))
def test_every_original_result_leaf(monkeypatch, path, value):
    r, malformed = request(), corrupt(sample(), path, alias(value))
    calls = []
    monkeypatch.setattr(v2, "sha256", lambda *args: calls.append(args))
    with pytest.raises(ValueError):
        v2.make_departure_result_v2(r, malformed)
    assert calls == []


def bind(r, **changes):
    expected = dict(
        startup=r.startup,
        admission_sha256=r.plan.admission_sha256,
        startup_deadline=r.plan.startup_deadline,
        operation_deadline=r.plan.operation_deadline,
        max_address_space_bytes=r.plan.max_address_space_bytes,
    )
    expected.update(changes)
    validate_departure_request_binding_v2(r, **expected)


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
def test_independent_launch_binding(field, value):
    bind(request())
    with pytest.raises(ValueError):
        bind(request(), **{field: value})


@pytest.mark.parametrize("path,value", list(leaves(request().startup)))
def test_independent_startup_aliases(path, value):
    with pytest.raises(ValueError):
        bind(request(), startup=corrupt(request().startup, path, alias(value)))


@pytest.mark.parametrize("path,value", list(leaves(request().plan.observer_admission)))
def test_independent_observer_aliases(path, value):
    with pytest.raises(ValueError):
        validate_departure_observer_binding_v2(
            request(), observer_admission=corrupt(request().plan.observer_admission, path, alias(value))
        )


def test_observer_expectation_is_mandatory_and_exact():
    plan = request().plan
    with pytest.raises(TypeError):
        SqlClientDeparturePlanV2(
            **{f.name: getattr(plan, f.name) for f in fields(plan) if f.name != "observer_admission"}
        )
    for invalid in (None, sample().observer.authority):
        with pytest.raises(ValueError):
            replace(plan, observer_admission=invalid)
    for group in ("server", "database"):
        original = getattr(plan.observer_admission, group)
        changes = {"machine_name": "different"} if group == "server" else {"owner_sid": "cc"}
        with pytest.raises(ValueError):
            replace(plan, observer_admission=replace(plan.observer_admission, **{group: replace(original, **changes)}))


def test_independently_configured_login_and_actual_authority():
    d = sample()
    expected = replace(d.admission, login=replace(d.admission.login, name="observer", original_name="observer"))
    r = replace(request(d), plan=replace(request(d).plan, observer_admission=expected))
    bind(r)
    validate_departure_observer_binding_v2(r, observer_admission=expected)
    with pytest.raises(ValueError):
        validate_departure_observer_binding_v2(r, observer_admission=d.admission)
    assert v2.departure_request_digest_v2(r) != v2.departure_request_digest_v2(request(d))
    with pytest.raises(ValueError):
        make_departure_result_v2(r, d)
    own = replace(d.observer, authority=replace(d.observer.authority, login=expected.login))
    digest = observer_incarnation_digest(own)
    actual = replace(
        d, observer=own, samples=tuple(replace(s, before_sha256=digest, after_sha256=digest) for s in d.samples)
    )
    result = make_departure_result_v2(r, actual)
    assert decode_departure_result_v2(encode_departure_result_v2(result, request=r), request=r) == result


def wire(kind, r):
    if kind == "plan":
        return v2.encode_departure_plan_v2(r.plan), v2.decode_departure_plan_v2
    if kind == "request":
        return encode_departure_request_v2(r), decode_departure_request_v2
    return encode_departure_result_v2(
        make_departure_result_v2(r, sample()), request=r
    ), lambda raw: decode_departure_result_v2(raw, request=r)


@pytest.mark.parametrize("kind", ["plan", "request", "result"])
@pytest.mark.parametrize("mutation", ["missing", "extra", "duplicate", "whitespace", "trailing", "v1schema", "mixed"])
def test_closed_canonical_wire(kind, mutation):
    raw, decode = wire(kind, request())
    data = json.loads(raw)
    if mutation == "missing":
        del data["schema"]
    elif mutation == "extra":
        data["unknown"] = None
    elif mutation == "v1schema":
        data["schema"] = data["schema"].replace(".v2", ".v1")
    elif mutation == "mixed":
        if kind == "request":
            data["plan"] = json.loads(v1.encode_departure_plan(legacy_request().plan))
        elif kind == "result":
            data["departure"]["schema"] = "dpone.sqlclient.create-departure.v1"
        else:
            data.pop("observer_admission")
    if mutation == "duplicate":
        raw = raw[:-1] + b',"schema":' + json.dumps(data["schema"]).encode() + b"}"
    elif mutation == "whitespace":
        raw += b" "
    elif mutation == "trailing":
        raw += b"{}"
    else:
        raw = canonical_json_bytes(data)
    with pytest.raises(ValueError):
        decode(raw)


@pytest.mark.parametrize("kind", ["plan", "request", "result"])
@pytest.mark.parametrize("bad", ["empty", "over", "bytearray", "subclass"])
def test_caps_and_original_bytes_before_parser(monkeypatch, kind, bad):
    raw, decode = wire(kind, request())

    class Binary(bytes):
        pass

    malformed = {
        "empty": b"",
        "over": b"x" * (32769 if kind == "result" else 65537),
        "bytearray": bytearray(raw),
        "subclass": Binary(raw),
    }[bad]
    calls = []
    monkeypatch.setattr(v2, "strict_json_object", lambda *args: calls.append(args))
    with pytest.raises(ValueError):
        decode(malformed)
    assert calls == []


@pytest.mark.parametrize("kind", ["plan", "request", "result"])
def test_v1_v2_boundaries_reject_both_objects_and_bytes(kind):
    old = legacy_request()
    new = request()
    from tests.test_mssql_sqlclient_create_departure_codec import sample as old_sample

    old_value = {"plan": old.plan, "request": old, "result": v1.make_departure_result(old, old_sample())}[kind]
    new_value = {"plan": new.plan, "request": new, "result": make_departure_result_v2(new, sample())}[kind]
    old_encode = getattr(v1, "encode_departure_" + kind)
    old_decode = getattr(v1, "decode_departure_" + kind)
    new_encode = getattr(v2, "encode_departure_" + kind + "_v2")
    new_decode = getattr(v2, "decode_departure_" + kind + "_v2")
    old_kw = {"request": old} if kind == "result" else {}
    new_kw = {"request": new} if kind == "result" else {}
    for call in (
        lambda: old_encode(new_value, **old_kw),
        lambda: new_encode(old_value, **new_kw),
        lambda: old_decode(new_encode(new_value, **new_kw), **old_kw),
        lambda: new_decode(old_encode(old_value, **old_kw), **new_kw),
    ):
        with pytest.raises(ValueError):
            call()
    assert v1.encode_departure_request(old) == GOLDEN_REQUEST


def maximum_request(character):
    d = sample(character=character)
    r = request(d)
    attempt = replace(
        r.plan.attempt,
        target_key=character * 256,
        run_id=character * 256,
        schema=d.database.name,
        table=d.database.name,
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
    return replace(
        r, plan=plan, startup=replace(r.startup, package_root=root, process=replace(process, pid=2**31 - 2))
    ), d


@pytest.mark.parametrize("character", ['"', "\\", "\uffff", "\U0010ffff"])
def test_actual_maximum_encoding(character):
    r, d = maximum_request(character)
    p = v2.encode_departure_plan_v2(r.plan)
    raw = encode_departure_request_v2(r)
    result = make_departure_result_v2(r, d)
    body = encode_departure_result_v2(result, request=r)
    assert len(r.plan.package_root.encode("utf8")) == 4096
    assert len(p) <= 65536 and len(raw) <= 65536 and len(body) <= 32768
    expected = {
        '"': (21405, 30093, 9685),
        "\\": (21405, 30093, 9685),
        "\uffff": (21406, 25999, 11861),
        "\U0010ffff": (19870, 24463, 9685),
    }
    assert (len(p), len(raw), len(body)) == expected[character]
    assert v2.decode_departure_plan_v2(p) == r.plan
    assert decode_departure_request_v2(raw) == r
    assert decode_departure_result_v2(body, request=r) == result
    bind(r)


@pytest.mark.parametrize("group", ["server", "database", "login", "transport"])
@pytest.mark.parametrize("mutation", ["missing", "unknown", "duplicate"])
def test_closed_observer_components(group, mutation):
    raw = v2.encode_departure_plan_v2(request().plan)
    data = json.loads(raw)
    component = data["observer_admission"][group]
    field = next(iter(component))
    if mutation == "missing":
        component.pop(field)
    elif mutation == "unknown":
        component["unknown"] = 0
    else:
        token = canonical_json_bytes(component)
        duplicate = token[:-1] + b"," + canonical_json_bytes({field: component[field]})[1:]
        # Both equal admission copies are mutated: duplicate detection is recursive.
        raw = raw.replace(token, duplicate)
    if mutation != "duplicate":
        raw = canonical_json_bytes(data)
    with pytest.raises(ValueError):
        v2.decode_departure_plan_v2(raw)


@pytest.mark.parametrize(
    "group,changes",
    [
        ("login", {"principal_id": 301}),
        ("login", {"name": "observer", "original_name": "observer"}),
        ("login", {"sid": "cc", "original_sid": "cc"}),
        ("login", {"authenticating_database_id": 1}),
        ("login", {"is_sysadmin": True}),
        ("transport", {"encrypt_option": "FALSE"}),
    ],
)
def test_each_variable_observer_expectation_binds_digest_and_result(group, changes):
    r = request()
    a = r.plan.observer_admission
    altered = replace(a, **{group: replace(getattr(a, group), **changes)})
    changed = replace(r, plan=replace(r.plan, observer_admission=altered))
    assert v2.departure_request_digest_v2(r) != v2.departure_request_digest_v2(changed)
    body = encode_departure_result_v2(make_departure_result_v2(r, sample()), request=r)
    with pytest.raises(ValueError):
        make_departure_result_v2(changed, sample())
    with pytest.raises(ValueError):
        decode_departure_result_v2(body, request=changed)


@pytest.mark.parametrize("kind", ["plan", "request", "result"])
def test_record_subclasses_rejected(kind):
    r = request()
    value = {"plan": r.plan, "request": r, "result": make_departure_result_v2(r, sample())}[kind]
    derived = type("Derived", (type(value),), {})
    invalid = derived(**{f.name: getattr(value, f.name) for f in fields(value)})
    with pytest.raises(ValueError):
        getattr(v2, "encode_departure_" + kind + "_v2")(invalid, **({"request": r} if kind == "result" else {}))


def test_result_requires_original_request_and_creator_groups():
    r = request()
    result = make_departure_result_v2(r, sample())
    for changed in (
        legacy_request(),
        corrupt(r, ("startup", "process", "pid"), 124.0),
        replace(r, plan=replace(r.plan, helper_id=type(r.plan.helper_id)(int=999))),
    ):
        with pytest.raises(ValueError):
            encode_departure_result_v2(result, request=changed)
        with pytest.raises(ValueError):
            decode_departure_result_v2(encode_departure_result_v2(result, request=r), request=changed)
    with pytest.raises(ValueError):
        make_departure_result_v2(r, sample(character="x"))
    with pytest.raises(ValueError):
        encode_departure_result_v2(replace(result, request_sha256="0" * 64), request=r)


@pytest.mark.parametrize(
    "field,value",
    [
        ("package_root", "relative"),
        ("package_root", "/" + "x" * 4096),
        ("max_address_space_bytes", 0),
        ("max_address_space_bytes", 2**63),
        ("startup_deadline", 0.0),
        ("operation_deadline", math.inf),
        ("operation_deadline", 1e20),
        ("startup_deadline", 1e-12),
    ],
)
def test_original_plan_bounds(field, value):
    with pytest.raises(ValueError):
        replace(request().plan, **{field: value})


@pytest.mark.parametrize("kind,limit", [("plan", 65536), ("request", 65536), ("result", 32768)])
def test_exact_cap_reaches_parser(monkeypatch, kind, limit):
    _, decode = wire(kind, request())
    calls = []

    def rejecting_parser(raw):
        calls.append(len(raw))
        raise ValueError

    monkeypatch.setattr(v2, "strict_json_object", rejecting_parser)
    with pytest.raises(ValueError):
        decode(b"x" * limit)
    assert calls == [limit]
