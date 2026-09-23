"""Raw/own partition commitments never masquerade as legacy literal-zero sweeps."""

import json
from dataclasses import fields, replace
from datetime import timedelta
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_create_departure_codec import decode_create_departure, encode_create_departure
from dpone.contracts.mssql_sqlclient_create_departure_v2_codec import (
    decode_create_departure_v2,
    encode_create_departure_v2,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation import observer_incarnation_digest
from dpone.contracts.mssql_sqlclient_observer_incarnation_codec import encode_observer_incarnation
from dpone.contracts.strict_json import canonical_json_bytes
from tests.mssql_sqlclient_departure_v2_fixtures import (
    DEPARTURE_GOLDEN,
    DEPARTURE_GOLDEN_SHA256,
    alias,
    corrupt,
    leaves,
    sample,
)
from tests.test_mssql_sqlclient_create_departure_codec import sample as legacy_sample


@pytest.mark.parametrize("reused", [False, True])
def test_exact_roundtrip_and_raw_partitions(reused):
    value = sample(reused=reused)
    raw = encode_create_departure_v2(value)
    assert decode_create_departure_v2(raw) == value
    assert b'"counts"' not in raw
    assert [s.raw_count for s in value.samples] == ([1, 1, 1, 0, 1, 1] if reused else [0] * 6)


@pytest.mark.parametrize("path,value", list(leaves(sample())))
@pytest.mark.parametrize("integer_enum", [False, True])
def test_recursive_original_aliases_before_hash_or_projection(path, value, monkeypatch, integer_enum):
    import dpone.contracts.mssql_sqlclient_create_departure_v2_codec as codec
    import dpone.contracts.mssql_sqlclient_observer_incarnation as own_model
    import dpone.contracts.mssql_tds_session as session

    changed = corrupt(sample(), path, alias(value, enum=integer_enum))
    effects = []
    for module, name in (
        (own_model, "sha256"),
        (session.hashlib, "sha256"),
        (codec, "asdict"),
        (codec, "canonical_json_bytes"),
    ):
        original = getattr(module, name)

        def track(*args, action=original, label=name, **kw):
            effects.append(label)
            return action(*args, **kw)

        monkeypatch.setattr(module, name, track)
    with pytest.raises(ValueError, match="^mssql_native.sqlclient_create_departure_v2_invalid$"):
        encode_create_departure_v2(changed)
    assert effects == []


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("observer",),
        ("observer", "visibility"),
        ("observer", "authority"),
        ("observer", "authority", "server"),
        ("observer", "authority", "principal_resolution"),
        ("admission",),
        ("principal",),
        ("original",),
        ("database",),
        ("samples", 0),
        ("samples", 2, "request"),
    ],
)
def test_exact_dto_types(path):
    value = sample()
    node = value
    for key in path:
        node = node[key] if type(key) is int else getattr(node, key)
    cls = type("Alias", (type(node),), {})
    nested = cls(**{f.name: getattr(node, f.name) for f in fields(node)})
    if not path:
        changed = nested
    elif type(path[-1]) is int:
        samples = list(value.samples)
        samples[path[-1]] = nested
        # Constructor itself must reject the exact-class alias.
        with pytest.raises(ValueError):
            replace(value, samples=tuple(samples))
        return
    else:
        changed = corrupt(value, path, nested)
    with pytest.raises(ValueError):
        encode_create_departure_v2(changed)


@pytest.mark.parametrize(
    "index,field,replacement",
    [
        (0, "raw_count", 2),
        (1, "own_count", 0),
        (2, "original_uuid_count", 1),
        (3, "raw_count", 1),
        (4, "original_uuid_count", 1),
        (5, "raw_count", 2),
    ],
)
def test_unrelated_or_original_survivor_cannot_be_hidden(index, field, replacement):
    value = sample()
    changed = list(value.samples)
    changed[index] = replace(changed[index], **{field: replacement})
    with pytest.raises(ValueError):
        replace(value, samples=tuple(changed))


@pytest.mark.parametrize("index", range(6))
@pytest.mark.parametrize("guard", ["before_sha256", "after_sha256"])
def test_all_twelve_guards_and_restored_drift_reject(index, guard):
    value = sample()
    changed = list(value.samples)
    changed[index] = replace(changed[index], **{guard: "a" * 64})
    with pytest.raises(ValueError):
        replace(value, samples=tuple(changed))


@pytest.mark.parametrize("mutation", ["short", "long", "list", "order", "deduplicated"])
def test_exact_six_distinct_positions(mutation):
    value = sample()
    replacements = {
        "short": value.samples[:-1],
        "long": value.samples + (value.samples[0],),
        "list": list(value.samples),
        "order": tuple(reversed(value.samples)),
        "deduplicated": value.samples[:4],
    }
    with pytest.raises(ValueError):
        replace(value, samples=replacements[mutation])


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("connection_id", UUID(int=3)),
        ("session_id", 1),
        ("start_time", sample().observer.login_time - timedelta(microseconds=1)),
    ],
)
def test_own_request_must_match_incarnation_and_login(field, replacement):
    value = sample()
    samples = list(value.samples)
    samples[2] = replace(samples[2], request=replace(samples[2].request, **{field: replacement}))
    with pytest.raises(ValueError):
        replace(value, samples=tuple(samples))


@pytest.mark.parametrize(
    "field,value",
    [("session_id", 0), ("request_id", -1), ("request_id", 2**31), ("connection_id", None), ("session_id", True)],
)
def test_invalid_request_local_fields(field, value):
    with pytest.raises(ValueError):
        replace(sample().samples[2].request, **{field: value})


@pytest.mark.parametrize(
    "index,field,value",
    [
        (0, "request", sample().samples[2].request),
        (2, "request", None),
        (3, "original_uuid_count", 0),
        (0, "original_uuid_count", None),
        (0, "raw_count", 2**63),
        (0, "raw_count", -1),
        (0, "own_count", 2),
        (0, "original_uuid_count", 2),
        (0, "before_sha256", "A" * 64),
    ],
)
def test_invalid_local_sample_shape(index, field, value):
    with pytest.raises(ValueError):
        replace(sample().samples[index], **{field: value})


def rebinding(value, own):
    digest = observer_incarnation_digest(own)
    return replace(
        value, observer=own, samples=tuple(replace(s, before_sha256=digest, after_sha256=digest) for s in value.samples)
    )


def test_different_valid_observer_login_and_principal_allowed():
    value = sample()
    authority = value.observer.authority
    changed = replace(
        authority,
        login=replace(authority.login, name="observer", original_name="observer", sid="cc", original_sid="cc"),
        principal_resolution=replace(authority.principal_resolution, name="observer_user", sid="cc"),
    )
    result = rebinding(value, replace(value.observer, authority=changed))
    assert result.principal == value.principal and result.original == value.original
    assert decode_create_departure_v2(encode_create_departure_v2(result)) == result


@pytest.mark.parametrize("mutation", ["server", "database", "same_uuid", "connect_equal", "login_equal", "backward"])
def test_original_and_own_cross_bindings(mutation):
    value = sample()
    own = value.observer
    if mutation == "server":
        own = replace(own, authority=replace(own.authority, server=replace(own.authority.server, server_name="other")))
    elif mutation == "database":
        own = replace(
            own, authority=replace(own.authority, database=replace(own.authority.database, database_name="other"))
        )
    elif mutation == "same_uuid":
        own = replace(own, connection_id=value.original.connection_id)
    else:
        stamp = value.original.connect_time if mutation == "connect_equal" else value.original.login_time
        if mutation == "backward":
            stamp -= timedelta(seconds=1)
        own = replace(own, connect_time=stamp, login_time=stamp)
    with pytest.raises(ValueError):
        rebinding(value, own)


def test_literal_golden_and_v1_coexistence_without_conversion():
    value = sample()
    assert encode_create_departure_v2(value) == DEPARTURE_GOLDEN
    assert sha256(DEPARTURE_GOLDEN).hexdigest() == DEPARTURE_GOLDEN_SHA256
    assert decode_create_departure_v2(DEPARTURE_GOLDEN) == value
    for operation, argument in (
        (encode_create_departure, value),
        (decode_create_departure, DEPARTURE_GOLDEN),
        (encode_create_departure_v2, legacy_sample()),
        (decode_create_departure_v2, encode_create_departure(legacy_sample())),
    ):
        with pytest.raises(ValueError):
            operation(argument)


@pytest.mark.parametrize(
    "character,own,different,reused",
    [
        ("x", 2777, 7227, 7364),
        ('"', 3801, 9403, 9540),
        ("\\", 3801, 9403, 9540),
        ("\uffff", 4825, 11579, 11716),
        ("\U0010ffff", 3801, 9403, 9540),
    ],
)
@pytest.mark.parametrize("same_spid", [False, True])
def test_actual_implemented_joint_maximum(character, own, different, reused, same_spid):
    value = sample(character=character, reused=same_spid)
    body = encode_create_departure_v2(value)
    assert len(encode_observer_incarnation(value.observer)) == own
    assert len(body) == (reused if same_spid else different) < 16384
    assert decode_create_departure_v2(body) == value


@pytest.mark.parametrize(
    "mutation",
    [
        "extra",
        "missing_null",
        "kind",
        "bool",
        "float",
        "request_extra",
        "request_time",
        "guard_case",
        "duplicate",
        "nested_duplicate",
        "spaces",
        "escaped",
        "schema",
    ],
)
def test_canonical_wire_rejection(mutation):
    data = json.loads(DEPARTURE_GOLDEN)
    if mutation == "extra":
        data["counts"] = [0] * 6
    elif mutation == "missing_null":
        del data["samples"][0]["request"]
    elif mutation == "kind":
        data["samples"][0]["kind"] = "CONNECTIONS"
    elif mutation == "bool":
        data["samples"][0]["raw_count"] = True
    elif mutation == "float":
        data["samples"][0]["raw_count"] = 1.0
    elif mutation == "request_extra":
        data["samples"][2]["request"]["sql_text"] = "SELECT"
    elif mutation == "request_time":
        data["samples"][2]["request"]["start_time"] = "9999-12-31 23:59:59.999999"
    elif mutation == "guard_case":
        data["samples"][0]["before_sha256"] = data["samples"][0]["before_sha256"].upper()
    elif mutation == "schema":
        data["schema"] = "dpone.sqlclient.create-departure.v1"
    body = canonical_json_bytes(data)
    if mutation == "duplicate":
        body = body.replace(b"{", b'{"samples":[],', 1)
    elif mutation == "nested_duplicate":
        body = body.replace(b'"raw_count":1', b'"raw_count":1,"raw_count":1', 1)
    elif mutation == "spaces":
        body = b" " + body
    elif mutation == "escaped":
        body = body.replace(b"writer", b"\\u0077riter")
    with pytest.raises(ValueError, match="^mssql_native.sqlclient_create_departure_v2_invalid$"):
        decode_create_departure_v2(body)


@pytest.mark.parametrize("body", [b"", b"x" * 16385, bytearray(b"{}"), alias(b"{}")])
def test_bound_before_parser(body, monkeypatch):
    import dpone.contracts.mssql_sqlclient_create_departure_v2_codec as codec

    monkeypatch.setattr(codec, "strict_json_object", lambda body: pytest.fail("premature parse"))
    with pytest.raises(ValueError):
        decode_create_departure_v2(body)


def test_exact_cap_invalid_body_does_not_become_valid_metadata():
    with pytest.raises(ValueError):
        decode_create_departure_v2(b" " * 16384)


def test_one_full_observer_and_exactly_twelve_guard_commitments():
    value = sample()
    body = encode_create_departure_v2(value)
    digest = observer_incarnation_digest(value.observer).encode("ascii")
    assert body.count(b'"schema":"dpone.sqlclient.observer-incarnation.v2"') == 1
    assert body.count(digest) == 12
    assert body.count(b'"before_sha256"') == body.count(b'"after_sha256"') == 6


@pytest.mark.parametrize("size", [0, 16385])
def test_encode_cap_stays_fixed(monkeypatch, size):
    import dpone.contracts.mssql_sqlclient_create_departure_v2_codec as codec

    value = sample()
    monkeypatch.setattr(codec, "canonical_json_bytes", lambda data: b"x" * size)
    with pytest.raises(ValueError, match="^mssql_native.sqlclient_create_departure_v2_invalid$"):
        encode_create_departure_v2(value)


def test_decoder_preserves_process_control(monkeypatch):
    import dpone.contracts.mssql_sqlclient_create_departure_v2_codec as codec

    def interrupt(body):
        raise KeyboardInterrupt

    monkeypatch.setattr(codec, "strict_json_object", interrupt)
    with pytest.raises(KeyboardInterrupt):
        decode_create_departure_v2(DEPARTURE_GOLDEN)
