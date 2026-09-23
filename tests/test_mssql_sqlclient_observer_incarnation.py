"""Own-incarnation canonical bytes and strict originals, without SQL provenance."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_observer_incarnation import (
    SqlClientDepartureVisibilityV2,
    observer_incarnation_digest,
)
from dpone.contracts.mssql_sqlclient_observer_incarnation_codec import (
    decode_observer_incarnation,
    encode_observer_incarnation,
)
from tests.mssql_sqlclient_departure_v2_fixtures import (
    OWN_GOLDEN,
    OWN_GOLDEN_SHA256,
    OWN_SEMANTIC_SHA256,
    alias,
    corrupt,
    leaves,
    sample,
)


def test_roundtrip_and_domain_separated_digest():
    observer = sample().observer
    raw = encode_observer_incarnation(observer)
    assert decode_observer_incarnation(raw) == observer
    assert (
        observer_incarnation_digest(observer) == sha256(b"dpone.sqlclient.observer-incarnation.v2\0" + raw).hexdigest()
    )


@pytest.mark.parametrize("path,value", list(leaves(sample().observer)))
@pytest.mark.parametrize("integer_enum", [False, True])
def test_every_original_leaf_alias_rejected_before_projection_and_hash(path, value, integer_enum, monkeypatch):
    import dpone.contracts.mssql_sqlclient_observer_incarnation as model

    changed = corrupt(sample().observer, path, alias(value, enum=integer_enum))
    effects = []
    for name in ("asdict", "canonical_json_bytes", "sha256"):
        original = getattr(model, name)

        def track(*args, action=original, label=name, **kw):
            effects.append(label)
            return action(*args, **kw)

        monkeypatch.setattr(model, name, track)
    for operation in (encode_observer_incarnation, observer_incarnation_digest):
        with pytest.raises(ValueError, match="^mssql_native.sqlclient_observer_incarnation_invalid$"):
            operation(changed)
    assert effects == []


@pytest.mark.parametrize("path,value", list(leaves(sample().observer)))
def test_each_context_leaf_changes_digest_or_is_a_fixed_constraint(path, value):
    from enum import Enum

    if type(value) is datetime:
        replacement = value - timedelta(microseconds=1)
    elif type(value) is UUID:
        replacement = UUID(int=3)
    elif type(value) is bool:
        replacement = not value
    elif type(value) is int:
        replacement = value + 1
    elif value is None:
        replacement = 0
    elif isinstance(value, Enum):
        replacement = "changed"
    else:
        replacement = value + "x"
    original = sample().observer
    try:
        changed_digest = observer_incarnation_digest(corrupt(original, path, replacement))
    except ValueError:
        return  # Exact fixed-profile/zero facts cannot form another valid identity.
    assert changed_digest != observer_incarnation_digest(original)


@pytest.mark.parametrize("version", range(13, 18))
@pytest.mark.parametrize("unused", [None, 0, 1])
def test_visibility_preserves_unused_raw_permission(version, unused):
    values = dict(
        server_major_version=version,
        engine_edition=2,
        database_id=1,
        view_server_state=1 if version <= 15 else unused,
        view_server_performance_state=unused if version <= 15 else 1,
    )
    assert replace(SqlClientDepartureVisibilityV2(**values)) == SqlClientDepartureVisibilityV2(**values)
    values["view_server_state" if version <= 15 else "view_server_performance_state"] = None
    with pytest.raises(ValueError):
        SqlClientDepartureVisibilityV2(**values)


@pytest.mark.parametrize(
    "field,value",
    [
        ("session_id", 0),
        ("session_id", 32768),
        ("connection_id", UUID(int=0)),
        ("connect_time", datetime(1900, 1, 1)),
        ("login_time", datetime(2026, 1, 1)),
        ("connect_time", datetime(2026, 1, 1, tzinfo=UTC)),
        ("parent_connection_id", UUID(int=3)),
        ("mars_child_count", 1),
        ("transaction_count", 1),
        ("xact_state", -1),
    ],
)
def test_incarnation_rejects_unsafe_local_shape(field, value):
    with pytest.raises(ValueError):
        replace(sample().observer, **{field: value})


def test_literal_golden_bytes_and_both_digests():
    assert encode_observer_incarnation(sample().observer) == OWN_GOLDEN
    assert sha256(OWN_GOLDEN).hexdigest() == OWN_GOLDEN_SHA256
    assert observer_incarnation_digest(sample().observer) == OWN_SEMANTIC_SHA256
    assert decode_observer_incarnation(OWN_GOLDEN) == sample().observer


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown",
        "omitted_null",
        "nested_unknown",
        "profile",
        "float",
        "bool",
        "uuid_case",
        "timestamp_space",
        "timestamp_short",
        "timestamp_zone",
        "whitespace",
        "duplicate",
        "nested_duplicate",
        "escaped",
    ],
)
def test_closed_canonical_wire(mutation):
    data = json.loads(OWN_GOLDEN)
    if mutation == "unknown":
        data["trusted"] = True
    elif mutation == "omitted_null":
        del data["parent_connection_id"]
    elif mutation == "nested_unknown":
        data["authority"]["login"]["extra"] = 1
    elif mutation == "profile":
        del data["authority"]["profile"]
    elif mutation == "float":
        data["session_id"] = 72.0
    elif mutation == "bool":
        data["visibility"]["engine_edition"] = True
    elif mutation == "uuid_case":
        data["connection_id"] = "{00000000-0000-0000-0000-000000000002}"
    elif mutation == "timestamp_space":
        data["connect_time"] = data["connect_time"].replace("T", " ")
    elif mutation == "timestamp_short":
        data["connect_time"] = data["connect_time"][:-1]
    elif mutation == "timestamp_zone":
        data["connect_time"] += "Z"
    from dpone.contracts.strict_json import canonical_json_bytes

    body = canonical_json_bytes(data)
    if mutation == "whitespace":
        body = b" " + body
    elif mutation == "duplicate":
        body = body.replace(b"{", b'{"session_id":72,', 1)
    elif mutation == "nested_duplicate":
        body = body.replace(b'"engine_edition":4', b'"engine_edition":4,"engine_edition":4')
    elif mutation == "escaped":
        body = body.replace(b"writer", b"\\u0077riter")
    with pytest.raises(ValueError, match="^mssql_native.sqlclient_observer_incarnation_invalid$"):
        decode_observer_incarnation(body)


@pytest.mark.parametrize("payload", [b"", b"x" * 16385, bytearray(b"{}"), alias(b"{}")])
def test_bounds_and_exact_bytes_before_parser(payload, monkeypatch):
    import dpone.contracts.mssql_sqlclient_observer_incarnation_codec as codec

    monkeypatch.setattr(codec, "strict_json_object", lambda body: pytest.fail("premature parse"))
    with pytest.raises(ValueError):
        decode_observer_incarnation(payload)


def test_cap_boundary_does_not_invent_valid_padding():
    with pytest.raises(ValueError):
        decode_observer_incarnation(b" " * 16384)


@pytest.mark.parametrize("size", [0, 16385])
def test_encode_enforces_fixed_cap_without_adding_metadata(monkeypatch, size):
    import dpone.contracts.mssql_sqlclient_observer_incarnation as model

    value = sample().observer
    monkeypatch.setattr(model, "canonical_json_bytes", lambda data: b"x" * size)
    for operation in (encode_observer_incarnation, observer_incarnation_digest):
        with pytest.raises(ValueError, match="^mssql_native.sqlclient_observer_incarnation_invalid$"):
            operation(value)


def test_codec_does_not_swallow_process_control(monkeypatch):
    import dpone.contracts.mssql_sqlclient_observer_incarnation_codec as codec

    def interrupt(body):
        raise KeyboardInterrupt

    monkeypatch.setattr(codec, "strict_json_object", interrupt)
    with pytest.raises(KeyboardInterrupt):
        decode_observer_incarnation(OWN_GOLDEN)
