"""Closed cross-language launch bindings, independent of process execution."""

from dataclasses import asdict, replace
from pathlib import Path

import pytest

from dpone.contracts.mssql_sqlclient_launch import (
    SqlClientDescriptors,
    SqlClientLaunch,
    SqlClientReady,
    decode_launch,
    decode_ready,
    encode_launch,
    encode_ready,
    launch_digest,
    validate_ready,
)
from dpone.contracts.mssql_tds_worker import TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes


def launch():
    return SqlClientLaunch(
        schema_version=1,
        nonce="11111111-1111-4111-8111-111111111111",
        attempt_sha256="a" * 64,
        build_sha256="b" * 64,
        input_binding_sha256="c" * 64,
        process=TdsProcessIdentity("d" * 64, "22222222-2222-4222-8222-222222222222", 123, 456),
        parent_pid=122,
        startup_deadline_ns=10_000_000_000,
        operation_deadline_ns=20_000_000_000,
        address_space_bytes=8 * 1024**3,
        descriptors=SqlClientDescriptors(3, 4, 5, 6, 7, 8),
    )


def ready(value):
    return SqlClientReady(
        1, launch_digest(value), value.process, value.address_space_bytes, 9, 0, "8.0.31", 536870912, False, "Disable"
    )


def test_closed_roundtrip_and_observation_binding():
    value = launch()
    assert decode_launch(encode_launch(value)) == value
    observed = ready(value)
    assert decode_ready(encode_ready(observed)) == observed
    validate_ready(value, observed, now_ns=value.startup_deadline_ns - 1)
    with pytest.raises(ValueError):
        validate_ready(replace(value, input_binding_sha256="e" * 64), observed, now_ns=1)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("schema_version", True),
        ("schema_version", 2),
        ("startup_deadline_ns", 1.0),
        ("operation_deadline_ns", 0),
        ("operation_deadline_ns", 2**63),
        ("address_space_bytes", 2 * 1024**3),
        ("attempt_sha256", "A" * 64),
        ("nonce", "00000000-0000-0000-0000-000000000000"),
        ("parent_pid", True),
    ],
)
def test_launch_rejects_coercion_and_invalid_bounds(field, bad):
    value = asdict(launch())
    value[field] = bad
    with pytest.raises(ValueError):
        decode_launch(canonical_json_bytes(value))


@pytest.mark.parametrize("mutation", ["extra", "missing", "duplicate", "oversize", "trailing", "nested_extra"])
def test_closed_wire_rejects_ambiguous_or_unbounded_launch(mutation):
    value = asdict(launch())
    if mutation == "extra":
        value["extra"] = 1
    if mutation == "missing":
        del value["nonce"]
    if mutation == "nested_extra":
        value["descriptors"]["extra"] = 1
    payload = canonical_json_bytes(value)
    if mutation == "duplicate":
        payload = payload[:-1] + b',"schema_version":1}'
    if mutation == "oversize":
        payload += b" " * 16384
    if mutation == "trailing":
        payload += b"{}"
    with pytest.raises(ValueError):
        decode_launch(payload)


def test_descriptors_are_distinct_and_do_not_alias_standard_streams():
    for fds in [(3, 3, 5, 6, 7, 8), (0, 4, 5, 6, 7, 8), (True, 4, 5, 6, 7, 8)]:
        with pytest.raises(ValueError):
            SqlClientDescriptors(*fds)


@pytest.mark.parametrize("field,bad", [("launch_sha256", "e" * 64), ("address_space_bytes", 16 * 1024**3)])
def test_readiness_cannot_change_launch_binding_or_effective_limit(field, bad):
    value = launch()
    with pytest.raises(ValueError):
        validate_ready(value, replace(ready(value), **{field: bad}), now_ns=1)


def test_startup_deadline_is_strict_and_never_renewed():
    value = launch()
    with pytest.raises(ValueError):
        validate_ready(value, ready(value), now_ns=value.startup_deadline_ns)


def test_frozen_cross_language_launch_bytes_and_digest():
    fixtures = Path(__file__).parent / "fixtures" / "mssql_sqlclient"
    payload = (fixtures / "launch-v1.json").read_bytes().rstrip(b"\n")
    value = decode_launch(payload)
    assert value == launch()
    assert encode_launch(value) == payload
    assert launch_digest(value) == (fixtures / "launch-v1.sha256").read_text().strip()


@pytest.mark.parametrize(
    "field,bad",
    [
        ("schema_version", True),
        ("parent_death_signal", 0),
        ("core_limit_bytes", False),
        ("gc_heap_limit_bytes", 536870911),
        ("server_gc", 0),
        ("runtime_version", "8.0.30"),
        ("roll_forward", "LatestPatch"),
        ("address_space_bytes", 2**63),
    ],
)
def test_ready_rejects_unadmitted_profile(field, bad):
    value = asdict(ready(launch()))
    value[field] = bad
    with pytest.raises(ValueError):
        decode_ready(canonical_json_bytes(value))


def test_shorter_operation_deadline_also_bounds_startup():
    value = replace(launch(), operation_deadline_ns=5)
    with pytest.raises(ValueError):
        validate_ready(value, ready(value), now_ns=5)
