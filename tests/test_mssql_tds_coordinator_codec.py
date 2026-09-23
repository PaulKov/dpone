"""Strict durable coordinator records reject ambiguous and changed identities."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_tds_coordinator import (
    CoordinatorFailed,
    CoordinatorLocalObserved,
    CoordinatorRemoteObserved,
    TdsCoordinatorLocalKind,
    TdsCoordinatorLocalObservation,
    TdsCoordinatorRemoteKind,
    TdsCoordinatorRemoteObservation,
    coordinator_identity_digest,
    take_over_coordinator_state,
)
from dpone.contracts.mssql_tds_coordinator_codec import (
    MAX_COORDINATOR_RECORD_BYTES,
    decode_coordinator_state,
    encode_coordinator_state,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptError
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_mssql_tds_coordinator import OWNER, PROCESS, SESSION, advance, identity, sequence


@pytest.mark.parametrize("phase", range(6))
def test_exact_roundtrip_for_every_barrier_and_recovery(phase):
    state = sequence()[0][phase]
    for value in (
        state,
        take_over_coordinator_state(state, replace(OWNER, fence=OWNER.fence + 1, supervisor_id=str(UUID(int=10)))),
    ):
        encoded = encode_coordinator_state(value)
        assert len(encoded) <= MAX_COORDINATOR_RECORD_BYTES
        decoded = decode_coordinator_state(encoded, identity=value.identity)
        assert decoded == value
        assert encode_coordinator_state(decoded) == encoded


def test_failure_and_independent_observation_roundtrip():
    state = advance(sequence()[0][-1], CoordinatorFailed(TdsAttemptError.CONNECTION))
    binding = coordinator_identity_digest(state.identity)
    state = advance(
        state,
        CoordinatorLocalObserved(
            TdsCoordinatorLocalObservation(binding, TdsCoordinatorLocalKind.CONTAINED, PROCESS, "b" * 64, "c" * 64)
        ),
    )
    state = advance(
        state,
        CoordinatorRemoteObserved(
            TdsCoordinatorRemoteObservation(binding, TdsCoordinatorRemoteKind.SETTLED, SESSION, "b" * 64, "c" * 64)
        ),
    )
    assert decode_coordinator_state(encode_coordinator_state(state), identity=identity()) == state


def mutate(payload, path, value):
    data = strict_json_object(payload)
    current = data
    for name in path[:-1]:
        current = current[name]
    current[path[-1]] = value
    return canonical_json_bytes(data)


@pytest.mark.parametrize(
    "path,value",
    [
        (("sequence",), True),
        (("sequence",), -1),
        (("sequence",), 2**63),
        (("identity", "slot_index"), False),
        (("identity", "original_fence"), 1.0),
        (("process", "pid"), True),
        (("ownership", "fence"), True),
        (("phase",), "grant_intent"),
        (("session", "session_id"), True),
        (("session", "connection_id"), str(UUID(int=3)).upper().replace("-", "")),
        (("session", "nonce"), "AA" * 32),
        (("session", "login_time"), "2026-01-01T00:00:00"),
        (("grant", "grant_id"), "{" + str(UUID(int=4)) + "}"),
        (("grant", "process", "start_ticks"), 999),
        (("grant", "authority_sha256"), "f" * 64),
        (("result", "outcome"), "arbitrary_sql"),
        (("result", "error"), "driver"),
    ],
)
def test_invalid_or_noncanonical_scalar_and_evidence_combinations(path, value):
    payload = mutate(encode_coordinator_state(sequence()[0][-1]), path, value)
    with pytest.raises(ValueError, match="coordinator_record_invalid"):
        decode_coordinator_state(payload, identity=identity())


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("identity",),
        ("identity", "parent"),
        ("grant",),
        ("grant", "ownership"),
        ("session",),
        ("result",),
        ("process",),
    ],
)
@pytest.mark.parametrize("change", ["missing", "extra"])
def test_closed_shape_at_every_nested_boundary(path, change):
    data = strict_json_object(encode_coordinator_state(sequence()[0][-1]))
    current = data
    for name in path:
        current = current[name]
    if change == "missing":
        del current[next(iter(current))]
    else:
        current["connection_string"] = "forbidden"
    with pytest.raises(ValueError):
        decode_coordinator_state(canonical_json_bytes(data), identity=identity())


@pytest.mark.parametrize(
    "payload",
    [
        b"{}",
        b"[]",
        b"null",
        b"\xff",
        b'{"schema":NaN}',
        b'{"x":1,"x":1}',
        b"{" + b" " * MAX_COORDINATOR_RECORD_BYTES + b"}",
        b'{"x":' + b"[" * 2000 + b"0" + b"]" * 2000 + b"}",
    ],
)
def test_malformed_ambiguous_and_oversized_records(payload):
    with pytest.raises(ValueError):
        decode_coordinator_state(payload, identity=identity())


def test_duplicate_nested_key_is_rejected_even_when_values_equal():
    encoded = encode_coordinator_state(sequence()[0][-1])
    encoded = encoded.replace(b'"slot_index":0', b'"slot_index":0,"slot_index":0')
    with pytest.raises(ValueError):
        decode_coordinator_state(encoded, identity=identity())


@pytest.mark.parametrize(
    "field,value",
    [
        ("operation_id", UUID(int=50)),
        ("command_sha256", "f" * 64),
        ("implementation_sha256", "f" * 64),
        ("original_fence", OWNER.fence + 1),
    ],
)
def test_full_expected_identity_cannot_be_replaced_by_same_stable_locator(field, value):
    payload = encode_coordinator_state(sequence()[0][0])
    with pytest.raises(ValueError):
        decode_coordinator_state(payload, identity=replace(identity(), **{field: value}))


def test_json_whitespace_is_not_an_identity_change():
    payload = encode_coordinator_state(sequence()[0][-1])
    assert decode_coordinator_state(b" \n" + payload + b"\n", identity=identity()) == sequence()[0][-1]


def test_oversized_record_is_rejected_before_json_parser(monkeypatch):
    import dpone.contracts.mssql_tds_coordinator_codec as codec

    monkeypatch.setattr(codec, "strict_json_object", lambda payload: pytest.fail("oversized JSON was parsed"))
    with pytest.raises(ValueError):
        decode_coordinator_state(b" " * (MAX_COORDINATOR_RECORD_BYTES + 1), identity=identity())


def test_explicit_pre_spawn_absence_observations_roundtrip_without_fake_identities():
    state = sequence()[0][0]
    binding = coordinator_identity_digest(state.identity)
    state = advance(
        state,
        CoordinatorLocalObserved(
            TdsCoordinatorLocalObservation(binding, TdsCoordinatorLocalKind.NO_PROCESS, None, "a" * 64, "b" * 64)
        ),
    )
    state = advance(
        state,
        CoordinatorRemoteObserved(
            TdsCoordinatorRemoteObservation(binding, TdsCoordinatorRemoteKind.NO_SESSION, None, "a" * 64, "b" * 64)
        ),
    )
    decoded = decode_coordinator_state(encode_coordinator_state(state), identity=identity())
    assert decoded == state and decoded.process is None and decoded.session is None
