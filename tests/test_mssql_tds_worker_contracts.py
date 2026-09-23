"""Closed lifecycle records reject ambiguous authority and unsafe transitions."""

from dataclasses import FrozenInstanceError, replace

import pytest

from dpone.contracts.mssql_tds_worker import (
    Contained,
    ContainmentRequired,
    Exited,
    LaunchIntent,
    ParentAuthority,
    ParentRetirementRequired,
    Prepared,
    ProcessRegistered,
    Retired,
    RetirementRequired,
    Running,
    TdsAttemptError,
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    TdsObjectIdentity,
    TdsProcessIdentity,
    Verified,
    advance_state,
    initial_state,
    replace_ownership,
)
from dpone.contracts.mssql_tds_worker_codec import decode_state, encode_state

H = "a" * 64
U = "12345678-1234-1234-1234-123456789abc"


def initial():
    identity = TdsAttemptIdentity("target", "run", 0, 0, H, H, H, H, "db", "test", "stage", H)
    return initial_state(identity, TdsAttemptOwnership("owner", 1, U))


def normal_states():
    state = initial()
    yield state
    for event in (
        Prepared(TdsObjectIdentity(1, H), H),
        LaunchIntent(H),
        ProcessRegistered(TdsProcessIdentity(H, U, 123, 1)),
        Running(),
        Exited(0, H),
        Verified(H),
    ):
        state = advance_state(state, event, expected_phase=state.phase)
        yield state


def test_normal_roundtrip_and_sequence():
    for sequence, state in enumerate(normal_states()):
        assert state.sequence == sequence
        assert decode_state(encode_state(state)) == state
        assert encode_state(decode_state(encode_state(state))) == encode_state(state)
    with pytest.raises(FrozenInstanceError):
        state.sequence = 9


@pytest.mark.parametrize("index", range(6))
def test_failure_retirement_from_every_gap(index):
    state = list(normal_states())[index]
    for event in (ContainmentRequired(TdsAttemptError.DECODER), Contained(H), RetirementRequired(), Retired(H)):
        state = advance_state(state, event, expected_phase=state.phase)
        assert decode_state(encode_state(state)) == state
    assert state.phase == "retired"


def test_verified_parent_retirement_requires_settled_parent_and_preserves_verification():
    state = list(normal_states())[-1]
    state = advance_state(
        state,
        ParentRetirementRequired(ParentAuthority("published", H)),
        expected_phase=TdsAttemptPhase.VERIFIED,
    )
    assert state.error is None
    for event in (Contained(H), RetirementRequired(), Retired(H)):
        state = advance_state(state, event, expected_phase=state.phase)
    assert state.verification_sha256 == H


def test_verified_failure_event_cannot_impersonate_normal_parent_retirement():
    state = list(normal_states())[-1]
    for authority in (None, ParentAuthority("published", H), ParentAuthority("aborted", H)):
        with pytest.raises(ValueError):
            advance_state(
                state,
                ContainmentRequired(TdsAttemptError.CLEANUP, authority),
                expected_phase=TdsAttemptPhase.VERIFIED,
            )


def test_stale_phase_and_out_of_order_are_rejected():
    for event in (Running(), Verified(H), Retired(H)):
        with pytest.raises(ValueError):
            advance_state(initial(), event, expected_phase=TdsAttemptPhase.CREATION_INTENT)
    with pytest.raises(ValueError):
        advance_state(initial(), Prepared(TdsObjectIdentity(1, H), H), expected_phase=TdsAttemptPhase.PREPARED)


@pytest.mark.parametrize("payload", [b'{"x":1,"x":2}', b'{"extra":true}', b"x" * 16385])
def test_untrusted_json_rejected(payload):
    with pytest.raises(ValueError):
        decode_state(payload)


@pytest.mark.parametrize(
    "field,value", [("ordinal", True), ("attempt", 3), ("file_sha256", "A" * 64), ("table", ""), ("run_id", "x" * 257)]
)
def test_identity_invalid_values(field, value):
    with pytest.raises(ValueError):
        replace(initial().identity, **{field: value})


def test_recovery_requires_newer_fence():
    state = initial()
    with pytest.raises(ValueError):
        replace_ownership(state, TdsAttemptOwnership("other", 1, U))
    changed = replace_ownership(state, TdsAttemptOwnership("other", 2, "22345678-1234-1234-1234-123456789abc"))
    assert changed.identity == state.identity and changed.sequence == state.sequence + 1


def all_events():
    return (
        Prepared(TdsObjectIdentity(1, H), H),
        LaunchIntent(H),
        ProcessRegistered(TdsProcessIdentity(H, U, 123, 1)),
        Running(),
        Exited(0, H),
        Verified(H),
        ContainmentRequired(TdsAttemptError.DECODER),
        Contained(H),
        RetirementRequired(),
        Retired(H),
    )


@pytest.mark.parametrize("phase_index", range(7))
@pytest.mark.parametrize("event_index", range(10))
def test_normal_phase_event_matrix(phase_index, event_index):
    state = list(normal_states())[phase_index]
    event = all_events()[event_index]
    allowed = (phase_index < 6 and event_index == phase_index) or (phase_index < 6 and event_index == 6)
    if allowed:
        advance_state(state, event, expected_phase=state.phase)
    else:
        with pytest.raises(ValueError):
            advance_state(state, event, expected_phase=state.phase)


@pytest.mark.parametrize(
    "field,value",
    [
        ("sequence", True),
        ("sequence", -1),
        ("phase", "creation_intent"),
        ("error", "driver"),
        ("process", {}),
        ("exit_code", True),
        ("verification_sha256", H),
        ("observation_sha256", H),
    ],
)
def test_constructor_invalid_phase_and_types(field, value):
    with pytest.raises(ValueError):
        replace(initial(), **{field: value})


@pytest.mark.parametrize(
    "mutation",
    [
        "extra",
        "nested_extra",
        "bool_sequence",
        "missing",
        "nullable_identity",
        "unknown_phase",
        "unearned_process",
        "duplicate_nested",
    ],
)
def test_decoder_rejects_structural_and_semantic_mutations(mutation):
    import json

    payload = encode_state(initial())
    value = json.loads(payload)
    if mutation == "extra":
        value["extra"] = "forbidden"
    elif mutation == "nested_extra":
        value["identity"]["extra"] = 1
    elif mutation == "bool_sequence":
        value["sequence"] = True
    elif mutation == "missing":
        del value["error"]
    elif mutation == "nullable_identity":
        value["identity"] = None
    elif mutation == "unknown_phase":
        value["phase"] = "success"
    elif mutation == "unearned_process":
        value["process"] = {"host_sha256": H, "boot_id": U, "pid": 1, "start_ticks": 1}
    else:
        payload = payload.replace(b'"attempt":0', b'"attempt":0,"attempt":0')
    if mutation != "duplicate_nested":
        payload = json.dumps(value).encode()
    with pytest.raises(ValueError):
        decode_state(payload)


@pytest.mark.parametrize("value", [False, 0, -1, 2**63, "1"])
def test_snapshot_invalid_revision(value):
    with pytest.raises(ValueError):
        TdsAttemptSnapshot(initial(), value)


@pytest.mark.parametrize(
    "field,value",
    [("pid", True), ("pid", 0), ("pid", 2**31), ("start_ticks", -1), ("boot_id", U.upper()), ("host_sha256", "x" * 64)],
)
def test_invalid_process_identity(field, value):
    with pytest.raises(ValueError):
        replace(TdsProcessIdentity(H, U, 123, 1), **{field: value})


def test_unknown_parent_authority_and_nonzero_exit_cannot_verify():
    with pytest.raises(ValueError):
        ParentAuthority("unknown", H)
    running = list(normal_states())[4]
    exited = advance_state(running, Exited(1, H), expected_phase=running.phase)
    with pytest.raises(ValueError):
        advance_state(exited, Verified(H), expected_phase=exited.phase)


def test_retired_is_terminal_and_takeover_preserves_all_evidence():
    state = list(normal_states())[-1]
    state = advance_state(state, ParentRetirementRequired(ParentAuthority("aborted", H)), expected_phase=state.phase)
    for event in (Contained(H), RetirementRequired(), Retired(H)):
        state = advance_state(state, event, expected_phase=state.phase)
    for event in all_events():
        with pytest.raises(ValueError):
            advance_state(state, event, expected_phase=state.phase)
    changed = replace_ownership(state, TdsAttemptOwnership("new", 2, "22345678-1234-1234-1234-123456789abc"))
    assert replace(changed, ownership=state.ownership, sequence=state.sequence) == state


@pytest.mark.parametrize("value", [True, 0, 2, "1"])
def test_schema_version_is_closed_strict_integer(value):
    with pytest.raises(ValueError):
        replace(initial(), schema_version=value)


def test_same_supervisor_cannot_claim_new_fence():
    with pytest.raises(ValueError):
        replace_ownership(initial(), TdsAttemptOwnership("other", 2, U))


def test_zero_start_ticks_is_valid_and_expected_phase_requires_enum():
    assert TdsProcessIdentity(H, U, 1, 0).start_ticks == 0
    with pytest.raises(ValueError):
        advance_state(initial(), all_events()[0], expected_phase="creation_intent")


def test_sequence_overflow_rejected():
    state = replace(initial(), sequence=2**63 - 1)
    with pytest.raises(ValueError):
        advance_state(state, all_events()[0], expected_phase=state.phase)


@pytest.mark.parametrize(
    "changes",
    [
        {"result_sha256": H},
        {"process": TdsProcessIdentity(H, U, 1, 0)},
        {"verification_sha256": H},
        {"parent_authority": ParentAuthority("published", H)},
        {"error": None},
        {"sequence": 0},
    ],
)
def test_failure_phase_evidence_combinations_are_closed(changes):
    state = advance_state(
        initial(), ContainmentRequired(TdsAttemptError.DECODER), expected_phase=TdsAttemptPhase.CREATION_INTENT
    )
    with pytest.raises(ValueError):
        replace(state, **changes)
