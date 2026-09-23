"""Pure coordinator barriers preserve uncertainty and never authorize replay."""

from dataclasses import replace
from datetime import datetime
from uuid import UUID

import pytest

from dpone.contracts.mssql_tds_coordinator import (
    CoordinatorCredentialIntent,
    CoordinatorFailed,
    CoordinatorGrantIntent,
    CoordinatorLocalObserved,
    CoordinatorProcessRegistered,
    CoordinatorRemoteObserved,
    CoordinatorResultReceived,
    CoordinatorSessionRegistered,
    TdsCoordinatorGrant,
    TdsCoordinatorIdentity,
    TdsCoordinatorLocalKind,
    TdsCoordinatorLocalObservation,
    TdsCoordinatorPhase,
    TdsCoordinatorRemoteKind,
    TdsCoordinatorRemoteObservation,
    TdsCoordinatorResult,
    TdsCoordinatorResultKind,
    advance_coordinator_state,
    coordinator_grant_digest,
    coordinator_identity_digest,
    coordinator_key,
    initial_coordinator_state,
    take_over_coordinator_state,
)
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand, initial_directory, reserve_operation
from dpone.contracts.mssql_tds_session import TdsRemoteSessionIdentity
from dpone.contracts.mssql_tds_worker import TdsAttemptError, TdsProcessIdentity
from tests.test_mssql_tds_directory import LIMITS, PARENT
from tests.test_mssql_tds_directory_journal import OWNER


def reservation():
    return reserve_operation(
        initial_directory(PARENT, LIMITS),
        operation_id=UUID(int=1),
        command=TdsCoordinatorCommand.CREATE,
        command_sha256="a" * 64,
        owner_fence=OWNER.fence,
    )


def identity():
    return TdsCoordinatorIdentity(PARENT, 0, UUID(int=1), TdsCoordinatorCommand.CREATE, "a" * 64, OWNER.fence, "b" * 64)


def test_initial_state_is_bound_to_original_directory_reservation():
    value = identity()
    state = initial_coordinator_state(value, reservation(), OWNER)
    assert state.phase is TdsCoordinatorPhase.INTENT
    assert state.execution_owner == state.ownership == OWNER
    assert state.process is None and state.session is None and state.grant is None
    assert coordinator_key(value).endswith("/operation/0")


PROCESS = TdsProcessIdentity("c" * 64, str(UUID(int=2)), 123, 456)
SESSION = TdsRemoteSessionIdentity(UUID(int=3), 12, datetime(2026, 1, 1), datetime(2026, 1, 1), b"x" * 32, b"a" * 32)


def advance(state, event):
    return advance_coordinator_state(state, event, expected_phase=state.phase)


def sequence():
    state = initial_coordinator_state(identity(), reservation(), OWNER)
    states = [state]
    grant = TdsCoordinatorGrant(coordinator_identity_digest(identity()), OWNER, PROCESS, SESSION, "d" * 64, UUID(int=4))
    result = TdsCoordinatorResult(
        coordinator_identity_digest(identity()),
        coordinator_grant_digest(grant),
        TdsCoordinatorResultKind.SUCCEEDED,
        "e" * 64,
    )
    events = [
        CoordinatorProcessRegistered(PROCESS, "c" * 64),
        CoordinatorCredentialIntent(),
        CoordinatorSessionRegistered(SESSION, "d" * 64),
        CoordinatorGrantIntent(grant),
        CoordinatorResultReceived(result),
    ]
    for event in events:
        state = advance(state, event)
        states.append(state)
    return states, events


def test_complete_barriers_have_no_implicit_settlement():
    states, _ = sequence()
    assert [state.phase for state in states] == list(TdsCoordinatorPhase)
    assert states[-1].result.outcome is TdsCoordinatorResultKind.SUCCEEDED
    assert all(state.local is None and state.remote is None for state in states)


@pytest.mark.parametrize("state_index", range(6))
@pytest.mark.parametrize("event_index", range(5))
def test_skipped_repeated_or_out_of_order_barriers_rejected(state_index, event_index):
    states, events = sequence()
    if state_index == event_index:
        assert advance(states[state_index], events[event_index]) == states[state_index + 1]
    else:
        with pytest.raises(ValueError):
            advance(states[state_index], events[event_index])


@pytest.mark.parametrize(
    "field,value",
    [
        ("operation_id", UUID(int=9)),
        ("command_sha256", "f" * 64),
        ("original_fence", OWNER.fence + 1),
        ("implementation_sha256", "f" * 64),
    ],
)
def test_changed_immutable_bindings_collide_at_stable_locator(field, value):
    changed = replace(identity(), **{field: value})
    assert coordinator_key(changed) == coordinator_key(identity())
    assert coordinator_identity_digest(changed) != coordinator_identity_digest(identity())
    if field != "implementation_sha256":
        with pytest.raises(ValueError):
            initial_coordinator_state(changed, reservation(), OWNER)


def test_parent_changed_policy_and_implementation_still_find_original_slot():
    changed = replace(identity(), parent=replace(PARENT, policy_sha256="f" * 64))
    assert coordinator_key(changed) == coordinator_key(identity())
    with pytest.raises(ValueError):
        initial_coordinator_state(changed, reservation(), OWNER)
    assert coordinator_key(replace(identity(), slot_index=1)) != coordinator_key(identity())


@pytest.mark.parametrize("index", range(6))
def test_takeover_preserves_evidence_and_cannot_resume_any_normal_barrier(index):
    states, events = sequence()
    old = states[index]
    owner = replace(OWNER, fence=OWNER.fence + 1, supervisor_id=str(UUID(int=20)))
    recovered = take_over_coordinator_state(old, owner)
    assert recovered.execution_owner == old.execution_owner
    assert recovered.grant == old.grant and recovered.result == old.result
    assert recovered.recovering
    for event in events:
        with pytest.raises(ValueError):
            advance(recovered, event)
    assert advance(recovered, CoordinatorFailed(TdsAttemptError.FENCING)).error is TdsAttemptError.FENCING


def test_failure_and_observations_retain_acknowledged_grant_and_result():
    state = sequence()[0][-1]
    failed = advance(state, CoordinatorFailed(TdsAttemptError.CLEANUP))
    binding = coordinator_identity_digest(state.identity)
    local = TdsCoordinatorLocalObservation(binding, TdsCoordinatorLocalKind.CONTAINED, PROCESS, "f" * 64, "a" * 64)
    contained = advance(failed, CoordinatorLocalObserved(local))
    assert contained.remote is None and contained.grant == state.grant and contained.result == state.result
    remote = TdsCoordinatorRemoteObservation(binding, TdsCoordinatorRemoteKind.SETTLED, SESSION, "f" * 64, "b" * 64)
    settled = advance(contained, CoordinatorRemoteObserved(remote))
    assert settled.local == local and settled.remote == remote
    assert advance(settled, CoordinatorRemoteObserved(remote)) == settled
    with pytest.raises(ValueError):
        advance(settled, CoordinatorRemoteObserved(replace(remote, proof_sha256="c" * 64)))


def test_pre_spawn_observations_need_explicit_authority_without_fake_process_or_session():
    state, events = sequence()[0][0], sequence()[1]
    assert state.local is None and state.remote is None
    binding = coordinator_identity_digest(state.identity)
    remote = TdsCoordinatorRemoteObservation(binding, TdsCoordinatorRemoteKind.NO_SESSION, None, "f" * 64, "b" * 64)
    observed = advance(state, CoordinatorRemoteObserved(remote))
    assert observed.local is None and observed.process is None
    with pytest.raises(ValueError):
        advance(observed, events[0])
    local = TdsCoordinatorLocalObservation(binding, TdsCoordinatorLocalKind.NO_PROCESS, None, "f" * 64, "a" * 64)
    contained = advance(observed, CoordinatorLocalObserved(local))
    assert contained.local.process is None and contained.remote.session is None
    with pytest.raises(ValueError):
        TdsCoordinatorLocalObservation(binding, TdsCoordinatorLocalKind.NO_PROCESS, PROCESS, "f" * 64, "a" * 64)


@pytest.mark.parametrize(
    "field,value",
    [
        ("ownership", replace(OWNER, owner="other")),
        ("process", replace(PROCESS, start_ticks=457)),
        ("session", replace(SESSION, login_time=datetime(2026, 1, 2))),
        ("authority_sha256", "f" * 64),
        ("operation_sha256", "f" * 64),
    ],
)
def test_grant_requires_exact_all_incarnation_and_authority_bindings(field, value):
    states, events = sequence()
    with pytest.raises(ValueError):
        advance(states[3], CoordinatorGrantIntent(replace(events[3].grant, **{field: value})))


def test_result_requires_exact_grant_and_no_fabricated_success_error():
    states, events = sequence()
    with pytest.raises(ValueError):
        advance(states[4], CoordinatorResultReceived(replace(events[4].result, grant_sha256="f" * 64)))
    with pytest.raises(ValueError):
        replace(events[4].result, error=TdsAttemptError.DRIVER)


def test_initialization_cannot_replay_a_slot_with_existing_settlement_observations():
    from dpone.contracts.mssql_tds_directory import TdsLocalContainment, parent_digest, record_local_containment

    directory = reservation()
    proof = TdsLocalContainment(parent_digest(PARENT), UUID(int=1), "a" * 64, "b" * 64)
    observed = record_local_containment(directory, 0, proof)
    with pytest.raises(ValueError):
        initial_coordinator_state(identity(), observed, OWNER)


def test_forged_recovery_cannot_claim_one_step_same_token_or_impossible_sequence():
    state = sequence()[0][0]
    with pytest.raises(ValueError):
        replace(state, ownership=replace(OWNER, fence=OWNER.fence + 1), sequence=1)
    with pytest.raises(ValueError):
        replace(state, sequence=1)


@pytest.mark.parametrize("revision", [True, 0, -1, 1.0, 2**63])
def test_coordinator_snapshot_rejects_invalid_store_revision(revision):
    from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorSnapshot

    with pytest.raises(ValueError):
        TdsCoordinatorSnapshot(sequence()[0][0], revision)


def test_coordinator_snapshot_preserves_immutable_state_and_full_revision_bound():
    from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorSnapshot

    state = sequence()[0][0]
    assert TdsCoordinatorSnapshot(state, 2**63 - 1).state is state
    with pytest.raises(ValueError):
        TdsCoordinatorSnapshot(None, 1)
