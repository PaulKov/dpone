"""Pure coordinator directory ordering; proof fixtures are not live observations."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_tds_directory import (
    TdsCoordinatorCommand as Command,
)
from dpone.contracts.mssql_tds_directory import (
    TdsDirectoryLimits,
    TdsLocalContainment,
    TdsRemoteSettlement,
    authorize_retirement,
    close_admission,
    directory_key,
    encode_directory,
    initial_directory,
    parent_digest,
    record_local_containment,
    record_remote_settlement,
    reserve_operation,
    reserve_reconciliation,
    seal_work,
)
from dpone.contracts.mssql_tds_worker import (
    ParentAuthority,
    TdsAttemptError,
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsAttemptPhase,
    TdsAttemptState,
    TdsObjectIdentity,
    TdsProcessIdentity,
)

H = "a" * 64
PARENT = TdsAttemptIdentity("target", "run", 0, 0, H, H, H, H, "db", "schema", "table", H)
LIMITS = TdsDirectoryLimits(8, 3, 50000, 10000)


def authority(verified=False):
    return TdsAttemptState(
        PARENT,
        TdsAttemptOwnership("owner", 1, str(UUID(int=99))),
        TdsAttemptPhase.CONTAINED,
        8,
        TdsObjectIdentity(1, H) if verified else None,
        TdsProcessIdentity(H, str(UUID(int=99)), 1, 1) if verified else None,
        0 if verified else None,
        H if verified else None,
        H if verified else None,
        TdsAttemptError.OPERATION_TIMEOUT,
        H,
        ParentAuthority("published", H) if verified else None,
    )


def initial():
    return initial_directory(PARENT, LIMITS)


def reserve(state, command=Command.CREATE, number=1, fence=1):
    return reserve_operation(state, operation_id=UUID(int=number), command=command, command_sha256=H, owner_fence=fence)


def local(state, slot=0):
    return TdsLocalContainment(parent_digest(state.parent), state.slots[slot].operation_id, H, H)


def remote(state, slot=0):
    return TdsRemoteSettlement(parent_digest(state.parent), state.slots[slot].operation_id, H, H)


def settled(state, slot=0):
    state = record_local_containment(state, slot, local(state, slot))
    return record_remote_settlement(state, slot, remote(state, slot))


def test_unresolved_slot_blocks_new_uuid_and_local_exit_is_not_remote_settlement():
    state = reserve(initial())
    with pytest.raises(ValueError):
        reserve(state, number=2)
    state = record_local_containment(state, 0, local(state))
    assert not state.slots[0].settled
    with pytest.raises(ValueError):
        reserve(state, number=2)
    state = settled(state)
    assert reserve(state, number=2).slots[1].index == 1


def test_stable_locator_detects_changed_full_parent_binding():
    state = initial()
    changed = initial_directory(replace(PARENT, policy_sha256="b" * 64), LIMITS)
    assert directory_key(state.parent) == directory_key(changed.parent)
    assert encode_directory(state) != encode_directory(changed)


def test_reconciliation_needs_new_fence_and_exact_local_proof():
    state = reserve(initial())
    kwargs = dict(operation_id=UUID(int=2), command_sha256=H, reconciles_slot=0, containment=local(state))
    with pytest.raises(ValueError):
        reserve_reconciliation(state, owner_fence=1, **kwargs)
    recovery = reserve_reconciliation(state, owner_fence=2, **kwargs)
    assert recovery.slots[1].reconciles_slot == 0
    assert recovery.slots[0].local_containment == local(state)
    assert recovery.slots[0].remote_settlement is None
    with pytest.raises(ValueError):
        reserve(recovery, number=3, fence=2)


def test_work_barrier_then_retirement_then_final_close():
    state = seal_work(settled(reserve(initial())))
    with pytest.raises(ValueError):
        reserve(state, Command.VERIFY, number=2)
    with pytest.raises(ValueError):
        reserve(state, Command.RETIRE, number=2)
    state = authorize_retirement(state, authority(verified=True))
    state = reserve(state, Command.RETIRE, number=2)
    with pytest.raises(ValueError):
        close_admission(state)
    state = close_admission(settled(state, 1))
    with pytest.raises(ValueError):
        reserve(state, Command.RETIRE, number=3)
    assert state.admission_closed


def test_retirement_headroom_survives_normal_exhaustion():
    state = initial_directory(PARENT, TdsDirectoryLimits(4, 2, 50000, 10000))
    state = settled(reserve(state))
    state = settled(reserve(state, number=2), 1)
    with pytest.raises(ValueError):
        reserve(state, number=3)
    state = authorize_retirement(seal_work(state), authority())
    assert reserve(state, Command.RETIRE, number=3).slots[-1].command is Command.RETIRE


def test_proof_families_and_bindings_cannot_be_substituted():
    state = reserve(initial())
    with pytest.raises(ValueError):
        record_local_containment(state, 0, remote(state))
    with pytest.raises(ValueError):
        record_remote_settlement(state, 0, local(state))
    with pytest.raises(ValueError):
        record_local_containment(state, 0, replace(local(state), operation_id=UUID(int=20)))
    with pytest.raises(ValueError):
        record_remote_settlement(state, 0, replace(remote(state), parent_sha256="b" * 64))


def test_duplicates_gaps_and_closed_barrier_rewrites_rejected():
    state = settled(reserve(initial()))
    with pytest.raises(ValueError):
        reserve(state)
    with pytest.raises(ValueError):
        replace(state, slots=(replace(state.slots[0], index=1),))
    state = authorize_retirement(seal_work(state), authority(verified=True))
    with pytest.raises(ValueError):
        authorize_retirement(state, authority())


def test_limits_require_explicit_retirement_entry_and_byte_headroom():
    for values in [(True, 1, 10000, 3000), (3, 0, 10000, 3000), (3, 3, 10000, 3000)]:
        with pytest.raises(ValueError):
            TdsDirectoryLimits(*values)
    with pytest.raises(ValueError):
        initial_directory(PARENT, TdsDirectoryLimits(3, 1, 3000, 10))


def recover(state, number, fence):
    index = len(state.slots) - 1
    return reserve_reconciliation(
        state,
        operation_id=UUID(int=number),
        command_sha256=H,
        owner_fence=fence,
        reconciles_slot=index,
        containment=local(state, index),
    )


def test_reconciliation_child_never_resolves_root_and_chain_cannot_branch():
    state = recover(reserve(initial()), 2, 2)
    state = settled(state, 1)
    assert not state.slots[0].settled
    with pytest.raises(ValueError):
        seal_work(state)
    with pytest.raises(ValueError):
        reserve_reconciliation(
            state,
            operation_id=UUID(int=3),
            command_sha256=H,
            owner_fence=3,
            reconciles_slot=0,
            containment=local(state, 0),
        )
    state = recover(state, 3, 3)
    assert state.slots[2].reconciles_slot == 1
    with pytest.raises(ValueError):
        record_remote_settlement(state, 0, remote(state, 2))
    state = settled(state, 2)
    state = record_remote_settlement(state, 0, remote(state, 0))
    assert seal_work(state).work_sealed


def test_work_reconciliation_exhaustion_keeps_cleanup_partition():
    state = initial_directory(PARENT, TdsDirectoryLimits(4, 2, 50000, 10000))
    state = recover(reserve(state), 2, 2)
    with pytest.raises(ValueError):
        recover(state, 3, 3)
    state = settled(settled(state, 1), 0)
    state = authorize_retirement(seal_work(state), authority())
    state = reserve(state, Command.RETIRE, number=3, fence=2)
    state = recover(state, 4, 3)
    assert len(state.slots) == 4
    with pytest.raises(ValueError):
        recover(state, 5, 4)


def test_retirement_authority_is_exact_parent_and_contained_phase():
    state = seal_work(initial())
    with pytest.raises(ValueError):
        authorize_retirement(state, replace(authority(), identity=replace(PARENT, file_sha256="b" * 64)))
    with pytest.raises(ValueError):
        authorize_retirement(state, replace(authority(), phase=TdsAttemptPhase.CONTAINMENT_REQUIRED))
    failed = authorize_retirement(state, authority())
    assert failed.retirement_authority.parent_authority is None
    verified = authorize_retirement(state, authority(True))
    assert verified.retirement_authority.parent_authority.kind == "published"


def test_admission_closure_is_not_a_resource_release_receipt():
    state = authorize_retirement(seal_work(initial()), authority())
    state = reserve(state, Command.RETIRE)
    # A caller may prove the failed DROP session settled while the table remains.
    state = close_admission(settled(state))
    assert state.admission_closed
    assert not hasattr(state, "retired") and not hasattr(state, "release_resources")
    assert state.slots[0].remote_settlement == remote(state)


@pytest.mark.parametrize("value", [True, -1, 1.0, "1", None, 2**63])
def test_strict_slot_fence(value):
    with pytest.raises(ValueError):
        reserve(initial(), fence=value)


@pytest.mark.parametrize(
    "field,value", [("operation_id", "notuuid"), ("command", "create"), ("command_sha256", "A" * 64), ("index", True)]
)
def test_slot_rejects_coercible_or_invalid_bindings(field, value):
    slot = reserve(initial()).slots[0]
    with pytest.raises(ValueError):
        replace(slot, **{field: value})


def test_conflicting_proof_does_not_replace_acknowledged_observation():
    state = settled(reserve(initial()))
    assert record_local_containment(state, 0, local(state)) is state
    with pytest.raises(ValueError):
        record_local_containment(state, 0, replace(local(state), proof_sha256="b" * 64))
    with pytest.raises(ValueError):
        record_remote_settlement(state, 0, replace(remote(state), authority_sha256="b" * 64))


def test_byte_budget_exhausts_before_entries_but_later_evidence_still_fits():
    generous = reserve(initial())
    from dpone.contracts import mssql_tds_directory as model

    # Derive the exact conservative envelope from admitted record shapes, not a
    # production magic cap; search budget values without any backend effects.
    unit = model._slot_bound()
    reserved = 2 * unit
    state = None
    for budget in range(reserved + 1000, reserved + 10000):
        try:
            candidate = initial_directory(PARENT, TdsDirectoryLimits(8, 2, budget, reserved))
            candidate = reserve(candidate)
        except ValueError:
            continue
        state = candidate
        break
    assert state is not None
    state = settled(state)
    with pytest.raises(ValueError):
        reserve(state, number=2)
    state = authorize_retirement(seal_work(state), authority(True))
    state = settled(reserve(state, Command.RETIRE, number=2), 1)
    state = close_admission(state)
    assert len(encode_directory(state)) <= state.limits.max_encoded_bytes
    assert generous.slots[0].operation_id == state.slots[0].operation_id


def test_unsettled_intermediate_reconciliation_cannot_be_stranded():
    state = recover(reserve(initial()), 2, 2)
    state = recover(state, 3, 3)
    state = settled(state, 0)
    state = settled(state, 2)
    assert not state.slots[1].settled
    with pytest.raises(ValueError):
        seal_work(state)
    resumed = recover(state, 4, 4)
    assert resumed.slots[3].reconciles_slot == 2
    assert not resumed.slots[1].settled


def test_identical_barrier_and_retirement_authorization_replays_are_noops():
    state = seal_work(settled(reserve(initial())))
    assert seal_work(state) is state
    proof = authority()
    state = authorize_retirement(state, proof)
    assert authorize_retirement(state, proof) is state
    state = close_admission(state)
    assert close_admission(state) is state
    assert seal_work(state) is state
    assert authorize_retirement(state, proof) is state
    with pytest.raises(ValueError):
        authorize_retirement(state, authority(verified=True))
