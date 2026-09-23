"""Attempt composition must acknowledge parent intent before directory admission."""

from contextlib import contextmanager
from time import monotonic
from uuid import UUID

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.app.mssql_tds_attempt_composition import create_tds_attempt
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.mssql_tds_worker import TdsAttemptPhase
from tests.test_mssql_tds_directory import LIMITS, PARENT
from tests.test_mssql_tds_directory_journal import OWNER


@pytest.fixture
def setup(tmp_path):
    store = SQLiteWindowStore(tmp_path / "attempt.sqlite", clock=lambda: 1.0)
    lease = store.acquire(PARENT.target_key, "owner", 30)
    pool = TdsActorPool(capacity=2)

    @contextmanager
    def factory():
        yield store

    yield store, lease, pool, factory
    pool.close(deadline=monotonic() + 2)


def create(setup):
    _, lease, pool, factory = setup
    return create_tds_attempt(
        pool, factory, PARENT, LIMITS, lease, supervisor_token=OWNER.supervisor_id, deadline=monotonic() + 2
    )


def test_intent_precedes_directory_and_reservation_is_not_sql_authority(setup, monkeypatch):
    store = setup[0]
    original = store.save
    events = []

    def save(key, *args):
        if key.startswith("mssql-tds-directory"):
            assert TdsAttemptJournal(store).read(PARENT).state.phase is TdsAttemptPhase.CREATION_INTENT
        events.append(key.split("/")[0])
        return original(key, *args)

    monkeypatch.setattr(store, "save", save)
    attempt = create(setup)
    assert events == ["mssql-tds-attempt-v1", "mssql-tds-directory-v1"]
    reserved = attempt.reserve_operation(UUID(int=8), TdsCoordinatorCommand.CREATE, "a" * 64, deadline=monotonic() + 1)
    assert len(reserved.state.slots) == 1
    assert attempt.lifecycle.state.phase is TdsAttemptPhase.CREATION_INTENT
    assert (
        TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store)).read(PARENT, LIMITS) == reserved
    )
    assert not any(hasattr(attempt, name) for name in ("advance", "writer", "gateway", "prepared", "verified"))
    attempt.close(deadline=monotonic() + 1)


@pytest.mark.parametrize("authority_kind", ("published", "aborted"))
def test_verified_parent_is_contained_without_fabricated_error(setup, authority_kind):
    from dpone.contracts.mssql_tds_worker import (
        Exited,
        LaunchIntent,
        ParentAuthority,
        Prepared,
        ProcessRegistered,
        Running,
        TdsObjectIdentity,
        TdsProcessIdentity,
        Verified,
    )

    attempt = create(setup)
    deadline = monotonic() + 2
    lifecycle = attempt._lifecycle
    for event in (
        Prepared(TdsObjectIdentity(1, "a" * 64), "a" * 64),
        LaunchIntent("a" * 64),
        ProcessRegistered(TdsProcessIdentity("a" * 64, str(UUID(int=8)), 123, 1)),
        Running(),
        Exited(0, "a" * 64),
        Verified("a" * 64),
    ):
        lifecycle.advance(event, expected_phase=lifecycle.snapshot.state.phase, deadline=deadline)
    authority = ParentAuthority(authority_kind, "b" * 64)
    contained = attempt.contain_verified_parent(authority, "c" * 64, deadline=deadline)
    assert contained.state.phase is TdsAttemptPhase.CONTAINED
    assert contained.state.parent_authority == authority
    assert contained.state.error is None
    assert contained.state.verification_sha256 == "a" * 64
    attempt.close(deadline=deadline)


def test_unresolved_work_blocks_seal_and_further_reservation(setup):
    from dpone.contracts.bounded_window import WindowOutcomeUnknown

    attempt = create(setup)
    attempt.reserve_operation(UUID(int=8), TdsCoordinatorCommand.CREATE, "a" * 64, deadline=monotonic() + 1)
    before = attempt.directory
    with pytest.raises(WindowOutcomeUnknown):
        attempt.seal_work(deadline=monotonic() + 1)
    with pytest.raises(WindowOutcomeUnknown):
        attempt.reserve_operation(UUID(int=9), TdsCoordinatorCommand.OBSERVE, "b" * 64, deadline=monotonic() + 1)
    assert attempt.directory == before
    assert attempt.lifecycle.state.phase is TdsAttemptPhase.CREATION_INTENT


@pytest.mark.parametrize("kind", ["parent", "directory"])
def test_lost_create_ack_retains_partial_state_without_usable_attempt(setup, monkeypatch, kind):
    from dpone.services.mssql_tds_attempt import TdsAttemptUnknown

    store = setup[0]
    original = store.save

    def save(key, *args):
        result = original(key, *args)
        if key.startswith("mssql-tds-" + ("attempt" if kind == "parent" else "directory")):
            raise OSError("lost acknowledgement")
        return result

    monkeypatch.setattr(store, "save", save)
    with pytest.raises(TdsAttemptUnknown) as caught:
        create(setup)
    assert TdsAttemptJournal(store).read(PARENT).state.phase is TdsAttemptPhase.CREATION_INTENT
    assert (
        TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store)).read(PARENT, LIMITS) is None
    ) == (kind == "parent")
    caught.value.close(deadline=monotonic() + 1)
    assert setup[2].live_count == 0


def test_capacity_exhausted_between_openings_preserves_parent(setup):
    from dpone.services.mssql_tds_attempt import TdsAttemptUnknown

    store, lease, _, factory = setup
    pool = TdsActorPool(capacity=1)
    with pytest.raises(TdsAttemptUnknown):
        create((store, lease, pool, factory))
    assert TdsAttemptJournal(store).read(PARENT) is not None
    assert TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store)).read(PARENT, LIMITS) is None
    assert pool.live_count == 0
    pool.close(deadline=monotonic() + 1)


def saved_contained(setup):
    """Persist an existing observed containment for testing recovery composition.

    This storage fixture is not a production proof producer or live certification.
    """
    from dpone.contracts.mssql_tds_worker import Contained, ContainmentRequired, TdsAttemptError

    store, lease, _, _ = setup
    parent = TdsAttemptJournal(store).create(PARENT, lease, supervisor_token=OWNER.supervisor_id)
    directory = (
        TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store))
        .create(PARENT, LIMITS, lease, supervisor_token=OWNER.supervisor_id)
        .snapshot
    )
    parent.advance(ContainmentRequired(TdsAttemptError.DRIVER), expected_phase=TdsAttemptPhase.CREATION_INTENT)
    parent.advance(Contained("a" * 64), expected_phase=TdsAttemptPhase.CONTAINMENT_REQUIRED)
    store.release(lease)
    new = store.acquire(PARENT.target_key, "successor", 30)
    return parent.snapshot, directory, new


def recover(setup, observations):
    from dpone.app.mssql_tds_attempt_composition import recover_tds_attempt

    _, _, pool, factory = setup
    parent, directory, lease = observations
    return recover_tds_attempt(
        pool, factory, parent, directory, LIMITS, lease, supervisor_token=str(UUID(int=99)), deadline=monotonic() + 1
    )


def test_exact_observed_recovery_serializes_retirement(setup):
    attempt = recover(setup, saved_contained(setup))
    result = attempt.reserve_retirement(UUID(int=10), "b" * 64, deadline=monotonic() + 1)
    assert result.state.work_sealed
    assert result.state.retirement_authority == attempt.lifecycle.state
    assert attempt.lifecycle.state.phase is TdsAttemptPhase.RETIREMENT_REQUIRED
    assert result.state.slots[-1].command is TdsCoordinatorCommand.RETIRE
    assert not result.state.slots[-1].settled
    assert not result.state.admission_closed


@pytest.mark.parametrize("kind", ["parent", "directory"])
def test_lost_retirement_ack_forbids_dependent_reservation(setup, monkeypatch, kind):
    from dpone.contracts.bounded_window import WindowOutcomeUnknown

    attempt = recover(setup, saved_contained(setup))
    store = setup[0]
    original = store.save

    def save(key, revision, payload, lease):
        result = original(key, revision, payload, lease)
        parent_fault = kind == "parent" and key.startswith("mssql-tds-attempt")
        directory_fault = (
            kind == "directory" and key.startswith("mssql-tds-directory") and '"retirement_required"' in payload
        )
        if parent_fault or directory_fault:
            raise OSError("lost acknowledgement")
        return result

    monkeypatch.setattr(store, "save", save)
    with pytest.raises(WindowOutcomeUnknown):
        attempt.reserve_retirement(UUID(int=10), "b" * 64, deadline=monotonic() + 1)
    with pytest.raises(WindowOutcomeUnknown):
        attempt.reserve_retirement(UUID(int=11), "b" * 64, deadline=monotonic() + 1)
    saved = TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store)).read(PARENT, LIMITS)
    assert not saved.state.slots
    assert TdsAttemptJournal(store).read(PARENT).state.phase is TdsAttemptPhase.RETIREMENT_REQUIRED


def test_partial_takeover_failure_can_be_observed_under_further_new_fence(setup, monkeypatch):
    from dpone.services.mssql_tds_attempt import TdsAttemptUnknown

    observations = saved_contained(setup)
    store = setup[0]
    original = store.save

    def save(key, *args):
        if key.startswith("mssql-tds-directory"):
            raise OSError("before directory takeover")
        return original(key, *args)

    with monkeypatch.context() as patch:
        patch.setattr(store, "save", save)
        with pytest.raises(TdsAttemptUnknown):
            recover(setup, observations)
    parent = TdsAttemptJournal(store).read(PARENT)
    directory = TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store)).read(PARENT, LIMITS)
    assert parent.state.ownership.fence > directory.ownership.fence
    store.release(observations[2])
    lease = store.acquire(PARENT.target_key, "third", 30)
    from dpone.app.mssql_tds_attempt_composition import recover_tds_attempt

    attempt = recover_tds_attempt(
        setup[2],
        setup[3],
        parent,
        directory,
        LIMITS,
        lease,
        supervisor_token=str(UUID(int=100)),
        deadline=monotonic() + 1,
    )
    assert attempt.lifecycle.state.ownership == attempt.directory.ownership
    attempt.reserve_retirement(UUID(int=10), "b" * 64, deadline=monotonic() + 1)


def test_foreign_thread_fork_and_reentrant_call_fail_before_effects(setup, monkeypatch):
    import os
    from concurrent.futures import ThreadPoolExecutor

    from dpone.contracts.bounded_window import WindowContractError

    attempt = create(setup)
    before = attempt.directory
    with ThreadPoolExecutor(max_workers=1) as executor:
        with pytest.raises(WindowContractError, match="owner_mismatch"):
            executor.submit(attempt.seal_work, deadline=monotonic() + 1).result()
    pid = os.getpid()
    with monkeypatch.context() as patch:
        patch.setattr(os, "getpid", lambda: pid + 1)
        with pytest.raises(WindowContractError, match="owner_mismatch"):
            attempt.seal_work(deadline=monotonic() + 1)
    original = attempt._lifecycle.assert_authority

    def authority(*, deadline):
        with pytest.raises(WindowContractError, match="reentrant"):
            attempt.close(deadline=deadline)
        original(deadline=deadline)

    monkeypatch.setattr(attempt._lifecycle, "assert_authority", authority)
    assert attempt.directory == before
    from dpone.services.mssql_tds_attempt import TdsAttemptUnknown

    with pytest.raises(TdsAttemptUnknown):
        attempt.seal_work(deadline=monotonic() + 1)
    assert attempt.directory == before


def test_blocked_directory_reservation_cannot_advance_parent_concurrently(setup, monkeypatch):
    import threading

    from dpone.contracts.bounded_window import WindowContractError

    attempt = create(setup)
    entered, finished = threading.Event(), threading.Event()
    original = setup[0].save
    errors = []

    def concurrent():
        assert entered.wait(2)
        try:
            attempt.reserve_retirement(UUID(int=7), "b" * 64, deadline=monotonic() + 1)
        except WindowContractError as error:
            errors.append(str(error))
        finally:
            finished.set()

    def save(key, *args):
        if key.startswith("mssql-tds-directory"):
            entered.set()
            assert finished.wait(2)
        return original(key, *args)

    monkeypatch.setattr(setup[0], "save", save)
    worker = threading.Thread(target=concurrent)
    worker.start()
    attempt.reserve_operation(UUID(int=8), TdsCoordinatorCommand.CREATE, "a" * 64, deadline=monotonic() + 3)
    worker.join(2)
    assert errors == ["mssql_native.tds_attempt_owner_mismatch"]
    assert attempt.lifecycle.state.phase is TdsAttemptPhase.CREATION_INTENT


def test_teardown_signals_both_even_when_first_close_raises(setup, monkeypatch):
    from dpone.services.mssql_tds_attempt import TdsAttemptUnknown

    attempt = create(setup)
    original = attempt._lifecycle.close
    deadlines = []

    def fail(*, deadline):
        deadlines.append(deadline)
        original(deadline=deadline)
        raise RuntimeError("teardown failed")

    original_directory = attempt._directory.close

    def close_directory(*, deadline):
        deadlines.append(deadline)
        original_directory(deadline=deadline)

    monkeypatch.setattr(attempt._lifecycle, "close", fail)
    monkeypatch.setattr(attempt._directory, "close", close_directory)
    deadline = monotonic() + 1
    with pytest.raises(TdsAttemptUnknown) as caught:
        attempt.close(deadline=deadline)
    assert deadlines == [deadline, deadline]
    assert setup[2].live_count == 0
    monkeypatch.setattr(attempt._lifecycle, "close", original)
    caught.value.close(deadline=monotonic() + 1)


@pytest.mark.parametrize("phase", [TdsAttemptPhase.CONTAINED, TdsAttemptPhase.RETIREMENT_REQUIRED])
def test_recovered_retirement_reuses_exact_old_eligibility_with_current_fence(setup, phase):
    from dpone.contracts.mssql_tds_worker import RetirementRequired

    parent, directory, lease = saved_contained(setup)
    store = setup[0]
    # Use a new current writer to arrange an already-authorized durable record.
    token = str(UUID(int=50))
    writer = TdsAttemptJournal(store).take_over(parent, lease, supervisor_token=token)
    index = TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store)).take_over(
        directory, lease, supervisor_token=token
    )
    if phase is TdsAttemptPhase.RETIREMENT_REQUIRED:
        writer.advance(RetirementRequired(), expected_phase=TdsAttemptPhase.CONTAINED)
    index.seal_work()
    saved_authority = writer.snapshot.state
    index.authorize_retirement(saved_authority)
    store.release(lease)
    next_lease = store.acquire(PARENT.target_key, "next", 30)
    attempt = recover(setup, (writer.snapshot, index.snapshot, next_lease))
    result = attempt.reserve_retirement(UUID(int=10), "b" * 64, deadline=monotonic() + 1)
    assert result.state.retirement_authority == saved_authority
    assert result.state.slots[-1].owner_fence == next_lease.fence
    assert attempt.lifecycle.state.phase is TdsAttemptPhase.RETIREMENT_REQUIRED


@pytest.mark.parametrize("mutation", ["proof", "phase", "sequence", "owner", "one_step_token"])
def test_retirement_descendant_rejects_changed_evidence(mutation):
    from dataclasses import replace

    from dpone.contracts.bounded_window import WindowContractError
    from dpone.contracts.mssql_tds_worker import (
        Contained,
        ContainmentRequired,
        TdsAttemptError,
        advance_state,
        initial_state,
    )
    from dpone.services.mssql_tds_attempt import _retirement_descendant

    state = initial_state(PARENT, OWNER)
    state = advance_state(state, ContainmentRequired(TdsAttemptError.DRIVER), expected_phase=state.phase)
    state = advance_state(state, Contained("a" * 64), expected_phase=state.phase)
    changes = {
        "proof": {"observation_sha256": "b" * 64},
        "phase": {"phase": TdsAttemptPhase.CONTAINMENT_REQUIRED},
        "sequence": {"sequence": state.sequence + 1},
        "owner": {"ownership": replace(OWNER, owner="other")},
        "one_step_token": {"ownership": replace(OWNER, fence=OWNER.fence + 1), "sequence": state.sequence + 1},
    }
    with pytest.raises(WindowContractError):
        _retirement_descendant(state, replace(state, **changes[mutation]))


@pytest.mark.parametrize("mutation", ["absent", "limits", "owner", "parent", "token"])
def test_recovery_invalid_bindings_fail_before_takeover(setup, mutation):
    from dataclasses import replace

    from dpone.app.mssql_tds_attempt_composition import recover_tds_attempt

    parent, directory, lease = saved_contained(setup)
    saved_parent = parent
    token = str(UUID(int=99))
    if mutation == "absent":
        directory = None
    elif mutation == "limits":
        directory = replace(
            directory, state=replace(directory.state, limits=replace(LIMITS, max_entries=LIMITS.max_entries + 1))
        )
    elif mutation == "owner":
        directory = replace(directory, ownership=replace(directory.ownership, owner="other"))
    elif mutation == "parent":
        parent = replace(parent, state=replace(parent.state, identity=replace(PARENT, file_sha256="b" * 64)))
    else:
        token = parent.state.ownership.supervisor_id
    with pytest.raises(ValueError):
        recover_tds_attempt(
            setup[2], setup[3], parent, directory, LIMITS, lease, supervisor_token=token, deadline=monotonic() + 1
        )
    assert TdsAttemptJournal(setup[0]).read(PARENT) == saved_parent
    assert setup[2].live_count == 0


@pytest.mark.parametrize("closed", [False, True])
def test_no_retirement_bypass_of_unresolved_work_or_closed_admission(setup, closed):
    from dpone.contracts.bounded_window import WindowContractError, WindowOutcomeUnknown

    attempt = recover(setup, saved_contained(setup))
    if closed:
        attempt.seal_work(deadline=monotonic() + 1)
        attempt.close_admission(deadline=monotonic() + 1)
    else:
        attempt.reserve_retirement(UUID(int=10), "b" * 64, deadline=monotonic() + 1)
    before = attempt.directory
    with pytest.raises((WindowContractError, WindowOutcomeUnknown)):
        attempt.reserve_retirement(UUID(int=11), "b" * 64, deadline=monotonic() + 1)
    assert attempt.directory == before


def test_blocked_teardown_retains_shared_capacity_until_actual_settlement(setup):
    import threading

    from dpone.services.mssql_tds_attempt import TdsAttemptUnknown

    store, lease, pool, _ = setup
    release = threading.Event()
    entered = []

    @contextmanager
    def factory():
        try:
            yield store
        finally:
            entered.append(threading.current_thread())
            assert release.wait(3)

    attempt = create((store, lease, pool, factory))
    start = monotonic()
    try:
        with pytest.raises(TdsAttemptUnknown):
            attempt.close(deadline=start + 0.05)
        assert monotonic() - start < 0.5
        assert pool.live_count == 2
    finally:
        release.set()
        attempt.close(deadline=monotonic() + 1)
    assert len(entered) == 2
    assert pool.live_count == 0


def test_parent_fence_change_during_blocked_directory_create_returns_no_handle(setup, monkeypatch):
    import threading

    from dpone.services.mssql_tds_attempt import TdsAttemptUnknown

    store, lease, _, _ = setup
    entered, changed = threading.Event(), threading.Event()
    original = TdsCoordinatorDirectoryJournal.create
    failures = []

    def create_directory(self, *args, **kwargs):
        writer = original(self, *args, **kwargs)
        entered.set()
        assert changed.wait(2)
        return writer

    def replace_parent():
        try:
            assert entered.wait(2)
            observed = TdsAttemptJournal(store).read(PARENT)
            store.release(lease)
            successor = store.acquire(PARENT.target_key, "successor", 30)
            TdsAttemptJournal(store).take_over(observed, successor, supervisor_token=str(UUID(int=98)))
        except BaseException as error:
            failures.append(error)
        finally:
            changed.set()

    monkeypatch.setattr(TdsCoordinatorDirectoryJournal, "create", create_directory)
    worker = threading.Thread(target=replace_parent)
    worker.start()
    with pytest.raises(TdsAttemptUnknown):
        create(setup)
    worker.join(2)
    assert not failures and changed.is_set()
    assert TdsAttemptJournal(store).read(PARENT).state.ownership.fence > lease.fence
    assert (
        TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store))
        .read(PARENT, LIMITS)
        .ownership.fence
        == lease.fence
    )


def test_real_recovery_allows_nonadjacent_historical_token_reuse(setup):
    from dpone.app.mssql_tds_attempt_composition import recover_tds_attempt

    store, _, pool, factory = setup
    parent, directory, lease = saved_contained(setup)
    token_a, token_b = str(UUID(int=50)), str(UUID(int=51))
    writer = TdsAttemptJournal(store).take_over(parent, lease, supervisor_token=token_a)
    index = TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store)).take_over(
        directory, lease, supervisor_token=token_a
    )
    index.seal_work()
    saved_authority = writer.snapshot.state
    index.authorize_retirement(saved_authority)
    parent, directory = writer.snapshot, index.snapshot
    for token in (token_b, token_a):
        store.release(lease)
        lease = store.acquire(PARENT.target_key, "next", 30)
        attempt = recover_tds_attempt(
            pool, factory, parent, directory, LIMITS, lease, supervisor_token=token, deadline=monotonic() + 1
        )
        parent, directory = attempt.lifecycle, attempt.directory
        if token == token_b:
            attempt.close(deadline=monotonic() + 1)
    result = attempt.reserve_retirement(UUID(int=10), "b" * 64, deadline=monotonic() + 1)
    assert result.state.retirement_authority == saved_authority
    assert result.state.slots[-1].owner_fence == lease.fence
    assert attempt.lifecycle.state.ownership.supervisor_id == saved_authority.ownership.supervisor_id
    assert attempt.lifecycle.state.phase is TdsAttemptPhase.RETIREMENT_REQUIRED


def test_sqlclient_creation_admits_matching_version_two_directory(setup):
    store, lease, pool, factory = setup
    attempt = create_tds_attempt(
        pool,
        factory,
        PARENT,
        LIMITS,
        lease,
        supervisor_token=OWNER.supervisor_id,
        deadline=monotonic() + 2,
        backend="mssql_sqlclient",
    )
    assert attempt.lifecycle.state.schema_version == 2
    assert attempt.directory.state.schema_version == 2
    assert TdsAttemptJournal(store).read(PARENT).state.backend == "mssql_sqlclient"
    attempt.close(deadline=monotonic() + 1)


def test_invalid_backend_rejects_before_store_effects(setup, monkeypatch):
    store, lease, pool, factory = setup

    def forbidden(*args):
        pytest.fail("invalid admission reached store")

    monkeypatch.setattr(store, "save", forbidden)
    with pytest.raises(ValueError):
        create_tds_attempt(
            pool,
            factory,
            PARENT,
            LIMITS,
            lease,
            supervisor_token=OWNER.supervisor_id,
            deadline=monotonic() + 2,
            backend="other",
        )
    assert pool.live_count == 0


def test_sqlclient_recovery_rejects_directory_version_drift_before_writes(setup, monkeypatch):
    from dataclasses import replace

    from dpone.app.mssql_tds_attempt_composition import recover_tds_attempt

    store, lease, pool, factory = setup
    attempt = create_tds_attempt(
        pool,
        factory,
        PARENT,
        LIMITS,
        lease,
        supervisor_token=OWNER.supervisor_id,
        deadline=monotonic() + 2,
        backend="mssql_sqlclient",
    )
    parent, directory = attempt.lifecycle, attempt.directory
    attempt.close(deadline=monotonic() + 1)
    wrong = replace(directory, state=replace(directory.state, schema_version=1))

    def forbidden(*args):
        pytest.fail("mismatched recovery reached store")

    monkeypatch.setattr(store, "save", forbidden)
    with pytest.raises(ValueError, match="recovery_binding_mismatch"):
        recover_tds_attempt(
            pool,
            factory,
            parent,
            wrong,
            LIMITS,
            replace(lease, fence=lease.fence + 1),
            supervisor_token=str(UUID(int=100)),
            deadline=monotonic() + 2,
        )
    assert pool.live_count == 0


def test_observe_retains_original_owner_between_short_calls(setup):
    from dpone.contracts.bounded_window import WindowContractError
    from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity

    _, lease, pool, factory = setup
    deadline = monotonic() + 2
    attempt = create_tds_attempt(
        pool,
        factory,
        PARENT,
        LIMITS,
        lease,
        supervisor_token=OWNER.supervisor_id,
        backend="mssql_sqlclient",
        deadline=deadline,
    )
    reserved = attempt.reserve_operation(UUID(int=200), TdsCoordinatorCommand.OBSERVE, "a" * 64, deadline=deadline)
    slot = reserved.state.slots[-1]
    identity = TdsCoordinatorIdentity(
        PARENT, slot.index, slot.operation_id, slot.command, slot.command_sha256, slot.owner_fence, "b" * 64
    )
    helper = UUID(int=201)
    attempt._begin_observe(helper, identity, deadline=deadline)
    for _ in range(2):
        with attempt._observe_sequence(helper, identity, deadline=deadline):
            attempt._assert_observe(helper, deadline=deadline)
    with pytest.raises(WindowContractError):
        attempt.close(deadline=deadline)
    assert pool.live_count == 2
    attempt._end_observe(helper)
    attempt.close(deadline=deadline)
