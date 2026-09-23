"""Concrete coordinator composition keeps all persistence on its bounded actor."""

from contextlib import contextmanager
from dataclasses import replace
from threading import Event, current_thread
from time import monotonic
from uuid import UUID

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.app.mssql_tds_coordinator_composition import (
    create_tds_coordinator,
    observe_tds_coordinator,
    recover_tds_coordinator,
)
from dpone.contracts.mssql_tds_coordinator import coordinator_key
from dpone.contracts.mssql_tds_directory import directory_key
from tests.test_mssql_tds_coordinator import identity
from tests.test_mssql_tds_coordinator_journal import TOKEN
from tests.test_mssql_tds_directory import LIMITS, PARENT


@pytest.fixture
def prepared(tmp_path):
    store = SQLiteWindowStore(tmp_path / "composition.sqlite", clock=lambda: 1.0)
    lease = store.acquire(PARENT.target_key, "owner", 30)
    parent = TdsAttemptJournal(store).create(PARENT, lease, supervisor_token=TOKEN)
    directory = TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store))
    writer = directory.create(PARENT, LIMITS, lease, supervisor_token=TOKEN)
    operation = replace(identity(), original_fence=lease.fence)
    writer.reserve_operation(
        operation_id=operation.operation_id, command=operation.command, command_sha256=operation.command_sha256
    )

    @contextmanager
    def factory():
        yield store

    return store, lease, parent, directory, operation, factory


def make(pool, prepared, **kwargs):
    return create_tds_coordinator(
        pool,
        prepared[5],
        prepared[4],
        LIMITS,
        prepared[1],
        supervisor_token=TOKEN,
        deadline=monotonic() + 1,
        **kwargs,
    )


def test_real_create_observe_and_absence_do_not_change_parent_directory(prepared):
    store, _, _, _, operation, factory = prepared
    before = store.load(directory_key(PARENT))
    parent_before = TdsAttemptJournal(store).read(PARENT)
    pool = TdsActorPool(capacity=1)
    absent = observe_tds_coordinator(pool, factory, operation, LIMITS, deadline=monotonic() + 1)
    assert absent.identity == operation and absent.limits == LIMITS and absent.snapshot is None
    writer = make(pool, prepared)
    saved = writer.observation.snapshot
    writer.close(deadline=monotonic() + 1)
    observed = observe_tds_coordinator(pool, factory, operation, LIMITS, deadline=monotonic() + 1)
    assert observed.snapshot == saved
    assert store.load(directory_key(PARENT)) == before
    assert TdsAttemptJournal(store).read(PARENT) == parent_before
    assert pool.live_count == 0
    pool.close(deadline=monotonic() + 1)


def test_factory_and_all_io_share_one_actor_context(prepared, monkeypatch):
    store, _, _, _, operation, _ = prepared
    events = []
    active = []
    supervisor = current_thread()
    for name in ("load", "save", "assert_lease"):
        original = getattr(store, name)

        def tracked(*args, method=original, label=name, **kwargs):
            assert active == [current_thread()] and current_thread() is not supervisor
            events.append(label)
            return method(*args, **kwargs)

        monkeypatch.setattr(store, name, tracked)

    @contextmanager
    def factory():
        assert not active and current_thread() is not supervisor
        active.append(current_thread())
        events.append("enter")
        try:
            yield store
        finally:
            assert active.pop() is current_thread()
            events.append("exit")

    pool = TdsActorPool(capacity=1)
    gateway = create_tds_coordinator(
        pool, factory, operation, LIMITS, prepared[1], supervisor_token=TOKEN, deadline=monotonic() + 1
    )
    gateway.close(deadline=monotonic() + 1)
    assert events[0] == "enter" and events[-1] == "exit"
    assert events.count("save") == 1 and events.count("assert_lease") >= 2
    pool.close(deadline=monotonic() + 1)


@pytest.mark.parametrize("bad", ["slot", "owner", "missing", "limits"])
def test_rejects_changed_reservation_without_creating(prepared, bad):
    store, lease, _, _, operation, factory = prepared
    token, limits = TOKEN, LIMITS
    if bad == "slot":
        operation = replace(operation, operation_id=UUID(int=99))
    elif bad == "owner":
        token = str(UUID(int=99))
    elif bad == "limits":
        limits = replace(limits, max_entries=limits.max_entries + 1)
    else:
        operation = replace(operation, parent=replace(operation.parent, attempt=1))
    pool = TdsActorPool(capacity=1)
    with pytest.raises(TdsJournalActorUnknown) as raised:
        create_tds_coordinator(
            pool, factory, operation, limits, lease, supervisor_token=token, deadline=monotonic() + 1
        )
    assert raised.value.gateway is not None
    assert store.load(coordinator_key(operation)) is None
    pool.close(deadline=monotonic() + 1)


def test_takeover_uses_exact_snapshot_without_parent_directory_mutation(prepared):
    store, old_lease, parent, directory, operation, factory = prepared
    pool = TdsActorPool(capacity=1)
    gateway = make(pool, prepared)
    observed = gateway.observation.snapshot
    gateway.close(deadline=monotonic() + 1)
    old_directory = directory.read(PARENT, LIMITS)
    store.release(old_lease)
    lease = store.acquire(PARENT.target_key, "recovery", 30)
    token = str(UUID(int=99))
    TdsAttemptJournal(store).take_over(parent.snapshot, lease, supervisor_token=token)
    directory.take_over(old_directory, lease, supervisor_token=token)
    before = store.load(directory_key(PARENT))
    recovered = recover_tds_coordinator(
        pool, factory, observed, LIMITS, lease, supervisor_token=token, deadline=monotonic() + 1
    )
    assert recovered.observation.snapshot.state.execution_owner == observed.state.execution_owner
    assert recovered.observation.snapshot.state.ownership.fence == lease.fence
    recovered.close(deadline=monotonic() + 1)
    assert store.load(directory_key(PARENT)) == before
    with pytest.raises(TdsJournalActorUnknown):
        recover_tds_coordinator(
            pool, factory, observed, LIMITS, lease, supervisor_token=token, deadline=monotonic() + 1
        )
    assert (
        observe_tds_coordinator(pool, factory, operation, LIMITS, deadline=monotonic() + 1).snapshot.revision
        > observed.revision
    )
    pool.close(deadline=monotonic() + 1)


def test_missing_recovery_and_same_fence_fail_without_store_entry(prepared):
    pool = TdsActorPool(capacity=1)
    gateway = make(pool, prepared)
    snapshot = gateway.observation.snapshot
    gateway.close(deadline=monotonic() + 1)

    @contextmanager
    def forbidden():
        pytest.fail("invalid recovery entered store")
        yield

    for observed in (None, snapshot):
        with pytest.raises(ValueError):
            recover_tds_coordinator(
                pool, forbidden, observed, LIMITS, prepared[1], supervisor_token=TOKEN, deadline=monotonic() + 1
            )
    pool.close(deadline=monotonic() + 1)


@pytest.mark.parametrize("stage", ["entry", "read", "exit"])
def test_stalled_context_retains_capability_and_pool_capacity(prepared, monkeypatch, stage):
    store, _, _, _, operation, _ = prepared
    entered, release = Event(), Event()

    def stall():
        entered.set()
        assert release.wait(2)

    if stage == "read":
        original = store.load

        def load(key):
            stall()
            return original(key)

        monkeypatch.setattr(store, "load", load)

    @contextmanager
    def factory():
        if stage == "entry":
            stall()
        try:
            yield store
        finally:
            if stage == "exit":
                stall()

    pool = TdsActorPool(capacity=1)
    try:
        with pytest.raises(TdsJournalActorUnknown) as raised:
            observe_tds_coordinator(pool, factory, operation, LIMITS, deadline=monotonic() + 0.05)
        assert entered.is_set() and raised.value.gateway is not None
        assert pool.live_count == 1
        with pytest.raises(TdsJournalActorUnknown):
            make(pool, prepared)
    finally:
        release.set()
        pool.close(deadline=monotonic() + 1)
    assert store.load(coordinator_key(operation)) is None


def test_final_teardown_deadline_preserves_unknown_gateway(prepared):
    now = [0.0]

    @contextmanager
    def factory():
        yield prepared[0]
        now[0] = 11.0

    pool = TdsActorPool(capacity=1, clock=lambda: now[0])
    with pytest.raises(TdsJournalActorUnknown) as raised:
        observe_tds_coordinator(pool, factory, prepared[4], LIMITS, deadline=10.0)
    assert raised.value.gateway is not None
    assert pool.live_count == 0
    pool.close(deadline=12.0)


def test_lost_write_ack_is_not_retried_and_retains_gateway(prepared, monkeypatch):
    store = prepared[0]
    original = store.save
    calls = []

    def lose(*args):
        calls.append(args[0])
        original(*args)
        raise OSError("lost ack")

    monkeypatch.setattr(store, "save", lose)
    pool = TdsActorPool(capacity=1)
    with pytest.raises(TdsJournalActorUnknown) as raised:
        make(pool, prepared)
    assert raised.value.gateway is not None
    assert calls == [coordinator_key(prepared[4])]
    assert store.load(calls[0]) is not None
    pool.close(deadline=monotonic() + 1)


@pytest.mark.parametrize("fail_read", [False, True])
def test_failed_read_and_teardown_never_become_absence(prepared, monkeypatch, fail_read):
    def fail(*args):
        raise OSError("unavailable")

    if fail_read:
        monkeypatch.setattr(prepared[0], "load", fail)

    @contextmanager
    def factory():
        try:
            yield prepared[0]
        finally:
            raise RuntimeError("teardown failed")

    pool = TdsActorPool(capacity=1)
    with pytest.raises(TdsJournalActorUnknown) as raised:
        observe_tds_coordinator(pool, factory, prepared[4], LIMITS, deadline=monotonic() + 1)
    assert raised.value.gateway is not None
    with pytest.raises(TdsJournalActorUnknown):
        pool.close(deadline=monotonic() + 1)


def test_shared_pool_lifecycle_actor_blocks_coordinator_factory(prepared):
    from dpone.adapters.mssql_tds_journal_actor import TdsAttemptObserverActor

    @contextmanager
    def lifecycle():
        yield TdsAttemptJournal(prepared[0])

    @contextmanager
    def forbidden():
        pytest.fail("capacity-excluded factory entered")
        yield

    pool = TdsActorPool(capacity=1)
    reader = pool.open(
        lambda deadline, clock: TdsAttemptObserverActor(lifecycle, PARENT, deadline, clock), deadline=monotonic() + 1
    )
    try:
        with pytest.raises(TdsJournalActorUnknown):
            observe_tds_coordinator(pool, forbidden, prepared[4], LIMITS, deadline=monotonic() + 1)
        assert pool.live_count == 1
    finally:
        reader.close(deadline=monotonic() + 1)
        pool.close(deadline=monotonic() + 1)


def test_authority_change_after_save_prevents_returning_gateway(prepared, monkeypatch):
    store, lease = prepared[:2]
    save = store.save

    def revoke(*args):
        record = save(*args)
        store.release(lease)
        return record

    monkeypatch.setattr(store, "save", revoke)
    pool = TdsActorPool(capacity=1)
    with pytest.raises(TdsJournalActorUnknown) as raised:
        make(pool, prepared)
    assert raised.value.gateway is not None
    assert store.load(coordinator_key(prepared[4])) is not None
    pool.close(deadline=monotonic() + 1)


def test_observation_rejects_stable_key_with_changed_full_identity(prepared):
    pool = TdsActorPool(capacity=1)
    gateway = make(pool, prepared)
    gateway.close(deadline=monotonic() + 1)
    changed = replace(prepared[4], command_sha256="f" * 64)
    with pytest.raises(TdsJournalActorUnknown) as raised:
        observe_tds_coordinator(pool, prepared[5], changed, LIMITS, deadline=monotonic() + 1)
    assert raised.value.gateway is not None
    pool.close(deadline=monotonic() + 1)
