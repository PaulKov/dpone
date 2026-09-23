"""Bounded read-only attempt discovery never creates or takes over journals."""

import threading
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, replace
from time import monotonic
from uuid import UUID

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.adapters.mssql_tds_directory_actor import TdsDirectoryActor
from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
from dpone.adapters.mssql_tds_journal_actor import TdsAttemptObserverActor, TdsJournalActor
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.app.mssql_tds_attempt_composition import observe_tds_attempt
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.contracts.mssql_tds_worker import TdsAttemptObservation
from dpone.services.mssql_tds_attempt import TdsAttemptUnknown
from tests.test_mssql_tds_directory import LIMITS, PARENT
from tests.test_mssql_tds_directory_journal import OWNER


def test_absent_pair_reads_both_journals_with_capacity_one_and_no_writes(tmp_path, monkeypatch):
    store = SQLiteWindowStore(tmp_path / "read.sqlite", clock=lambda: 1.0)
    reads = []
    original = store.load

    def load(key):
        reads.append(key)
        return original(key)

    def forbidden(*args, **kwargs):
        raise AssertionError("observation attempted a write")

    monkeypatch.setattr(store, "load", load)
    monkeypatch.setattr(store, "save", forbidden)
    monkeypatch.setattr(store, "acquire", forbidden)

    @contextmanager
    def factory():
        yield store

    pool = TdsActorPool(capacity=1)
    observed = observe_tds_attempt(pool, factory, PARENT, LIMITS, deadline=monotonic() + 1)
    assert observed.parent is None and observed.directory is None
    assert observed.identity == PARENT and observed.limits == LIMITS
    assert len(reads) == 2
    assert reads[0].startswith("mssql-tds-attempt-v1/")
    assert reads[1].startswith("mssql-tds-directory-v1/")
    assert pool.live_count == 0
    pool.close(deadline=monotonic() + 1)


def test_observer_gateway_has_no_mutation_api_and_acknowledges_absence(tmp_path):
    store = SQLiteWindowStore(tmp_path / "observer.sqlite", clock=lambda: 1.0)

    @contextmanager
    def factory():
        yield TdsAttemptJournal(store)

    pool = TdsActorPool(capacity=1)
    actor = pool.open(
        lambda actor_deadline, actor_clock: TdsAttemptObserverActor(factory, PARENT, actor_deadline, actor_clock),
        deadline=monotonic() + 1,
    )
    try:
        assert actor.observation.snapshot is None
        assert not any(
            hasattr(actor, name) for name in ("advance", "execute", "assert_authority", "create", "take_over")
        )
    finally:
        pool.close(deadline=monotonic() + 1)


@pytest.fixture
def saved(tmp_path):
    store = SQLiteWindowStore(tmp_path / "saved.sqlite", clock=lambda: 1.0)
    lease = store.acquire(PARENT.target_key, "owner", 30)
    parent = TdsAttemptJournal(store).create(PARENT, lease, supervisor_token=OWNER.supervisor_id).snapshot
    directory = (
        TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store))
        .create(PARENT, LIMITS, lease, supervisor_token=OWNER.supervisor_id)
        .snapshot
    )
    return store, lease, parent, directory


def test_present_pair_uses_only_actor_threads_and_is_immutable(saved, monkeypatch):
    store, _, parent, directory = saved
    calls = []
    original = store.load

    def load(key):
        calls.append(("read", threading.current_thread()))
        return original(key)

    def forbidden(*args, **kwargs):
        pytest.fail("discovery attempted mutation")

    monkeypatch.setattr(store, "load", load)
    for name in ("save", "acquire", "renew", "release"):
        monkeypatch.setattr(store, name, forbidden)

    @contextmanager
    def factory():
        calls.append(("enter", threading.current_thread()))
        try:
            yield store
        finally:
            calls.append(("exit", threading.current_thread()))

    pool = TdsActorPool(capacity=1)
    pair = observe_tds_attempt(pool, factory, PARENT, LIMITS, deadline=monotonic() + 1)
    assert pair.parent == parent and pair.directory == directory
    assert [kind for kind, _ in calls] == ["enter", "read", "exit", "enter", "read", "exit"]
    assert all(thread is not threading.current_thread() for _, thread in calls)
    assert calls[0][1] is calls[2][1] and calls[3][1] is calls[5][1]
    with pytest.raises(FrozenInstanceError):
        pair.parent = None
    pool.close(deadline=monotonic() + 1)


def test_absent_parent_does_not_hide_orphan_directory(saved, monkeypatch):
    store, _, _, directory = saved
    original = store.load
    monkeypatch.setattr(store, "load", lambda key: None if key.startswith("mssql-tds-attempt") else original(key))

    @contextmanager
    def factory():
        yield store

    pool = TdsActorPool(capacity=1)
    pair = observe_tds_attempt(pool, factory, PARENT, LIMITS, deadline=monotonic() + 1)
    assert pair.parent is None and pair.directory == directory
    pool.close(deadline=monotonic() + 1)


@pytest.mark.parametrize("phase", ["entry", "read", "exit"])
def test_stalled_observer_retains_capacity_until_actual_settlement(saved, monkeypatch, phase):
    store, _, parent, _ = saved
    entered, release = threading.Event(), threading.Event()
    journal = TdsAttemptJournal(store)
    original = journal.read

    def block():
        entered.set()
        assert release.wait(3)

    def read(identity):
        block()
        return original(identity)

    if phase == "read":
        monkeypatch.setattr(journal, "read", read)

    @contextmanager
    def factory():
        if phase == "entry":
            block()
        try:
            yield journal
        finally:
            if phase == "exit":
                block()

    pool = TdsActorPool(capacity=1)
    gateway = None
    try:
        start = monotonic()
        if phase == "exit":
            gateway = pool.open(
                lambda actor_deadline, actor_clock: TdsAttemptObserverActor(
                    factory, PARENT, actor_deadline, actor_clock
                ),
                deadline=start + 1,
            )
            assert gateway.observation.snapshot == parent
            with pytest.raises(WindowOutcomeUnknown):
                gateway.close(deadline=monotonic() + 0.05)
        else:
            with pytest.raises(WindowOutcomeUnknown) as caught:
                pool.open(
                    lambda actor_deadline, actor_clock: TdsAttemptObserverActor(
                        factory, PARENT, actor_deadline, actor_clock
                    ),
                    deadline=start + 0.05,
                )
            gateway = caught.value.gateway
            with pytest.raises(WindowOutcomeUnknown):
                _ = gateway.observation
        assert entered.is_set() and monotonic() - start < 0.5
        assert pool.live_count == 1
        with pytest.raises(WindowOutcomeUnknown):
            pool.open(
                lambda actor_deadline, actor_clock: TdsAttemptObserverActor(
                    factory, PARENT, actor_deadline, actor_clock
                ),
                deadline=monotonic() + 1,
            )
    finally:
        release.set()
        if gateway is not None:
            gateway.close(deadline=monotonic() + 1)
    assert pool.live_count == 0
    if phase != "exit":
        with pytest.raises(WindowOutcomeUnknown):
            _ = gateway.observation
    pool.close(deadline=monotonic() + 1)


@pytest.mark.parametrize("bad", ["identity", "malformed"])
def test_observer_rejects_wrong_identity_or_result_family(saved, monkeypatch, bad):
    store, _, parent, _ = saved
    journal = TdsAttemptJournal(store)
    result = (
        object()
        if bad == "malformed"
        else replace(parent, state=replace(parent.state, identity=replace(PARENT, file_sha256="b" * 64)))
    )
    monkeypatch.setattr(journal, "read", lambda identity: result)

    @contextmanager
    def factory():
        yield journal

    pool = TdsActorPool(capacity=1)
    with pytest.raises(WindowOutcomeUnknown) as caught:
        pool.open(
            lambda actor_deadline, actor_clock: TdsAttemptObserverActor(factory, PARENT, actor_deadline, actor_clock),
            deadline=monotonic() + 1,
        )
    with pytest.raises(WindowOutcomeUnknown):
        _ = caught.value.gateway.observation
    pool.close(deadline=monotonic() + 1)


@pytest.mark.parametrize("side", ["parent", "directory"])
def test_read_failure_returns_no_partial_pair(saved, monkeypatch, side):
    store = saved[0]
    original = store.load
    reads = []

    def load(key):
        reads.append(key)
        if key.startswith("mssql-tds-" + ("attempt" if side == "parent" else "directory")):
            raise OSError("read acknowledgement unavailable")
        return original(key)

    monkeypatch.setattr(store, "load", load)

    @contextmanager
    def factory():
        yield store

    pool = TdsActorPool(capacity=1)
    with pytest.raises(TdsAttemptUnknown) as caught:
        observe_tds_attempt(pool, factory, PARENT, LIMITS, deadline=monotonic() + 1)
    assert len(reads) == (1 if side == "parent" else 2)
    assert not hasattr(caught.value, "parent") and not hasattr(caught.value, "directory")
    caught.value.close(deadline=monotonic() + 1)
    pool.close(deadline=monotonic() + 1)


def test_pair_teardown_timeout_never_returns_observations_or_opens_second_actor(saved):
    store = saved[0]
    entered, release = threading.Event(), threading.Event()
    count = []

    @contextmanager
    def factory():
        count.append(1)
        try:
            yield store
        finally:
            entered.set()
            assert release.wait(3)

    pool = TdsActorPool(capacity=1)
    try:
        with pytest.raises(TdsAttemptUnknown) as caught:
            observe_tds_attempt(pool, factory, PARENT, LIMITS, deadline=monotonic() + 0.05)
        assert entered.is_set() and count == [1] and pool.live_count == 1
    finally:
        release.set()
        pool.close(deadline=monotonic() + 1)
    caught.value.close(deadline=monotonic() + 1)


def test_all_reads_and_closes_share_one_absolute_deadline(saved, monkeypatch):
    from dpone.adapters.mssql_tds_directory_actor import TdsDirectoryActor
    from dpone.adapters.mssql_tds_journal_actor import TdsAttemptObserverActor

    calls = []
    for cls, method in (
        (TdsActorPool, "open"),
        (TdsAttemptObserverActor, "close"),
        (TdsDirectoryActor, "close"),
    ):
        original = getattr(cls, method)

        def capture(self, *args, _original=original, _method=method, **kwargs):
            calls.append((_method, kwargs["deadline"]))
            return _original(self, *args, **kwargs)

        monkeypatch.setattr(cls, method, capture)

    @contextmanager
    def factory():
        yield saved[0]

    pool = TdsActorPool(capacity=1)
    deadline = monotonic() + 1
    observe_tds_attempt(pool, factory, PARENT, LIMITS, deadline=deadline)
    assert calls == [
        ("open", deadline),
        ("close", deadline),
        ("open", deadline),
        ("close", deadline),
    ]
    pool.close(deadline=monotonic() + 1)


def test_foreign_thread_and_process_cannot_use_observer(saved, monkeypatch):
    import os
    from concurrent.futures import ThreadPoolExecutor

    @contextmanager
    def factory():
        yield TdsAttemptJournal(saved[0])

    pool = TdsActorPool(capacity=1)
    actor = pool.open(
        lambda actor_deadline, actor_clock: TdsAttemptObserverActor(factory, PARENT, actor_deadline, actor_clock),
        deadline=monotonic() + 1,
    )
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            with pytest.raises(WindowOutcomeUnknown):
                executor.submit(lambda: actor.observation).result()
        pid = os.getpid()
        with monkeypatch.context() as patch:
            patch.setattr(os, "getpid", lambda: pid + 1)
            with pytest.raises(WindowOutcomeUnknown):
                _ = actor.observation
            with pytest.raises(WindowOutcomeUnknown):
                pool.open(
                    lambda actor_deadline, actor_clock: TdsAttemptObserverActor(
                        factory, PARENT, actor_deadline, actor_clock
                    ),
                    deadline=monotonic() + 1,
                )
        assert actor.observation.snapshot == saved[2]
    finally:
        pool.close(deadline=monotonic() + 1)


def test_observed_pair_feeds_existing_exact_snapshot_recovery(saved):
    from dpone.app.mssql_tds_attempt_composition import recover_tds_attempt

    store, lease, _, _ = saved

    @contextmanager
    def factory():
        yield store

    reader_pool = TdsActorPool(capacity=1)
    pair = observe_tds_attempt(reader_pool, factory, PARENT, LIMITS, deadline=monotonic() + 1)
    reader_pool.close(deadline=monotonic() + 1)
    store.release(lease)
    new = store.acquire(PARENT.target_key, "successor", 30)
    writer_pool = TdsActorPool(capacity=2)
    try:
        attempt = recover_tds_attempt(
            writer_pool,
            factory,
            pair.parent,
            pair.directory,
            LIMITS,
            new,
            supervisor_token=str(UUID(int=4)),
            deadline=monotonic() + 1,
        )
        assert attempt.lifecycle.state.ownership.fence == new.fence
        assert attempt.lifecycle.state.ownership == attempt.directory.ownership
    finally:
        writer_pool.close(deadline=monotonic() + 1)


def test_observation_envelope_rejects_non_snapshot_and_is_frozen():
    with pytest.raises(ValueError):
        TdsAttemptObservation(object())
    observed = TdsAttemptObservation(None)
    with pytest.raises(FrozenInstanceError):
        observed.snapshot = None


@pytest.mark.parametrize("kind", ["writer", "directory"])
def test_stalled_observer_consumes_other_actor_capacity(saved, kind):
    from dpone.ports.mssql_tds_directory import ReadDirectory

    release, entered = threading.Event(), threading.Event()
    forbidden_calls = []

    @contextmanager
    def blocked():
        entered.set()
        assert release.wait(3)
        yield TdsAttemptJournal(saved[0])

    @contextmanager
    def forbidden():
        forbidden_calls.append(1)
        yield None

    pool = TdsActorPool(capacity=1)
    try:
        with pytest.raises(WindowOutcomeUnknown):
            pool.open(
                lambda actor_deadline, actor_clock: TdsAttemptObserverActor(
                    blocked, PARENT, actor_deadline, actor_clock
                ),
                deadline=monotonic() + 0.05,
            )
        assert entered.is_set()
        with pytest.raises(WindowOutcomeUnknown):
            if kind == "writer":
                pool.open(
                    lambda actor_deadline, actor_clock: TdsJournalActor(forbidden, actor_deadline, actor_clock),
                    deadline=monotonic() + 1,
                )
            else:
                pool.open(
                    lambda actor_deadline, actor_clock: TdsDirectoryActor(
                        forbidden, ReadDirectory(PARENT, LIMITS), actor_deadline, actor_clock
                    ),
                    deadline=monotonic() + 1,
                )
        assert forbidden_calls == [] and pool.live_count == 1
    finally:
        release.set()
        pool.close(deadline=monotonic() + 1)


def test_late_directory_read_produces_no_pair_and_retains_pool_capacity(saved, monkeypatch):
    store = saved[0]
    entered, release = threading.Event(), threading.Event()
    original = store.load

    def load(key):
        if key.startswith("mssql-tds-directory"):
            entered.set()
            assert release.wait(3)
        return original(key)

    monkeypatch.setattr(store, "load", load)

    @contextmanager
    def factory():
        yield store

    pool = TdsActorPool(capacity=1)
    try:
        with pytest.raises(TdsAttemptUnknown) as caught:
            observe_tds_attempt(pool, factory, PARENT, LIMITS, deadline=monotonic() + 0.05)
        assert entered.is_set() and pool.live_count == 1
        assert not hasattr(caught.value, "parent")
    finally:
        release.set()
        pool.close(deadline=monotonic() + 1)
    caught.value.close(deadline=monotonic() + 1)
    assert pool.live_count == 0


def test_pair_rejects_wrong_directory_binding(saved, monkeypatch):
    wrong = replace(saved[3], state=replace(saved[3].state, parent=replace(PARENT, file_sha256="b" * 64)))
    monkeypatch.setattr(TdsCoordinatorDirectoryJournal, "read", lambda *args: wrong)

    @contextmanager
    def factory():
        yield saved[0]

    pool = TdsActorPool(capacity=1)
    with pytest.raises(TdsAttemptUnknown):
        observe_tds_attempt(pool, factory, PARENT, LIMITS, deadline=monotonic() + 1)
    assert pool.live_count == 0
    pool.close(deadline=monotonic() + 1)


def test_final_teardown_crossing_pool_deadline_returns_no_pair(saved):
    now = [0.0]
    contexts = []

    @contextmanager
    def factory():
        contexts.append(1)
        try:
            yield saved[0]
        finally:
            if len(contexts) == 2:
                now[0] = 11.0

    pool = TdsActorPool(capacity=1, clock=lambda: now[0])
    with pytest.raises(TdsAttemptUnknown) as caught:
        observe_tds_attempt(pool, factory, PARENT, LIMITS, deadline=10.0)
    assert now[0] == 11.0 and len(contexts) == 2
    assert pool.live_count == 0
    assert not hasattr(caught.value, "parent") and not hasattr(caught.value, "directory")
    caught.value.close(deadline=20.0)
    pool.close(deadline=20.0)


def test_already_settled_gateway_close_preserves_expired_deadline_behavior(saved):
    now = [0.0]

    @contextmanager
    def factory():
        yield TdsAttemptJournal(saved[0])

    pool = TdsActorPool(capacity=1, clock=lambda: now[0])
    gateway = pool.open(
        lambda actor_deadline, actor_clock: TdsAttemptObserverActor(factory, PARENT, actor_deadline, actor_clock),
        deadline=10.0,
    )
    gateway.close(deadline=10.0)
    now[0] = 11.0
    gateway.close(deadline=10.0)
    with pytest.raises(WindowOutcomeUnknown):
        pool.assert_deadline(deadline=10.0)
    pool.close(deadline=10.0)
