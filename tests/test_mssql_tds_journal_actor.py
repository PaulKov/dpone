"""Real blocked actor threads prove bounded waits without claiming I/O cancellation."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from uuid import uuid4

import pytest

from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.adapters.mssql_tds_journal_actor import TdsJournalActor
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsAttemptOwnership, TdsAttemptSnapshot, initial_state


def snapshot():
    identity = TdsAttemptIdentity(
        "target", "run", 0, 0, "1" * 64, "2" * 64, "3" * 64, "4" * 64, "db", "test", "owned", "5" * 64
    )
    return TdsAttemptSnapshot(initial_state(identity, TdsAttemptOwnership("owner", 1, str(uuid4()))), 1)


class Writer:
    def __init__(self):
        self.owner = threading.current_thread()
        self.snapshot = snapshot()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.block = False
        self.calls = 0

    def assert_authority(self):
        assert threading.current_thread() is self.owner
        self.calls += 1
        self.entered.set()
        if self.block:
            assert self.release.wait(5)

    def advance(self, event, *, expected_phase):
        from dpone.contracts.mssql_tds_worker import advance_state

        self.assert_authority()
        self.snapshot = TdsAttemptSnapshot(
            advance_state(self.snapshot.state, event, expected_phase=expected_phase), self.snapshot.revision + 1
        )
        return self.snapshot


def factory(box, *, teardown=None):
    @contextmanager
    def create():
        writer = Writer()
        box.append(writer)
        try:
            yield writer
        finally:
            assert threading.current_thread() is writer.owner
            if teardown:
                teardown()

    return create


def test_factory_writer_and_teardown_are_actor_owned():
    box = []
    closed = []
    pool = TdsActorPool(capacity=1)
    gateway = pool.open(
        lambda actor_deadline, actor_clock: TdsJournalActor(
            factory(box, teardown=lambda: closed.append(threading.current_thread())), actor_deadline, actor_clock
        ),
        deadline=time.monotonic() + 1,
    )
    gateway.assert_authority(deadline=time.monotonic() + 1)
    assert box[0].owner is not threading.current_thread()
    gateway.close(deadline=time.monotonic() + 1)
    assert closed == [box[0].owner]
    assert pool.live_count == 0


def test_blocked_backend_poison_retains_capacity_until_actual_exit():
    box = []
    pool = TdsActorPool(capacity=1)
    gateway = pool.open(
        lambda actor_deadline, actor_clock: TdsJournalActor(factory(box), actor_deadline, actor_clock),
        deadline=time.monotonic() + 1,
    )
    box[0].block = True
    start = time.monotonic()
    with pytest.raises(WindowOutcomeUnknown):
        gateway.assert_authority(deadline=start + 0.05)
    assert time.monotonic() - start < 1
    assert box[0].entered.is_set()
    assert pool.live_count == 1
    with pytest.raises(WindowOutcomeUnknown):
        gateway.assert_authority(deadline=time.monotonic() + 1)
    with pytest.raises(WindowOutcomeUnknown):
        pool.open(
            lambda actor_deadline, actor_clock: TdsJournalActor(factory([]), actor_deadline, actor_clock),
            deadline=time.monotonic() + 1,
        )
    box[0].release.set()
    gateway.close(deadline=time.monotonic() + 1)
    assert pool.live_count == 0


def test_blocked_initialization_returns_unknown_and_keeps_pool_reservation():
    entered = threading.Event()
    release = threading.Event()

    @contextmanager
    def create():
        entered.set()
        assert release.wait(5)
        yield Writer()

    pool = TdsActorPool(capacity=1)
    try:
        with pytest.raises(WindowOutcomeUnknown) as caught:
            pool.open(
                lambda actor_deadline, actor_clock: TdsJournalActor(create, actor_deadline, actor_clock),
                deadline=time.monotonic() + 0.05,
            )
        assert entered.is_set()
        assert pool.live_count == 1
        with pytest.raises(WindowOutcomeUnknown):
            pool.open(
                lambda actor_deadline, actor_clock: TdsJournalActor(factory([]), actor_deadline, actor_clock),
                deadline=time.monotonic() + 1,
            )
    finally:
        release.set()
        caught.value.gateway.close(deadline=time.monotonic() + 1)
    assert pool.live_count == 0


def test_blocked_teardown_does_not_release_capacity_or_hang_close():
    entered = threading.Event()
    release = threading.Event()

    def teardown():
        entered.set()
        assert release.wait(5)

    pool = TdsActorPool(capacity=1)
    gateway = pool.open(
        lambda actor_deadline, actor_clock: TdsJournalActor(
            factory([], teardown=teardown), actor_deadline, actor_clock
        ),
        deadline=time.monotonic() + 1,
    )
    try:
        with pytest.raises(WindowOutcomeUnknown):
            gateway.close(deadline=time.monotonic() + 0.05)
        assert entered.is_set() and pool.live_count == 1
        with pytest.raises(WindowOutcomeUnknown):
            pool.open(
                lambda actor_deadline, actor_clock: TdsJournalActor(factory([]), actor_deadline, actor_clock),
                deadline=time.monotonic() + 1,
            )
    finally:
        release.set()
        gateway.close(deadline=time.monotonic() + 1)
    replacement = pool.open(
        lambda actor_deadline, actor_clock: TdsJournalActor(factory([]), actor_deadline, actor_clock),
        deadline=time.monotonic() + 1,
    )
    replacement.close(deadline=time.monotonic() + 1)


@pytest.mark.parametrize("kind", ["running", "exited"])
def test_late_commit_never_changes_acknowledged_snapshot_or_allows_retry(kind):
    from dpone.contracts.mssql_tds_worker import (
        Exited,
        LaunchIntent,
        Prepared,
        ProcessRegistered,
        Running,
        TdsAttemptPhase,
        TdsObjectIdentity,
        TdsProcessIdentity,
        advance_state,
    )

    entered = threading.Event()
    release = threading.Event()
    box = []

    @contextmanager
    def create():
        writer = Writer()
        for event in [
            Prepared(TdsObjectIdentity(1, "6" * 64), "7" * 64),
            LaunchIntent("8" * 64),
            ProcessRegistered(TdsProcessIdentity("a" * 64, str(uuid4()), 123, 1)),
        ]:
            writer.snapshot = TdsAttemptSnapshot(
                advance_state(writer.snapshot.state, event, expected_phase=writer.snapshot.state.phase),
                writer.snapshot.revision + 1,
            )
        if kind == "exited":
            writer.snapshot = TdsAttemptSnapshot(
                advance_state(writer.snapshot.state, Running(), expected_phase=TdsAttemptPhase.SPAWNED_WAITING),
                writer.snapshot.revision + 1,
            )
        original = writer.advance

        def delayed(event, *, expected_phase):
            entered.set()
            assert release.wait(5)
            return original(event, expected_phase=expected_phase)

        writer.advance = delayed
        box.append(writer)
        yield writer

    pool = TdsActorPool(capacity=1)
    gateway = pool.open(
        lambda actor_deadline, actor_clock: TdsJournalActor(create, actor_deadline, actor_clock),
        deadline=time.monotonic() + 1,
    )
    old = gateway.snapshot
    event = Running() if kind == "running" else Exited(0, "b" * 64)
    try:
        with pytest.raises(WindowOutcomeUnknown):
            gateway.advance(event, expected_phase=old.state.phase, deadline=time.monotonic() + 0.05)
        assert entered.is_set()
        assert gateway.snapshot == old
    finally:
        release.set()
        gateway.close(deadline=time.monotonic() + 1)
    assert box[0].snapshot.state.phase.value == kind
    assert gateway.snapshot == old
    with pytest.raises(WindowOutcomeUnknown):
        gateway.advance(event, expected_phase=old.state.phase, deadline=time.monotonic() + 1)


def test_expired_queued_command_never_enters_backend(monkeypatch):
    from dpone.adapters.mssql_tds_journal_actor import TdsJournalActor

    box = []
    picked = threading.Event()
    release = threading.Event()
    original = TdsJournalActor._eligible

    def delayed(self, command):
        if command.kind == "assert":
            picked.set()
            assert release.wait(5)
        return original(self, command)

    monkeypatch.setattr(TdsJournalActor, "_eligible", delayed)
    pool = TdsActorPool(capacity=1)
    gateway = pool.open(
        lambda actor_deadline, actor_clock: TdsJournalActor(factory(box), actor_deadline, actor_clock),
        deadline=time.monotonic() + 1,
    )
    try:
        with pytest.raises(WindowOutcomeUnknown):
            gateway.assert_authority(deadline=time.monotonic() + 0.05)
        assert picked.is_set()
    finally:
        release.set()
        gateway.close(deadline=time.monotonic() + 1)
    assert box[0].calls == 0


def test_gateway_rejects_different_supervisor_thread():
    pool = TdsActorPool(capacity=1)
    box = []
    gateway = pool.open(
        lambda actor_deadline, actor_clock: TdsJournalActor(factory(box), actor_deadline, actor_clock),
        deadline=time.monotonic() + 1,
    )
    errors = []

    def other():
        try:
            gateway.assert_authority(deadline=time.monotonic() + 1)
        except WindowOutcomeUnknown:
            errors.append(True)

    thread = threading.Thread(target=other)
    thread.start()
    thread.join(1)
    assert errors == [True] and box[0].calls == 0
    gateway.close(deadline=time.monotonic() + 1)


def test_pool_shutdown_wait_is_bounded_for_all_live_actors():
    pool = TdsActorPool(capacity=2)
    entered = [threading.Event(), threading.Event()]
    release = threading.Event()

    def teardown(index):
        entered[index].set()
        assert release.wait(5)

    gateways = [
        pool.open(
            lambda actor_deadline, actor_clock: TdsJournalActor(
                factory([], teardown=lambda i=i: teardown(i)), actor_deadline, actor_clock
            ),
            deadline=time.monotonic() + 1,
        )
        for i in range(2)
    ]
    try:
        start = time.monotonic()
        with pytest.raises(WindowOutcomeUnknown):
            pool.close(deadline=start + 0.05)
        assert time.monotonic() - start < 0.5 and pool.live_count == 2
        assert all(event.is_set() for event in entered)
    finally:
        release.set()
        pool.close(deadline=time.monotonic() + 1)
    assert pool.live_count == 0
    with pytest.raises(WindowOutcomeUnknown):
        pool.open(
            lambda actor_deadline, actor_clock: TdsJournalActor(factory([]), actor_deadline, actor_clock),
            deadline=time.monotonic() + 1,
        )
    assert gateways


def test_real_journal_writer_remains_actor_thread_owned(tmp_path):
    from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
    from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
    from dpone.contracts.mssql_tds_worker import Prepared, TdsAttemptPhase, TdsObjectIdentity

    box = []

    @contextmanager
    def create():
        store = SQLiteWindowStore(tmp_path / "actor.sqlite", clock=lambda: 1.0)
        lease = store.acquire("target", "owner", 60)
        writer = TdsAttemptJournal(store).create(snapshot().state.identity, lease, supervisor_token=str(uuid4()))
        box.append(writer)
        yield writer
        store.release(lease)

    pool = TdsActorPool(capacity=1)
    gateway = pool.open(
        lambda actor_deadline, actor_clock: TdsJournalActor(create, actor_deadline, actor_clock),
        deadline=time.monotonic() + 2,
    )
    try:
        gateway.assert_authority(deadline=time.monotonic() + 2)
        result = gateway.advance(
            Prepared(TdsObjectIdentity(1, "6" * 64), "7" * 64),
            expected_phase=TdsAttemptPhase.CREATION_INTENT,
            deadline=time.monotonic() + 2,
        )
        assert result == gateway.snapshot and result.state.phase == TdsAttemptPhase.PREPARED
        with pytest.raises(Exception, match="supervisor_thread_mismatch"):
            box[0].assert_authority()
    finally:
        gateway.close(deadline=time.monotonic() + 2)


@pytest.mark.parametrize("capacity", [0, -1, True, 1025, 1.5])
def test_invalid_capacity_does_not_create_actor(capacity):
    with pytest.raises(ValueError):
        TdsActorPool(capacity=capacity)


def test_forked_process_cannot_acquire_inherited_pool_lock(monkeypatch):
    pool = TdsActorPool(capacity=1)
    import os

    original = os.getpid()
    pool._lock.acquire()
    try:
        monkeypatch.setattr("dpone.adapters.mssql_tds_actor_core.os.getpid", lambda: original + 1)
        with pytest.raises(WindowOutcomeUnknown):
            pool.open(
                lambda actor_deadline, actor_clock: TdsJournalActor(factory([]), actor_deadline, actor_clock),
                deadline=time.monotonic() + 1,
            )
    finally:
        pool._lock.release()


def test_arbitrary_callback_is_not_a_command():
    from dpone.contracts.mssql_tds_worker import TdsAttemptPhase

    pool = TdsActorPool(capacity=1)
    gateway = pool.open(
        lambda actor_deadline, actor_clock: TdsJournalActor(factory([]), actor_deadline, actor_clock),
        deadline=time.monotonic() + 1,
    )
    try:
        with pytest.raises(ValueError):
            gateway.advance(lambda: None, expected_phase=TdsAttemptPhase.CREATION_INTENT, deadline=time.monotonic() + 1)
    finally:
        gateway.close(deadline=time.monotonic() + 1)


def test_ambiguous_thread_start_keeps_reservation(monkeypatch):
    entered = threading.Event()
    release = threading.Event()

    @contextmanager
    def create():
        entered.set()
        assert release.wait(5)
        yield Writer()

    original = threading.Thread.start

    def uncertain(thread):
        original(thread)
        assert entered.wait(1)
        raise KeyboardInterrupt

    pool = TdsActorPool(capacity=1)
    with monkeypatch.context() as patch:
        patch.setattr(threading.Thread, "start", uncertain)
        with pytest.raises(WindowOutcomeUnknown) as caught:
            pool.open(
                lambda actor_deadline, actor_clock: TdsJournalActor(create, actor_deadline, actor_clock),
                deadline=time.monotonic() + 1,
            )
    try:
        assert pool.live_count == 1
        with pytest.raises(WindowOutcomeUnknown):
            pool.open(
                lambda actor_deadline, actor_clock: TdsJournalActor(factory([]), actor_deadline, actor_clock),
                deadline=time.monotonic() + 1,
            )
    finally:
        release.set()
        caught.value.gateway.close(deadline=time.monotonic() + 1)
    assert pool.live_count == 0


def test_failed_teardown_is_not_clean_close_but_dead_actor_releases_capacity():
    def teardown():
        raise RuntimeError("private teardown detail")

    pool = TdsActorPool(capacity=1)
    gateway = pool.open(
        lambda actor_deadline, actor_clock: TdsJournalActor(
            factory([], teardown=teardown), actor_deadline, actor_clock
        ),
        deadline=time.monotonic() + 1,
    )
    with pytest.raises(WindowOutcomeUnknown, match="^mssql_native.tds_journal_actor_unknown$"):
        gateway.close(deadline=time.monotonic() + 1)
    assert pool.live_count == 0
    with pytest.raises(WindowOutcomeUnknown):
        pool.close(deadline=time.monotonic() + 1)


def test_sigint_close_does_not_release_still_running_actor():
    import json
    import os
    import subprocess
    import sys
    import textwrap

    script = textwrap.dedent("""
        import json, os, signal, threading, time
        from contextlib import contextmanager
        from dpone.adapters.mssql_tds_actor_core import TdsActorPool
        from dpone.adapters.mssql_tds_journal_actor import TdsJournalActor
        from dpone.contracts.bounded_window import WindowOutcomeUnknown
        from tests.test_mssql_tds_journal_actor import Writer, factory
        entered=threading.Event()
        release=threading.Event()
        exited=threading.Event()
        @contextmanager
        def create():
            try:
                yield Writer()
            finally:
                entered.set()
                release.wait(5)
                exited.set()
        pool=TdsActorPool(capacity=1)
        gateway=pool.open(lambda d,c: TdsJournalActor(create,d,c),deadline=time.monotonic()+1)
        def interrupt():
            entered.wait(1)
            time.sleep(0.03)
            os.kill(os.getpid(),signal.SIGINT)
        timer=threading.Thread(target=interrupt,daemon=True)
        timer.start()
        try:
            gateway.close(deadline=time.monotonic()+1)
        except KeyboardInterrupt:
            pass
        result={'live':pool.live_count,'teardown_exited':exited.is_set()}
        try:
            pool.open(lambda d,c: TdsJournalActor(factory([]),d,c),deadline=time.monotonic()+1)
            result['replacement']=True
        except WindowOutcomeUnknown:
            result['replacement']=False
        release.set()
        pool.close(deadline=time.monotonic()+1)
        print(json.dumps(result))
    """)
    result = subprocess.run(
        [sys.executable, "-c", script], env=os.environ.copy(), capture_output=True, text=True, timeout=5, check=True
    )
    assert json.loads(result.stdout) == {"live": 1, "teardown_exited": False, "replacement": False}
