"""Bounded directory access shares the existing run-scoped actor capacity."""

import threading
from contextlib import contextmanager
from time import monotonic
from uuid import UUID

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.adapters.mssql_tds_directory_actor import TdsDirectoryActor
from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
from dpone.adapters.mssql_tds_journal_actor import TdsJournalActor
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.ports.mssql_tds_directory import (
    AssertDirectoryAuthority,
    CreateDirectory,
    ReadDirectory,
    SealDirectoryWork,
    TakeOverDirectory,
)
from tests.test_mssql_tds_directory import LIMITS, PARENT
from tests.test_mssql_tds_directory_journal import OWNER
from tests.test_mssql_tds_journal_actor import factory as lifecycle_factory


def test_read_absence_is_not_create_and_creation_stays_on_actor(tmp_path):
    store = SQLiteWindowStore(tmp_path / "actor.sqlite", clock=lambda: 1.0)
    lease = store.acquire(PARENT.target_key, "owner", 30)
    TdsAttemptJournal(store).create(PARENT, lease, supervisor_token=OWNER.supervisor_id)

    @contextmanager
    def factory():
        yield TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store))

    pool = TdsActorPool(capacity=1)
    reader = pool.open(
        lambda actor_deadline, actor_clock: TdsDirectoryActor(
            factory, ReadDirectory(PARENT, LIMITS), actor_deadline, actor_clock
        ),
        deadline=monotonic() + 1,
    )
    assert reader.observation.snapshot is None
    reader.close(deadline=monotonic() + 1)
    writer = pool.open(
        lambda actor_deadline, actor_clock: TdsDirectoryActor(
            factory, CreateDirectory(PARENT, LIMITS, lease, OWNER.supervisor_id), actor_deadline, actor_clock
        ),
        deadline=monotonic() + 1,
    )
    assert writer.observation.snapshot.revision == 1
    sealed = writer.execute(SealDirectoryWork(), deadline=monotonic() + 1)
    assert sealed.state.work_sealed
    assert writer.observation.snapshot == sealed
    pool.close(deadline=monotonic() + 1)


@pytest.fixture
def directory(tmp_path):
    store = SQLiteWindowStore(tmp_path / "directory.sqlite", clock=lambda: 1.0)
    lease = store.acquire(PARENT.target_key, "owner", 30)
    parent = TdsAttemptJournal(store).create(PARENT, lease, supervisor_token=OWNER.supervisor_id)
    journal = TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store))

    @contextmanager
    def factory():
        yield journal

    return store, lease, parent, journal, factory


def open_writer(pool, directory):
    return pool.open(
        lambda actor_deadline, actor_clock: TdsDirectoryActor(
            directory[4],
            CreateDirectory(PARENT, LIMITS, directory[1], OWNER.supervisor_id),
            actor_deadline,
            actor_clock,
        ),
        deadline=monotonic() + 1,
    )


@pytest.mark.parametrize("phase", ["initialize", "read", "teardown"])
def test_blocked_directory_consumes_lifecycle_capacity(directory, monkeypatch, phase):
    entered, release = threading.Event(), threading.Event()
    journal = directory[3]
    original = journal.read

    def block():
        entered.set()
        assert release.wait(5)

    def read(*args):
        block()
        return original(*args)

    if phase == "read":
        monkeypatch.setattr(journal, "read", read)

    @contextmanager
    def factory():
        if phase == "initialize":
            block()
        try:
            yield journal
        finally:
            if phase == "teardown":
                block()

    pool = TdsActorPool(capacity=1)
    gateway = None
    try:
        start = monotonic()
        if phase == "teardown":
            gateway = pool.open(
                lambda actor_deadline, actor_clock: TdsDirectoryActor(
                    factory, ReadDirectory(PARENT, LIMITS), actor_deadline, actor_clock
                ),
                deadline=start + 1,
            )
            with pytest.raises(WindowOutcomeUnknown):
                gateway.close(deadline=monotonic() + 0.05)
        else:
            with pytest.raises(WindowOutcomeUnknown) as caught:
                pool.open(
                    lambda actor_deadline, actor_clock: TdsDirectoryActor(
                        factory, ReadDirectory(PARENT, LIMITS), actor_deadline, actor_clock
                    ),
                    deadline=start + 0.05,
                )
            gateway = caught.value.gateway
        assert monotonic() - start < 1 and entered.is_set() and pool.live_count == 1
        box = []
        with pytest.raises(WindowOutcomeUnknown):
            pool.open(
                lambda actor_deadline, actor_clock: TdsJournalActor(
                    lifecycle_factory(box), actor_deadline, actor_clock
                ),
                deadline=monotonic() + 1,
            )
        assert box == []
    finally:
        release.set()
        if gateway is not None:
            gateway.close(deadline=monotonic() + 1)
    assert pool.live_count == 0


def test_blocked_lifecycle_prevents_directory_initialization(directory):
    entered, release, directory_entered = threading.Event(), threading.Event(), threading.Event()

    @contextmanager
    def stuck():
        entered.set()
        assert release.wait(5)
        with lifecycle_factory([])() as writer:
            yield writer

    @contextmanager
    def forbidden():
        directory_entered.set()
        yield directory[3]

    pool = TdsActorPool(capacity=1)
    try:
        with pytest.raises(WindowOutcomeUnknown) as caught:
            pool.open(
                lambda actor_deadline, actor_clock: TdsJournalActor(stuck, actor_deadline, actor_clock),
                deadline=monotonic() + 0.05,
            )
        assert entered.is_set()
        with pytest.raises(WindowOutcomeUnknown):
            pool.open(
                lambda actor_deadline, actor_clock: TdsDirectoryActor(
                    forbidden, ReadDirectory(PARENT, LIMITS), actor_deadline, actor_clock
                ),
                deadline=monotonic() + 1,
            )
        assert not directory_entered.is_set()
    finally:
        release.set()
        caught.value.gateway.close(deadline=monotonic() + 1)


def test_late_commit_never_refreshes_gateway_and_retains_capacity(directory, monkeypatch):
    pool = TdsActorPool(capacity=1)
    gateway = open_writer(pool, directory)
    before = gateway.observation
    entered, release = threading.Event(), threading.Event()
    original = directory[0].save

    def delayed(*args):
        entered.set()
        assert release.wait(5)
        return original(*args)

    monkeypatch.setattr(directory[0], "save", delayed)
    try:
        with pytest.raises(WindowOutcomeUnknown):
            gateway.execute(SealDirectoryWork(), deadline=monotonic() + 0.05)
        assert entered.is_set() and pool.live_count == 1
        assert gateway.observation == before
        with pytest.raises(WindowOutcomeUnknown):
            pool.open(
                lambda actor_deadline, actor_clock: TdsJournalActor(lifecycle_factory([]), actor_deadline, actor_clock),
                deadline=monotonic() + 1,
            )
    finally:
        release.set()
        gateway.close(deadline=monotonic() + 1)
    assert directory[3].read(PARENT, LIMITS).state.work_sealed
    assert gateway.observation == before
    with pytest.raises(WindowOutcomeUnknown):
        gateway.execute(SealDirectoryWork(), deadline=monotonic() + 1)


def test_expired_directory_request_never_enters_writer(directory, monkeypatch):
    from dpone.adapters.mssql_tds_directory_actor import TdsDirectoryActor

    picked, release = threading.Event(), threading.Event()
    original = TdsDirectoryActor._eligible

    def delayed(self, command):
        if command.kind == "directory":
            picked.set()
            assert release.wait(5)
        return original(self, command)

    monkeypatch.setattr(TdsDirectoryActor, "_eligible", delayed)
    pool = TdsActorPool(capacity=1)
    gateway = open_writer(pool, directory)
    before = gateway.observation.snapshot
    try:
        with pytest.raises(WindowOutcomeUnknown):
            gateway.execute(SealDirectoryWork(), deadline=monotonic() + 0.05)
        assert picked.is_set()
    finally:
        release.set()
        gateway.close(deadline=monotonic() + 1)
    assert directory[3].read(PARENT, LIMITS) == before


def test_observation_cannot_acquire_writer_or_accept_callbacks(directory):
    pool = TdsActorPool(capacity=1)
    reader = pool.open(
        lambda actor_deadline, actor_clock: TdsDirectoryActor(
            directory[4], ReadDirectory(PARENT, LIMITS), actor_deadline, actor_clock
        ),
        deadline=monotonic() + 1,
    )
    try:
        with pytest.raises(ValueError):
            reader.execute(SealDirectoryWork(), deadline=monotonic() + 1)
        assert directory[3].read(PARENT, LIMITS) is None
    finally:
        reader.close(deadline=monotonic() + 1)
    writer = open_writer(pool, directory)
    try:
        with pytest.raises(ValueError):
            writer.execute(lambda: pytest.fail("callback ran"), deadline=monotonic() + 1)
        writer.execute(AssertDirectoryAuthority(), deadline=monotonic() + 1)
    finally:
        pool.close(deadline=monotonic() + 1)


def test_takeover_and_barriers_execute_on_actor_thread(directory):
    pool = TdsActorPool(capacity=1)
    writer = open_writer(pool, directory)
    observed = writer.observation.snapshot
    writer.close(deadline=monotonic() + 1)
    store, lease = directory[:2]
    store.release(lease)
    new = store.acquire(PARENT.target_key, "new", 30)
    successor = pool.open(
        lambda actor_deadline, actor_clock: TdsDirectoryActor(
            directory[4], TakeOverDirectory(observed, new, str(UUID(int=3))), actor_deadline, actor_clock
        ),
        deadline=monotonic() + 1,
    )
    try:
        assert successor.observation.snapshot.ownership.fence == new.fence
        successor.execute(SealDirectoryWork(), deadline=monotonic() + 1)
    finally:
        pool.close(deadline=monotonic() + 1)


def test_mixed_pool_close_sends_shutdown_to_both_before_waiting(directory):
    entered = [threading.Event(), threading.Event()]
    release = threading.Event()

    def block(index):
        entered[index].set()
        assert release.wait(5)

    @contextmanager
    def factory():
        try:
            yield directory[3]
        finally:
            block(1)

    pool = TdsActorPool(capacity=2)
    pool.open(
        lambda actor_deadline, actor_clock: TdsJournalActor(
            lifecycle_factory([], teardown=lambda: block(0)), actor_deadline, actor_clock
        ),
        deadline=monotonic() + 1,
    )
    pool.open(
        lambda actor_deadline, actor_clock: TdsDirectoryActor(
            factory, ReadDirectory(PARENT, LIMITS), actor_deadline, actor_clock
        ),
        deadline=monotonic() + 1,
    )
    try:
        start = monotonic()
        with pytest.raises(WindowOutcomeUnknown):
            pool.close(deadline=start + 0.05)
        assert monotonic() - start < 0.5 and all(x.is_set() for x in entered)
        assert pool.live_count == 2
    finally:
        release.set()
        pool.close(deadline=monotonic() + 1)
    for kind in ("lifecycle", "directory"):
        with pytest.raises(WindowOutcomeUnknown):
            if kind == "lifecycle":
                pool.open(
                    lambda actor_deadline, actor_clock: TdsJournalActor(
                        lifecycle_factory([]), actor_deadline, actor_clock
                    ),
                    deadline=monotonic() + 1,
                )
            else:
                pool.open(
                    lambda actor_deadline, actor_clock: TdsDirectoryActor(
                        directory[4], ReadDirectory(PARENT, LIMITS), actor_deadline, actor_clock
                    ),
                    deadline=monotonic() + 1,
                )


def test_all_closed_transitions_recover_and_retire_real_directory(directory):
    from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
    from dpone.contracts.mssql_tds_worker import Contained, ContainmentRequired, TdsAttemptError, TdsAttemptPhase
    from dpone.ports.mssql_tds_directory import (
        AuthorizeDirectoryRetirement,
        CloseDirectoryAdmission,
        RecordDirectoryContainment,
        RecordDirectorySettlement,
        ReserveDirectoryOperation,
        ReserveDirectoryReconciliation,
    )
    from tests.test_mssql_tds_directory import local, remote

    pool = TdsActorPool(capacity=1)
    writer = open_writer(pool, directory)
    observed = writer.execute(
        ReserveDirectoryOperation(UUID(int=1), TdsCoordinatorCommand.CREATE, "a" * 64), deadline=monotonic() + 1
    )
    writer.close(deadline=monotonic() + 1)
    store, lease, parent = directory[:3]
    store.release(lease)
    new = store.acquire(PARENT.target_key, "new", 30)
    token = str(UUID(int=3))
    parent = TdsAttemptJournal(store).take_over(parent.snapshot, new, supervisor_token=token)
    parent.advance(
        ContainmentRequired(TdsAttemptError.OPERATION_TIMEOUT), expected_phase=TdsAttemptPhase.CREATION_INTENT
    )
    authority = parent.advance(Contained("a" * 64), expected_phase=TdsAttemptPhase.CONTAINMENT_REQUIRED).state
    writer = pool.open(
        lambda actor_deadline, actor_clock: TdsDirectoryActor(
            directory[4], TakeOverDirectory(observed, new, token), actor_deadline, actor_clock
        ),
        deadline=monotonic() + 1,
    )

    def execute(request):
        return writer.execute(request, deadline=monotonic() + 1)

    try:
        recovered = execute(ReserveDirectoryReconciliation(UUID(int=2), "a" * 64, 0, local(observed.state)))
        execute(RecordDirectoryContainment(1, local(recovered.state, 1)))
        execute(RecordDirectorySettlement(1, remote(recovered.state, 1)))
        execute(RecordDirectorySettlement(0, remote(recovered.state, 0)))
        execute(SealDirectoryWork())
        execute(AuthorizeDirectoryRetirement(authority))
        retired = execute(ReserveDirectoryOperation(UUID(int=4), TdsCoordinatorCommand.RETIRE, "a" * 64))
        execute(RecordDirectoryContainment(2, local(retired.state, 2)))
        execute(RecordDirectorySettlement(2, remote(retired.state, 2)))
        closed = execute(CloseDirectoryAdmission())
        assert closed.state.admission_closed
        assert directory[3].read(PARENT, LIMITS) == closed
    finally:
        pool.close(deadline=monotonic() + 1)


@pytest.mark.parametrize("phase", ["read", "create", "transition"])
def test_wrong_result_family_fails_closed(directory, monkeypatch, phase):
    journal = directory[3]
    pool = TdsActorPool(capacity=1)
    if phase == "read":
        monkeypatch.setattr(journal, "read", lambda *args: object())
        initialization = ReadDirectory(PARENT, LIMITS)
    else:
        original = journal.create

        def create(*args, **kwargs):
            writer = original(*args, **kwargs)
            if phase == "create":
                return object()
            monkeypatch.setattr(writer, "seal_work", lambda: None)
            return writer

        monkeypatch.setattr(journal, "create", create)
        initialization = CreateDirectory(PARENT, LIMITS, directory[1], OWNER.supervisor_id)
    try:
        if phase != "transition":
            with pytest.raises(WindowOutcomeUnknown):
                pool.open(
                    lambda actor_deadline, actor_clock: TdsDirectoryActor(
                        directory[4], initialization, actor_deadline, actor_clock
                    ),
                    deadline=monotonic() + 1,
                )
        else:
            gateway = pool.open(
                lambda actor_deadline, actor_clock: TdsDirectoryActor(
                    directory[4], initialization, actor_deadline, actor_clock
                ),
                deadline=monotonic() + 1,
            )
            before = gateway.observation
            with pytest.raises(WindowOutcomeUnknown):
                gateway.execute(SealDirectoryWork(), deadline=monotonic() + 1)
            assert gateway.observation == before
    finally:
        pool.close(deadline=monotonic() + 1)


def test_foreign_process_and_thread_cannot_use_directory_gateway(directory, monkeypatch):
    import os
    from concurrent.futures import ThreadPoolExecutor

    pool = TdsActorPool(capacity=1)
    gateway = open_writer(pool, directory)
    before = gateway.observation.snapshot
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            with pytest.raises(WindowOutcomeUnknown):
                executor.submit(gateway.execute, SealDirectoryWork(), deadline=monotonic() + 1).result()
        pid = os.getpid()
        with monkeypatch.context() as patch:
            patch.setattr(os, "getpid", lambda: pid + 1)
            with pytest.raises(WindowOutcomeUnknown):
                gateway.execute(SealDirectoryWork(), deadline=monotonic() + 1)
            pool._lock.acquire()
            try:
                with pytest.raises(WindowOutcomeUnknown):
                    pool.open(
                        lambda actor_deadline, actor_clock: TdsDirectoryActor(
                            directory[4], ReadDirectory(PARENT, LIMITS), actor_deadline, actor_clock
                        ),
                        deadline=monotonic() + 1,
                    )
            finally:
                pool._lock.release()
        assert directory[3].read(PARENT, LIMITS) == before
    finally:
        pool.close(deadline=monotonic() + 1)
