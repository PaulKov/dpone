"""Bounded coordinator persistence actors reuse the shared run capacity."""

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.adapters.mssql_tds_coordinator_actor import TdsCoordinatorActor
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.contracts.mssql_tds_coordinator import (
    CoordinatorCredentialIntent,
    CoordinatorFailed,
    CoordinatorProcessRegistered,
    TdsCoordinatorPhase,
    advance_coordinator_state,
)
from dpone.contracts.mssql_tds_worker import TdsAttemptError
from dpone.ports.mssql_tds_coordinator import (
    AdvanceCoordinator,
    AssertCoordinatorAuthority,
    CoordinatorObservation,
    CreateCoordinator,
    ReadCoordinator,
    TakeOverCoordinator,
)
from tests.test_mssql_tds_coordinator import PROCESS
from tests.test_mssql_tds_coordinator_journal import (
    TOKEN,
    create,
    successor,
)
from tests.test_mssql_tds_coordinator_journal import (
    setup as setup,
)
from tests.test_mssql_tds_directory import LIMITS


def open_actor(pool, factory, initialization, deadline=None):
    deadline = monotonic() + 1 if deadline is None else deadline
    return pool.open(
        lambda admitted_deadline, clock: TdsCoordinatorActor(factory, initialization, admitted_deadline, clock),
        deadline=deadline,
    )


def test_read_absence_never_creates_coordinator_record(setup):
    _, _, journal, _, operation = setup

    @contextmanager
    def factory():
        yield journal

    pool = TdsActorPool(capacity=1)
    actor = open_actor(pool, factory, ReadCoordinator(operation, LIMITS))
    try:
        assert actor.observation.snapshot is None
        assert journal.read(operation) is None
    finally:
        pool.close(deadline=monotonic() + 1)


def creation(setup):
    return CreateCoordinator(setup[4], LIMITS, setup[1], TOKEN)


def register():
    return AdvanceCoordinator(CoordinatorProcessRegistered(PROCESS, "a" * 64), TdsCoordinatorPhase.INTENT)


def test_real_context_create_and_advance_stay_on_actor_thread(setup, monkeypatch):
    journal = setup[2]
    events = []
    original = journal.create

    def make(*args, **kwargs):
        events.append(("create", threading.current_thread()))
        writer = original(*args, **kwargs)
        advance = writer.advance

        def apply(*args, **kwargs):
            events.append(("advance", threading.current_thread()))
            return advance(*args, **kwargs)

        monkeypatch.setattr(writer, "advance", apply)
        return writer

    monkeypatch.setattr(journal, "create", make)

    @contextmanager
    def factory():
        events.append(("enter", threading.current_thread()))
        try:
            yield journal
        finally:
            events.append(("exit", threading.current_thread()))

    pool = TdsActorPool(capacity=1)
    actor = open_actor(pool, factory, creation(setup))
    try:
        result = actor.execute(register(), deadline=monotonic() + 1)
        assert result.state.process == PROCESS
        assert actor.observation.snapshot == result
        assert actor.execute(AssertCoordinatorAuthority(), deadline=monotonic() + 1) == result
    finally:
        pool.close(deadline=monotonic() + 1)
    assert [kind for kind, _ in events] == ["enter", "create", "advance", "exit"]
    assert all(thread is events[0][1] and thread is not threading.current_thread() for _, thread in events)


def test_read_only_rejects_assert_advance_and_callback_without_backend_io(setup, monkeypatch):
    snapshot = create(setup).snapshot

    @contextmanager
    def factory():
        yield setup[2]

    pool = TdsActorPool(capacity=1)
    actor = open_actor(pool, factory, ReadCoordinator(setup[4], LIMITS))
    try:
        monkeypatch.setattr(setup[0], "load", lambda *args: pytest.fail("read mode performed extra I/O"))
        for request in (AssertCoordinatorAuthority(), register(), lambda: pytest.fail("callback ran")):
            with pytest.raises(ValueError, match="command_invalid"):
                actor.execute(request, deadline=monotonic() + 1)
        assert actor.observation.snapshot == snapshot
    finally:
        pool.close(deadline=monotonic() + 1)


def test_takeover_exact_snapshot_retains_execution_owner_and_forbids_replay(setup):
    @contextmanager
    def factory():
        yield setup[2]

    pool = TdsActorPool(capacity=1)
    actor = open_actor(pool, factory, creation(setup))
    observed = actor.execute(register(), deadline=monotonic() + 1)
    actor.close(deadline=monotonic() + 1)
    lease, token = successor(setup)
    recovered = open_actor(pool, factory, TakeOverCoordinator(observed, LIMITS, lease, token))
    try:
        assert recovered.observation.snapshot.state.execution_owner == observed.state.execution_owner
        with pytest.raises(WindowOutcomeUnknown):
            recovered.execute(
                AdvanceCoordinator(CoordinatorCredentialIntent(), TdsCoordinatorPhase.PROCESS_REGISTERED),
                deadline=monotonic() + 1,
            )
        assert recovered.observation.snapshot.state.phase is TdsCoordinatorPhase.PROCESS_REGISTERED
    finally:
        pool.close(deadline=monotonic() + 1)


@pytest.mark.parametrize("phase", ["entry", "read", "create", "exit"])
def test_timeout_keeps_actor_capacity_and_excludes_late_initial_ack(setup, monkeypatch, phase):
    entered, release = threading.Event(), threading.Event()
    journal = setup[2]

    def block():
        entered.set()
        assert release.wait(3)

    if phase in ("read", "create"):
        original = getattr(journal, phase)

        def delayed(*args, **kwargs):
            result = original(*args, **kwargs)
            block()
            return result

        monkeypatch.setattr(journal, phase, delayed)

    @contextmanager
    def factory():
        if phase == "entry":
            block()
        try:
            yield journal
        finally:
            if phase == "exit":
                block()

    initialization = creation(setup) if phase == "create" else ReadCoordinator(setup[4], LIMITS)
    pool = TdsActorPool(capacity=1)
    actor = None
    try:
        if phase == "exit":
            actor = open_actor(pool, factory, initialization)
            with pytest.raises(WindowOutcomeUnknown):
                actor.close(deadline=monotonic() + 0.05)
        else:
            with pytest.raises(WindowOutcomeUnknown) as caught:
                open_actor(pool, factory, initialization, deadline=monotonic() + 0.05)
            actor = caught.value.gateway
            with pytest.raises(WindowOutcomeUnknown):
                _ = actor.observation
        assert entered.is_set() and pool.live_count == 1
        with pytest.raises(WindowOutcomeUnknown):
            open_actor(pool, factory, initialization)
    finally:
        release.set()
        actor.close(deadline=monotonic() + 1)
        pool.close(deadline=monotonic() + 1)
    assert pool.live_count == 0
    if phase != "exit":
        with pytest.raises(WindowOutcomeUnknown):
            _ = actor.observation


def test_late_advance_commit_does_not_refresh_acknowledged_state(setup, monkeypatch):
    @contextmanager
    def factory():
        yield setup[2]

    pool = TdsActorPool(capacity=1)
    actor = open_actor(pool, factory, creation(setup))
    before = actor.observation
    entered, release = threading.Event(), threading.Event()
    original = setup[0].save

    def delayed(*args):
        result = original(*args)
        entered.set()
        assert release.wait(3)
        return result

    monkeypatch.setattr(setup[0], "save", delayed)
    try:
        with pytest.raises(WindowOutcomeUnknown) as caught:
            actor.execute(register(), deadline=monotonic() + 0.05)
        assert caught.value.gateway is actor
        assert entered.is_set() and pool.live_count == 1 and actor.observation == before
        with pytest.raises(WindowOutcomeUnknown):
            actor.execute(AssertCoordinatorAuthority(), deadline=monotonic() + 1)
    finally:
        release.set()
        actor.close(deadline=monotonic() + 1)
        pool.close(deadline=monotonic() + 1)
    assert actor.observation == before
    assert setup[2].read(setup[4]).state.phase is TdsCoordinatorPhase.PROCESS_REGISTERED


@pytest.mark.parametrize("bad", ["type", "identity", "phase", "revision"])
def test_bad_backend_advance_reply_poisoned_and_last_ack_retained(setup, monkeypatch, bad):
    journal = setup[2]
    original = journal.create

    def make(*args, **kwargs):
        writer = original(*args, **kwargs)
        before = writer.snapshot
        apply = writer.advance

        def advance(*args, **kwargs):
            result = apply(*args, **kwargs)
            if bad == "type":
                return object()
            if bad == "phase":
                return before
            if bad == "revision":
                return replace(result, revision=before.revision)
            state = replace(result.state, identity=replace(result.state.identity, implementation_sha256="f" * 64))
            return replace(result, state=state)

        monkeypatch.setattr(writer, "advance", advance)
        return writer

    monkeypatch.setattr(journal, "create", make)

    @contextmanager
    def factory():
        yield journal

    pool = TdsActorPool(capacity=1)
    actor = open_actor(pool, factory, creation(setup))
    before = actor.observation
    try:
        with pytest.raises(WindowOutcomeUnknown):
            actor.execute(register(), deadline=monotonic() + 1)
        assert actor.observation == before
        with pytest.raises(WindowOutcomeUnknown):
            actor.execute(AssertCoordinatorAuthority(), deadline=monotonic() + 1)
    finally:
        pool.close(deadline=monotonic() + 1)


@pytest.mark.parametrize(
    "mode,bad",
    [
        (mode, bad)
        for mode in ("read", "create", "takeover")
        for bad in ("type", "identity", "state")
        if (mode, bad) != ("read", "state")
    ],
)
def test_initialization_rejects_wrong_backend_result_binding(setup, monkeypatch, mode, bad):
    journal = setup[2]
    observed = create(setup).snapshot if mode != "create" else None
    if mode == "read":
        original = journal.read
        initialization = ReadCoordinator(setup[4], LIMITS)
    elif mode == "create":
        original = journal.create
        initialization = creation(setup)
    else:
        lease, token = successor(setup)
        original = journal.take_over
        initialization = TakeOverCoordinator(observed, LIMITS, lease, token)

    def corrupt(*args, **kwargs):
        result = original(*args, **kwargs)
        snapshot = result if mode == "read" else result.snapshot
        if bad == "type":
            snapshot = object()
        elif bad == "identity":
            snapshot = replace(
                snapshot,
                state=replace(
                    snapshot.state, identity=replace(snapshot.state.identity, implementation_sha256="f" * 64)
                ),
            )
        else:
            if mode == "create":
                state = advance_coordinator_state(
                    snapshot.state,
                    CoordinatorProcessRegistered(PROCESS, "a" * 64),
                    expected_phase=TdsCoordinatorPhase.INTENT,
                )
                snapshot = replace(snapshot, state=state)
            else:
                snapshot = observed
        return snapshot if mode == "read" else SimpleNamespace(snapshot=snapshot)

    monkeypatch.setattr(journal, "take_over" if mode == "takeover" else mode, corrupt)

    @contextmanager
    def factory():
        yield journal

    pool = TdsActorPool(capacity=1)
    try:
        with pytest.raises(WindowOutcomeUnknown) as caught:
            open_actor(pool, factory, initialization)
        with pytest.raises(WindowOutcomeUnknown):
            _ = caught.value.gateway.observation
    finally:
        pool.close(deadline=monotonic() + 1)


def test_foreign_thread_pid_and_malformed_command_rejected_before_effect(setup, monkeypatch):
    @contextmanager
    def factory():
        yield setup[2]

    pool = TdsActorPool(capacity=1)
    actor = open_actor(pool, factory, creation(setup))
    before = actor.observation
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            with pytest.raises(WindowOutcomeUnknown):
                executor.submit(actor.execute, register(), deadline=monotonic() + 1).result()
        pid = os.getpid()
        with monkeypatch.context() as patch:
            patch.setattr(os, "getpid", lambda: pid + 1)
            with pytest.raises(WindowOutcomeUnknown):
                actor.execute(register(), deadline=monotonic() + 1)
        with pytest.raises(ValueError):
            actor.execute(
                AdvanceCoordinator(lambda: pytest.fail("callback ran"), TdsCoordinatorPhase.INTENT),
                deadline=monotonic() + 1,
            )
        assert actor.observation == before
    finally:
        pool.close(deadline=monotonic() + 1)


def test_idempotent_observation_keeps_revision_and_expired_command_never_saves(setup, monkeypatch):
    @contextmanager
    def factory():
        yield setup[2]

    pool = TdsActorPool(capacity=1)
    actor = open_actor(pool, factory, creation(setup))
    request = AdvanceCoordinator(CoordinatorFailed(TdsAttemptError.FENCING), TdsCoordinatorPhase.INTENT)
    first = actor.execute(request, deadline=monotonic() + 1)
    assert actor.execute(request, deadline=monotonic() + 1) == first
    try:
        monkeypatch.setattr(setup[0], "save", lambda *args: pytest.fail("expired command saved"))
        with pytest.raises(WindowOutcomeUnknown):
            actor.execute(request, deadline=monotonic() - 1)
        assert actor.observation.snapshot == first
    finally:
        pool.close(deadline=monotonic() + 1)


def test_observation_is_closed_snapshot_family():
    with pytest.raises(ValueError):
        CoordinatorObservation(object())


@pytest.mark.parametrize("request_kind", ["assert", "idempotent"])
def test_stale_snapshot_reply_cannot_refresh_assert_or_idempotent_observation(setup, monkeypatch, request_kind):
    journal = setup[2]
    original = journal.create

    def make(*args, **kwargs):
        writer = original(*args, **kwargs)
        if request_kind == "assert":

            def changed():
                writer._snapshot = replace(writer.snapshot, revision=writer.snapshot.revision + 1)

            monkeypatch.setattr(writer, "assert_authority", changed)
        else:
            apply = writer.advance
            calls = []

            def advance(*args, **kwargs):
                result = apply(*args, **kwargs)
                calls.append(1)
                return replace(result, revision=result.revision + 1) if len(calls) > 1 else result

            monkeypatch.setattr(writer, "advance", advance)
        return writer

    monkeypatch.setattr(journal, "create", make)

    @contextmanager
    def factory():
        yield journal

    pool = TdsActorPool(capacity=1)
    actor = open_actor(pool, factory, creation(setup))
    if request_kind == "assert":
        request = AssertCoordinatorAuthority()
    else:
        request = AdvanceCoordinator(CoordinatorFailed(TdsAttemptError.FENCING), TdsCoordinatorPhase.INTENT)
        actor.execute(request, deadline=monotonic() + 1)
    before = actor.observation
    try:
        with pytest.raises(WindowOutcomeUnknown):
            actor.execute(request, deadline=monotonic() + 1)
        assert actor.observation == before
    finally:
        pool.close(deadline=monotonic() + 1)


def test_queued_request_expiring_before_dispatch_never_enters_backend(setup, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    eligible = TdsCoordinatorActor._eligible

    def delayed(self, command):
        if command.kind == "coordinator":
            entered.set()
            assert release.wait(3)
        return eligible(self, command)

    @contextmanager
    def factory():
        yield setup[2]

    pool = TdsActorPool(capacity=1)
    actor = open_actor(pool, factory, creation(setup))
    before = actor.observation
    monkeypatch.setattr(TdsCoordinatorActor, "_eligible", delayed)
    monkeypatch.setattr(setup[0], "save", lambda *args: pytest.fail("expired queued request saved"))
    try:
        with pytest.raises(WindowOutcomeUnknown):
            actor.execute(register(), deadline=monotonic() + 0.05)
        assert entered.is_set() and actor.observation == before
    finally:
        release.set()
        pool.close(deadline=monotonic() + 1)
    assert actor.observation == before
