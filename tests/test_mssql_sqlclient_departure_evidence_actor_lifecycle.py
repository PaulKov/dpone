"""Bounded helper actor lifecycle retains live capacity and actual cleanup ownership."""

import threading
from contextlib import contextmanager
from time import monotonic
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import pytest

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_departure_evidence_actor import SqlClientDepartureEvidenceActor
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceObservation as Observation,
)
from tests.test_mssql_sqlclient_departure_evidence_actor import ATTEMPT, HELPER, item, opened, sink
from tests.test_mssql_sqlclient_departure_evidence_actor import actor as actor


@pytest.mark.parametrize("mutation", ["helper", "attempt", "receipt", "str_alias", "wrong_type"])
def test_bad_initial_ack_retains_live_context_until_actual_exit(monkeypatch, mutation):
    class TextAlias(str):
        pass

    malformed = Observation(HELPER, ATTEMPT)
    field, value = {
        "helper": ("helper_id", UUID(int=7)),
        "attempt": ("attempt_sha256", "9" * 64),
        "receipt": ("receipt", item().receipt),
        "str_alias": ("attempt_sha256", TextAlias(ATTEMPT)),
        "wrong_type": ("helper_id", HELPER),
    }[mutation]
    object.__setattr__(malformed, field, value)
    monkeypatch.setattr(
        SqlClientDepartureEvidenceActor, "_initial", lambda *args: {} if mutation == "wrong_type" else malformed
    )
    exiting, release = threading.Event(), threading.Event()

    @contextmanager
    def factory():
        try:
            yield SimpleNamespace(write=lambda *args: None)
        finally:
            exiting.set()
            assert release.wait(2)

    pool = TdsActorPool(capacity=1)
    try:
        with pytest.raises(TdsJournalActorUnknown) as caught:
            opened(pool, factory)
        gateway = cast(SqlClientDepartureEvidenceActor, caught.value.gateway)
        assert gateway is not None and exiting.wait(1) and pool.live_count == 1
        with pytest.raises(TdsJournalActorUnknown):
            gateway.observation
        with pytest.raises(TdsJournalActorUnknown):
            opened(pool, sink)
    finally:
        release.set()
        pool.close(deadline=monotonic() + 2)
    assert pool.live_count == 0


def test_swallowed_clock_reentry_poison_survives_outer_finally():
    state = dict(armed=False)
    owner = threading.current_thread()

    def clock():
        if state["armed"] and threading.current_thread() is owner:
            state["armed"] = False
            try:
                gateway.write(item(2), deadline=monotonic() + 2)
            except TdsJournalActorUnknown:
                state["swallowed"] = True
        return monotonic()

    pool = TdsActorPool(capacity=1, clock=clock)
    gateway = opened(pool, sink)
    gateway.write(item(0), deadline=monotonic() + 2)
    previous = gateway.observation
    state["armed"] = True
    try:
        with pytest.raises(TdsJournalActorUnknown):
            gateway.write(item(1), deadline=monotonic() + 2)
        assert state["swallowed"] and not gateway._writing and gateway.observation is previous
        with pytest.raises(TdsJournalActorUnknown):
            gateway.write(item(5), deadline=monotonic() + 2)
    finally:
        gateway.close(deadline=monotonic() + 2)


@pytest.mark.parametrize("foreign", ["thread", "process"])
def test_foreign_owner_rejects_before_mutable_actor_access(actor, foreign, monkeypatch):
    from dpone.adapters import mssql_tds_actor_core as core

    failures = []

    def action():
        for operation in (
            lambda: actor.write(item(), deadline=monotonic() + 2),
            lambda: actor.close(deadline=monotonic() + 2),
            lambda: actor.observation,
        ):
            with pytest.raises(TdsJournalActorUnknown):
                operation()
            failures.append(True)

    with monkeypatch.context() as scoped:
        scoped.setattr(actor, "_shutdown", lambda: pytest.fail("foreign owner used inherited synchronization"))
        if foreign == "process":
            scoped.setattr(core.os, "getpid", lambda: actor._pid + 1)
            action()
        else:
            thread = threading.Thread(target=action)
            thread.start()
            thread.join(timeout=2)
            assert not thread.is_alive()
    assert len(failures) == 3 and not actor._attempted and actor.observation.receipt is None


@pytest.mark.parametrize("phase", ["initialize", "write", "fsync", "teardown"])
def test_blocked_io_retains_capacity_and_never_replaces_prior_ack(tmp_path, monkeypatch, phase):
    from dpone.adapters import filesystem_evidence

    entered, release = threading.Event(), threading.Event()
    persisted, exits = [], []
    active = False

    def block():
        entered.set()
        assert release.wait(3)

    if phase == "fsync":
        fsync = filesystem_evidence._fsync

        def blocked_fsync(*args):
            if active:
                block()
            fsync(*args)

        monkeypatch.setattr(filesystem_evidence, "_fsync", blocked_fsync)

    @contextmanager
    def factory():
        if phase == "initialize":
            block()
        writer = DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

        def write(name, payload):
            if active and phase == "write":
                block()
            writer.write(name, payload)
            persisted.append(name)

        try:
            yield SimpleNamespace(write=write)
        finally:
            exits.append(True)
            if phase == "teardown":
                block()

    pool = TdsActorPool(capacity=1)
    gateway, previous = None, None
    try:
        if phase == "initialize":
            with pytest.raises(TdsJournalActorUnknown) as caught:
                opened(pool, factory, monotonic() + 0.05)
            gateway = cast(SqlClientDepartureEvidenceActor, caught.value.gateway)
        else:
            gateway = opened(pool, factory)
            gateway.write(item(), deadline=monotonic() + 2)
            previous = gateway.observation
            active = True
            with pytest.raises(TdsJournalActorUnknown):
                if phase == "teardown":
                    gateway.close(deadline=monotonic() + 0.05)
                else:
                    gateway.write(item(1), deadline=monotonic() + 0.05)
            assert gateway.observation is previous
        assert entered.is_set() and pool.live_count == 1
        with pytest.raises(TdsJournalActorUnknown):
            opened(pool, sink)
    finally:
        release.set()
        if gateway is not None:
            gateway.close(deadline=monotonic() + 2)
    assert pool.live_count == 0 and len(exits) == 1
    assert gateway is not None
    if previous is not None:
        assert gateway.observation is previous
    if phase in {"write", "fsync"}:
        assert len(persisted) == 2
        with pytest.raises(ValueError, match="kind_consumed"):
            gateway.write(item(1), deadline=monotonic() + 2)
    usable = opened(pool, sink)
    usable.close(deadline=monotonic() + 2)


@pytest.mark.parametrize("phase", ["construct", "enter", "write", "exit"])
@pytest.mark.parametrize("error", [OSError, KeyboardInterrupt])
def test_backend_exception_constant_unknown_and_exact_teardown(phase, error):
    exits = []

    class Context:
        def __enter__(self):
            if phase == "enter":
                raise error("private-canary")

            def write(*args):
                if phase == "write":
                    raise error("private-canary")

            return SimpleNamespace(write=write)

        def __exit__(self, *args):
            exits.append(True)
            if phase == "exit":
                raise error("private-canary")

    def factory():
        if phase == "construct":
            raise error("private-canary")
        return Context()

    pool = TdsActorPool(capacity=1)
    if phase in {"construct", "enter"}:
        with pytest.raises(TdsJournalActorUnknown) as caught:
            opened(pool, factory)
        gateway = cast(SqlClientDepartureEvidenceActor, caught.value.gateway)
    else:
        gateway = opened(pool, factory)
        if phase == "write":
            with pytest.raises(TdsJournalActorUnknown) as caught:
                gateway.write(item(), deadline=monotonic() + 2)
            assert gateway.observation.receipt is None
        else:
            receipt = gateway.write(item(), deadline=monotonic() + 2)
            with pytest.raises(TdsJournalActorUnknown) as caught:
                gateway.close(deadline=monotonic() + 2)
            assert gateway.observation.receipt == receipt
    assert caught.value.gateway is gateway and "private" not in str(caught.value)
    if phase == "exit":
        with pytest.raises(TdsJournalActorUnknown):
            gateway.close(deadline=monotonic() + 2)
    else:
        gateway.close(deadline=monotonic() + 2)
    assert len(exits) == (0 if phase in {"construct", "enter"} else 1) and pool.live_count == 0


@pytest.mark.parametrize("conflict", [False, True])
def test_existing_bytes_via_fresh_actor_are_not_execution_retry_authority(tmp_path, conflict):
    value = item()
    target = tmp_path / value.receipt.relative_name

    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    pool = TdsActorPool(capacity=1)
    for index in range(2):
        if index and conflict:
            target.write_bytes(b"conflicting-content")
        gateway = opened(pool, factory)
        if index and conflict:
            with pytest.raises(TdsJournalActorUnknown):
                gateway.write(value, deadline=monotonic() + 2)
            assert gateway.observation.receipt is None and target.read_bytes() == b"conflicting-content"
        else:
            assert gateway.write(value, deadline=monotonic() + 2) == value.receipt
        gateway.close(deadline=monotonic() + 2)
    assert len(list(tmp_path.iterdir())) == 1


def test_gateway_close_leaves_unrelated_actor_and_shared_pool_usable():
    pool = TdsActorPool(capacity=2)
    first, second = opened(pool, sink), opened(pool, sink)
    first.close(deadline=monotonic() + 2)
    assert second.write(item(), deadline=monotonic() + 2) == item().receipt
    third = opened(pool, sink)
    assert pool.live_count == 2
    second.close(deadline=monotonic() + 2)
    third.close(deadline=monotonic() + 2)


def test_concurrent_admission_respects_shared_capacity():
    start, finished = threading.Barrier(5), threading.Event()
    release, lock = threading.Event(), threading.Lock()
    entries, failures = [], []
    pool = TdsActorPool(capacity=2)

    @contextmanager
    def factory():
        with lock:
            entries.append(threading.get_ident())
        assert release.wait(3)
        yield SimpleNamespace(write=lambda *args: None)

    def contender():
        gateway = None
        start.wait(timeout=2)
        try:
            opened(pool, factory, monotonic() + 0.05)
        except TdsJournalActorUnknown as error:
            gateway = error.gateway
            with lock:
                failures.append(error)
                if len(failures) == 4:
                    finished.set()
        finally:
            assert release.wait(3)
            if gateway is not None:
                gateway.close(deadline=monotonic() + 2)

    threads = [threading.Thread(target=contender) for _ in range(4)]
    for thread in threads:
        thread.start()
    start.wait(timeout=2)
    try:
        assert finished.wait(2)
        assert len(entries) == pool.live_count == 2
        assert sum(error.gateway is not None for error in failures) == 2
    finally:
        release.set()
        for thread in threads:
            thread.join(timeout=2)
    assert not any(thread.is_alive() for thread in threads) and pool.live_count == 0
