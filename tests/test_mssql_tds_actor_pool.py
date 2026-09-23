"""Shared thread admission precedes backend effects for every actor kind."""

import math
from contextlib import contextmanager
from time import monotonic

import pytest

from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.adapters.mssql_tds_journal_actor import TdsJournalActor
from tests.test_mssql_tds_journal_actor import factory


def test_deadline_after_uses_the_exact_pool_clock_domain():
    pool = TdsActorPool(capacity=1, clock=lambda: 400.0)

    assert pool.deadline_after(2.5) == 402.5


@pytest.mark.parametrize("timeout", [0, -1, math.inf, math.nan, True])
def test_deadline_after_rejects_invalid_timeout(timeout):
    pool = TdsActorPool(capacity=1, clock=lambda: 400.0)

    with pytest.raises(ValueError, match="tds_journal_timeout_invalid"):
        pool.deadline_after(timeout)


@pytest.mark.parametrize("condition", ["expired", "closed", "exhausted"])
def test_rejected_admission_never_constructs_actor(condition):
    pool = TdsActorPool(capacity=1)
    if condition == "closed":
        pool.close(deadline=monotonic() + 1)
    elif condition == "exhausted":
        pool.open(lambda deadline, clock: TdsJournalActor(factory([]), deadline, clock), deadline=monotonic() + 1)
    calls = []

    def forbidden(deadline, clock):
        calls.append(True)
        pytest.fail("rejected admission constructed actor")

    try:
        with pytest.raises(TdsJournalActorUnknown):
            pool.open(forbidden, deadline=monotonic() - 1 if condition == "expired" else monotonic() + 1)
        assert calls == []
    finally:
        pool.close(deadline=monotonic() + 1)


def test_backend_enters_only_after_reservation():
    pool = TdsActorPool(capacity=1)
    seen = []

    @contextmanager
    def backend():
        seen.append(pool.live_count)
        with factory([])() as writer:
            yield writer

    try:
        actor = pool.open(lambda deadline, clock: TdsJournalActor(backend, deadline, clock), deadline=monotonic() + 1)
        assert seen == [1]
        assert actor.snapshot is not None
    finally:
        pool.close(deadline=monotonic() + 1)


@pytest.mark.parametrize("mismatch", ["clock", "deadline", "already_admitted", "owner"])
def test_foreign_construction_contract_is_rejected(mismatch):
    pool = TdsActorPool(capacity=1)

    def build(deadline, clock):
        actor = TdsJournalActor(
            factory([]),
            deadline + (1 if mismatch == "deadline" else 0),
            (lambda: monotonic()) if mismatch == "clock" else clock,
        )
        if mismatch == "already_admitted":
            actor._admitted = True
        if mismatch == "owner":
            actor._pid = -1
        return actor

    with pytest.raises(TdsJournalActorUnknown):
        pool.open(build, deadline=monotonic() + 1)
    assert pool.live_count == 0
    pool.close(deadline=monotonic() + 1)


def test_already_started_or_reused_actor_cannot_take_another_slot():
    pool = TdsActorPool(capacity=2)
    try:
        actor = pool.open(
            lambda deadline, clock: TdsJournalActor(factory([]), deadline, clock), deadline=monotonic() + 1
        )
        with pytest.raises(TdsJournalActorUnknown):
            pool.open(lambda deadline, clock: actor, deadline=actor._ready.deadline)
        assert pool.live_count == 1
        actor.assert_authority(deadline=monotonic() + 1)
    finally:
        pool.close(deadline=monotonic() + 1)


def test_constructor_exceeding_deadline_never_starts_backend():
    now = [1.0]
    pool = TdsActorPool(capacity=1, clock=lambda: now[0])
    calls = []

    @contextmanager
    def forbidden():
        calls.append(True)
        pytest.fail("expired constructor started backend")
        yield

    def build(deadline, clock):
        actor = TdsJournalActor(forbidden, deadline, clock)
        now[0] = deadline
        return actor

    with pytest.raises(TdsJournalActorUnknown):
        pool.open(build, deadline=2.0)
    assert calls == [] and pool.live_count == 0
    pool.close(deadline=3.0)
