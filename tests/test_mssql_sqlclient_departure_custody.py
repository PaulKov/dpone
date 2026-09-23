"""Helper resources remain single-owned through ambiguous effects and actor waits."""

import os
from threading import current_thread
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dpone.app.mssql_sqlclient_departure_custody import DepartureHelperCustody
from dpone.app.mssql_sqlclient_departure_supervision import SqlClientCreateDepartureUnknown, _DepartureRetention
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.contracts.mssql_tds_worker import TdsChildExit, TdsProcessIdentity

PROCESS = TdsProcessIdentity("a" * 64, "11111111-1111-1111-1111-111111111111", 123, 456)


def test_unresolved_close_caught_reentry_stays_unknown():
    owner = custody()
    calls = []

    def close():
        calls.append("close")
        with pytest.raises(WindowOutcomeUnknown):
            owner.contain(3.0)

    owner.retain_unresolved(SimpleNamespace(contain=lambda **kw: calls.append("contain"), close=close))
    owner.bind_cleanup_deadline_once(3.0)
    with pytest.raises(WindowOutcomeUnknown):
        owner.contain(3.0)
    assert owner.unresolved_close_attempted and not owner.unresolved_closed
    with pytest.raises(WindowOutcomeUnknown):
        owner.contain(3.0)
    assert calls == ["contain", "close"]


def test_outer_cleanup_preserves_caught_unresolved_reentry():
    retained = _DepartureRetention(SimpleNamespace(close=lambda **kw: None), None, uuid4(), None, None, lambda: 1.0)

    def close():
        with pytest.raises(WindowOutcomeUnknown):
            retained.helper.contain(3.0)

    retained.helper.retain_unresolved(SimpleNamespace(contain=lambda **kw: None, close=close))
    retained.helper.bind_cleanup_deadline_once(3.0)
    with pytest.raises(SqlClientCreateDepartureUnknown):
        retained.close(deadline=3.0)
    assert not retained.unresolved_closed


@pytest.mark.parametrize("first_fails", [False, True])
@pytest.mark.parametrize("create_budget", [False, True])
def test_cleanup_budget_is_consumed_before_external_producer(first_fails, create_budget):
    calls = []

    def clock():
        calls.append("clock")
        if len(calls) == 1:
            retained.capture_budget(100.0)
            if first_fails:
                raise OSError("original clock failed")
        return 1.0

    original = (
        SimpleNamespace(capture_cleanup_budget=lambda timeout, clock: clock() + timeout) if create_budget else None
    )
    retained = _DepartureRetention(None, None, uuid4(), None, None, clock, create_retained=original)
    if first_fails:
        with pytest.raises(OSError):
            retained.capture_budget(2.0)
    else:
        retained.capture_budget(2.0)
    retained.capture_budget(200.0)
    assert calls == ["clock"]
    assert retained.containment_budget_captured
    assert retained.containment_deadline == (None if first_fails else 3.0)


def test_pending_or_failed_budget_capture_cannot_be_replaced():
    owner = custody()
    assert owner.begin_cleanup_deadline_capture()
    assert not owner.begin_cleanup_deadline_capture()
    owner.bind_cleanup_deadline_once(100.0)
    assert owner.containment_deadline is None
    owner.finish_cleanup_deadline_capture(None)
    with pytest.raises(WindowOutcomeUnknown):
        owner.finish_cleanup_deadline_capture(100.0)
    owner.bind_cleanup_deadline_once(200.0)
    assert owner.containment_budget_captured and owner.containment_deadline is None


def custody(clock=lambda: 1.0):
    return DepartureHelperCustody(clock, os.getpid(), current_thread())


def test_capture_precedes_metadata_and_replacement_is_rejected():
    owner = custody()
    child = SimpleNamespace(identity=None, received_result=b"complete", close=lambda: None)
    owner.retain_child(child)
    with pytest.raises(ValueError):
        owner.record_process(child.identity)
    assert owner.child is child
    with pytest.raises(WindowOutcomeUnknown):
        owner.retain_child(SimpleNamespace())
    with pytest.raises(WindowOutcomeUnknown):
        owner.retain_unresolved(SimpleNamespace())
    owner.capture_result()
    assert owner.raw_result == b"complete"
    with pytest.raises(AttributeError):
        owner.child = None


def test_snapshot_is_stable_while_compatibility_projection_stays_current():
    owner = custody()
    initial = owner.snapshot()
    child = SimpleNamespace(received_result=b"complete")

    owner.retain_child(child)
    owner.capture_result()

    assert initial.child is None and initial.raw_result is None
    assert owner.child is child and owner.raw_result == b"complete"


@pytest.mark.parametrize("after_effect", [False, True])
def test_ambiguous_raw_close_never_replays_on_reused_resource(after_effect):
    owner = custody()
    calls = []
    slot = {"resource": "original"}

    def close():
        calls.append(slot["resource"])
        if after_effect:
            slot["resource"] = "replacement"
        raise OSError("ambiguous")

    owner.retain_child(SimpleNamespace(close=close, received_result=b"eof"))
    owner.capture_result()
    with pytest.raises(OSError):
        owner.close_child()
    with pytest.raises(WindowOutcomeUnknown):
        owner.close_child()
    assert calls == ["original"] and owner.raw_result == b"eof"
    assert owner.child_close_attempted and not owner.child_closed


def test_failed_cleanup_capture_is_sticky_and_deadline_clamps():
    owner = custody()
    owner.bind_cleanup_deadline_once(None)
    owner.bind_cleanup_deadline_once(100.0)
    assert owner.containment_budget_captured and owner.containment_deadline is None
    with pytest.raises(WindowOutcomeUnknown):
        owner.contain(200.0)
    owner = custody()
    calls = []
    owner.retain_unresolved(SimpleNamespace(contain=lambda **kw: calls.append(kw["deadline"]), close=lambda: None))
    owner.bind_cleanup_deadline_once(3.0)
    owner.bind_cleanup_deadline_once(100.0)
    owner.contain(200.0)
    owner.contain(200.0)
    assert calls == [3.0]


def test_actual_reaped_nonzero_exit_is_retained_for_containment():
    owner = custody()
    owner.retain_child(SimpleNamespace(identity=PROCESS, close=lambda: None))
    owner.record_process(PROCESS)
    result = TdsChildExit(PROCESS, 3, True)
    owner.record_exit(result)
    assert owner.local_exit is result
    with pytest.raises(ValueError):
        owner.record_exit(
            TdsChildExit(TdsProcessIdentity("a" * 64, "11111111-1111-1111-1111-111111111111", 124, 456), 0, True)
        )


def test_actual_actor_can_wait_again_without_repeating_backend_teardown():
    from contextlib import contextmanager
    from threading import Event
    from time import monotonic

    from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
    from tests.test_mssql_sqlclient_departure_evidence_actor import opened

    release, exiting = Event(), Event()
    calls = []

    @contextmanager
    def factory():
        try:
            yield SimpleNamespace(write=lambda *args: None)
        finally:
            calls.append("backend_close")
            exiting.set()
            assert release.wait(3)

    pool = TdsActorPool(capacity=1)
    owner = custody(monotonic)
    gateway = opened(pool, factory)
    owner.retain_evidence(gateway)
    owner.bind_cleanup_deadline_once(monotonic() + 3.0)
    try:
        with pytest.raises(TdsJournalActorUnknown):
            owner.close_evidence(monotonic() + 0.03)
        assert exiting.wait(1)
        assert not owner.helper_evidence_closed
        release.set()
        owner.close_evidence(monotonic() + 2.0)
        assert owner.helper_evidence_closed and calls == ["backend_close"]
    finally:
        release.set()
        pool.close(deadline=monotonic() + 2.0)


@pytest.mark.parametrize("raw", [None, b"", b"x" * 32769, bytearray(b"eof")])
def test_incomplete_or_unbounded_result_never_retained(raw):
    owner = custody()
    owner.retain_child(SimpleNamespace(received_result=raw))
    owner.capture_result()
    assert owner.raw_result is None


def test_caught_containment_reentry_stays_poisoned():
    owner = custody()
    calls = []

    def clock():
        with pytest.raises(WindowOutcomeUnknown):
            owner.contain(3.0)
        return 1.0

    owner._clock = clock
    owner.retain_unresolved(
        SimpleNamespace(contain=lambda **kw: calls.append("contain"), close=lambda: calls.append("close"))
    )
    owner.bind_cleanup_deadline_once(3.0)
    with pytest.raises(WindowOutcomeUnknown):
        owner.contain(3.0)
    owner._clock = lambda: 1.0
    with pytest.raises(WindowOutcomeUnknown):
        owner.contain(3.0)
    assert calls == []


def test_foreign_thread_cannot_close_resource():
    from threading import Thread

    owner = custody()
    calls = []
    owner.retain_child(SimpleNamespace(close=lambda: calls.append("close")))

    def other():
        with pytest.raises(WindowOutcomeUnknown):
            owner.close_child()

    thread = Thread(target=other)
    thread.start()
    thread.join(2)
    assert not thread.is_alive() and calls == []


def test_unknown_evidence_allocation_none_cannot_be_replaced():
    owner = custody()
    owner.retain_evidence(None)
    with pytest.raises(WindowOutcomeUnknown):
        owner.retain_evidence(SimpleNamespace())


def test_caught_child_close_reentry_cannot_acknowledge_close():
    owner = custody()
    calls = []

    def close():
        calls.append("close")
        with pytest.raises(WindowOutcomeUnknown):
            owner.close_child()

    owner.retain_child(SimpleNamespace(close=close))
    with pytest.raises(WindowOutcomeUnknown):
        owner.close_child()
    assert calls == ["close"] and not owner.child_closed


def test_caught_evidence_close_reentry_cannot_acknowledge_close():
    owner = custody()
    calls = []

    def close(**kw):
        calls.append("close")
        with pytest.raises(WindowOutcomeUnknown):
            owner.close_evidence(3.0)

    owner.retain_evidence(SimpleNamespace(close=close))
    with pytest.raises(WindowOutcomeUnknown):
        owner.close_evidence(3.0)
    assert calls == ["close"] and not owner.helper_evidence_closed
