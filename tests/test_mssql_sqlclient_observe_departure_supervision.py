"""Application cleanup remains finite independently of settlement success."""

import os
from threading import current_thread
from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.app.mssql_sqlclient_departure_custody import DepartureHelperCustody
from dpone.app.mssql_sqlclient_observe_departure_supervision import _ObserveDepartureRetention
from dpone.services.mssql_tds_observe_settlement import ObserveSettlement
from dpone.services.mssql_tds_original_continuation import PreparationTransition


def retained(clock):
    origin = object.__new__(PreparationTransition)
    origin.pool = object()
    owner = ObserveSettlement(object(), origin, UUID(int=1), deadline=3.0)
    return _ObserveDepartureRetention(owner, DepartureHelperCustody(clock, os.getpid(), current_thread()), None, clock)


@pytest.mark.parametrize("fails", [False, True])
def test_cleanup_budget_consumed_before_clock_callback(fails):
    calls = []

    def clock():
        calls.append(1)
        state.capture_budget(90.0)
        if fails:
            raise RuntimeError("clock unavailable")
        return 1.0

    state = retained(clock)
    if fails:
        with pytest.raises(RuntimeError):
            state.capture_budget(2.0)
    else:
        state.capture_budget(2.0)
    state.capture_budget(90.0)
    assert calls == [1]
    assert state.helper.containment_budget_captured
    assert state.helper.containment_deadline == (None if fails else 3.0)


def test_cleanup_attempts_independent_resources_in_order():
    calls = []
    state = retained(lambda: 1.0)
    state.owner.close_containment = lambda **kw: calls.append("containment actor")
    state.helper.capture_result = lambda: calls.append("raw EOF")

    def contain(deadline):
        calls.append("child")
        raise RuntimeError("unknown child")

    state.helper.contain = contain
    state.helper.close_evidence = lambda deadline: calls.append("helper actor")
    state.capture_budget(2.0)
    with pytest.raises(Exception):
        state.close(deadline=3.0)
    assert calls == ["raw EOF", "child", "containment actor", "helper actor"]


def test_guard_rejects_custody_replacement_during_owner_callback():
    state = retained(lambda: 1.0)
    original = state.helper

    def change(*, deadline):
        state._helper = DepartureHelperCustody(state.clock, os.getpid(), current_thread())

    state.owner.assert_current = change
    with pytest.raises(Exception):
        state.guard(3.0)
    assert state.faulted
    assert state.helper is original


def test_guard_caught_reentry_cannot_be_acknowledged():
    state = retained(lambda: 1.0)

    def reenter(*, deadline):
        with pytest.raises(Exception):
            state.guard(deadline)

    state.owner.assert_current = reenter
    with pytest.raises(Exception):
        state.guard(3.0)
    assert state.faulted


def test_unknown_evidence_is_retained_before_service_validation():
    state = retained(lambda: 1.0)
    gateway = object()

    def fail(value):
        assert state.helper_evidence is gateway
        raise RuntimeError("service validation")

    state.owner.bind_helper_evidence = fail
    with pytest.raises(RuntimeError):
        state.bind_helper_evidence(gateway)
    assert state.helper_evidence is gateway


def test_complete_raw_result_retained_before_service_capture():
    state = retained(lambda: 1.0)
    raw = b"{}"
    state.helper.retain_child(SimpleNamespace(received_result=raw))

    def fail():
        assert state.raw_result is raw
        raise RuntimeError("service capture failure")

    state.owner.capture_helper_raw_result = fail
    with pytest.raises(RuntimeError):
        state.capture_result()
    assert state.raw_result is raw


def test_receipt_projection_does_not_promote_raw_observation_to_ack():
    from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind

    state = retained(lambda: 1.0)
    record, expected, receipt, observation = object(), object(), object(), object()
    state.owner._helper._records[Kind.LAUNCH_INTENT] = (record, expected, receipt, observation, None)
    assert state.receipts == {}
    state.owner._helper._records[Kind.LAUNCH_INTENT] = (record, expected, receipt, observation, object())
    assert state.receipts == {Kind.LAUNCH_INTENT: receipt}


def test_observation_snapshot_detaches_receipt_projection():
    from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind

    state = retained(lambda: 1.0)
    record, expected, receipt, observation = object(), object(), object(), object()
    state.owner._helper._records[Kind.LAUNCH_INTENT] = (record, expected, receipt, observation, object())
    snapshot = state.snapshot()

    state.owner._helper._records.clear()

    assert snapshot.receipts == {Kind.LAUNCH_INTENT: receipt}
    assert state.receipts == {}
    with pytest.raises(TypeError):
        snapshot.receipts[Kind.LAUNCH_INTENT] = object()


def test_cleanup_uses_original_clock_after_application_field_drift():
    state = retained(lambda: 1.0)
    state.clock = lambda: 100.0
    with pytest.raises(Exception):
        state.guard(3.0)
    state.capture_budget(2.0)
    assert state.containment_deadline == 3.0
