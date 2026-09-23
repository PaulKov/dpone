"""Containment retains every capability and the single original cleanup budget."""

from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown
from dpone.app.mssql_sqlclient_departure_supervision import SqlClientCreateDepartureUnknown
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown
from tests.test_mssql_sqlclient_create_departure_composition import harness as harness


@pytest.mark.parametrize("phase", ["create_writer", "create_evidence", "helper_evidence"])
def test_partial_actor_allocation_retained(harness, monkeypatch, phase):
    original = harness.pool.open
    allocated = []
    selected = {
        "create_writer": "TdsCoordinatorActor",
        "create_evidence": "TdsCoordinatorEvidenceActor",
        "helper_evidence": "SqlClientDepartureEvidenceActor",
    }[phase]

    def open_actor(factory, *, deadline):
        gateway = original(factory, deadline=deadline)
        allocated.append(gateway)
        if type(gateway).__name__ == selected:
            raise TdsJournalActorUnknown(gateway)
        return gateway

    monkeypatch.setattr(harness.pool, "open", open_actor)
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert getattr(caught.value.retained, phase) is allocated[-1]
    assert harness.pool.live_count == 0


@pytest.mark.parametrize("phase", ["create", "helper"])
def test_unknown_launch_capability_is_retained_and_close_never_repeated(harness, phase):
    calls = []

    def close():
        calls.append("close")
        raise OSError("synthetic_ambiguous_close")

    unknown = SimpleNamespace(contain=lambda **kw: calls.append(("contain", kw["deadline"])), close=close)

    def spawn(**kw):
        raise TdsLaunchUnknown(unknown)

    (harness.launcher if phase == "create" else harness.helper_launcher).spawn = spawn
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    r = caught.value.retained
    target = r.create_retained if phase == "create" else r
    assert target.unresolved_launch is unknown
    assert target.unresolved_close_attempted and not target.unresolved_closed
    original_deadline = r.containment_deadline
    with pytest.raises(SqlClientCreateDepartureUnknown):
        caught.value.close(deadline=harness.now + 1000.0)
    assert calls.count("close") == 1
    assert all(value == original_deadline for label, value in (c for c in calls if isinstance(c, tuple)))
    assert r.containment_deadline == original_deadline


def test_create_failure_uses_original_retention_and_does_not_repeat_journal_failure(harness):
    harness.fail_at = "child.close"
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    r = caught.value.retained
    assert r.create_retained is not None
    assert r.containment_deadline == r.create_retained.containment_deadline
    assert r.create_retained.child_close_attempted
    assert harness.events.count("child.close") == 1
    assert "helper.spawn" not in harness.events
    with pytest.raises(SqlClientCreateDepartureUnknown):
        caught.value.close(deadline=harness.now + 1000.0)
    assert harness.events.count("child.close") == 1


def test_cleanup_attempts_all_gateways_after_containment_failure(harness):
    harness.fail_at = "helper.terminate"
    harness.helper.startup = lambda **kw: (_ for _ in ()).throw(OSError("startup fault"))
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert "helper.terminate" in harness.events
    assert caught.value.retained.helper_evidence_closed
    assert harness.pool.live_count == 0


def test_failed_budget_capture_never_gets_new_deadline(harness):
    calls = 0

    def clock():
        nonlocal calls
        calls += 1
        raise OSError("clock unavailable")

    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run(clock=clock)
    r = caught.value.retained
    assert r.containment_budget_captured and r.containment_deadline is None
    with pytest.raises(SqlClientCreateDepartureUnknown):
        caught.value.close(deadline=harness.now + 1000.0)
    assert calls == 2


def test_retention_repr_does_not_expose_result_or_material(harness):
    harness.fail_at = "helper.result"
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert caught.value.retained.raw_result
    assert "privatepassword" not in repr(caught.value.retained)
    assert "request_sha256" not in repr(caught.value.retained)


@pytest.mark.parametrize(
    "binding", ["source", "admission", "request", "lease_owner", "lease_target", "lease_fence", "freshness"]
)
def test_original_inputs_fail_before_process_effects(harness, binding):
    from dataclasses import replace

    if binding == "source":
        harness.launcher.implementation_sha256 = "f" * 64
    elif binding == "admission":
        harness.launcher.admission_sha256 = "f" * 64
    elif binding == "request":
        harness.request = replace(harness.request, object_nonce=__import__("uuid").UUID(int=999))
    elif binding.startswith("lease"):
        changes = {
            "lease_owner": {"owner": "other"},
            "lease_target": {"target_id": "other"},
            "lease_fence": {"fence": True},
        }
        harness.lease = replace(harness.lease, **changes[binding])
    else:
        harness.attempt._fresh_creation = False
    with pytest.raises(SqlClientCreateDepartureUnknown):
        harness.run()
    assert "spawn" not in harness.events and "helper.spawn" not in harness.events


def test_historical_create_root_is_compared_to_launcher(harness):
    harness.launcher.package_root = "/different"
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert caught.value.retained.create_outcome is not None
    assert "helper.spawn" not in harness.events


def test_final_context_assertion_failure_prevents_return(harness, monkeypatch):
    from contextlib import contextmanager

    original = harness.attempt._create_departure_sequence

    @contextmanager
    def sequence(*args, **kw):
        with original(*args, **kw) as value:
            yield value
            raise OSError("final_original_journal_ack_lost")

    monkeypatch.setattr(harness.attempt, "_create_departure_sequence", sequence)
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert len(caught.value.retained.receipts) == 6
    assert harness.pool.live_count == 0


def test_final_pool_failure_prevents_return(harness, monkeypatch):
    from dpone.adapters.mssql_sqlclient_departure_evidence_actor import SqlClientDepartureEvidenceActor

    original = SqlClientDepartureEvidenceActor.close

    def fail(**kw):
        raise OSError("final_pool_deadline")

    def close(self, **kw):
        original(self, **kw)
        monkeypatch.setattr(harness.pool, "assert_deadline", fail)

    monkeypatch.setattr(SqlClientDepartureEvidenceActor, "close", close)
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    assert len(caught.value.retained.receipts) == 6


def test_close_refuses_other_thread_before_gateway_io(harness):
    from threading import Thread

    harness.fail_at = "helper.result"
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    retained = caught.value.retained
    seen = []

    def other():
        try:
            caught.value.close(deadline=harness.now + 1000.0)
        except SqlClientCreateDepartureUnknown:
            seen.append("rejected")

    thread = Thread(target=other)
    thread.start()
    thread.join(2)
    assert seen == ["rejected"] and retained.faulted


def test_outcome_construction_expiry_is_checked_inside_attempt_context(harness, monkeypatch):
    import dpone.app.mssql_sqlclient_create_departure_composition as module

    original = module.SqlClientCreateDepartureOutcome
    deadline = harness.now + 100.0

    def construct(*args):
        value = original(*args)
        harness.now = deadline
        return value

    monkeypatch.setattr(module, "SqlClientCreateDepartureOutcome", construct)
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run(operation_deadline=deadline)
    assert len(caught.value.retained.receipts) == 6


def test_constructor_transfers_actual_resources_and_rejects_fabricated_close():
    from uuid import uuid4

    from dpone.app.mssql_sqlclient_departure_supervision import _DepartureRetention

    child = SimpleNamespace(received_result=b"complete")
    args = (None, None, uuid4(), None, None, lambda: 1.0)
    retained = _DepartureRetention(*args, child=child, raw_result=b"complete")
    assert retained.child is retained.helper.child is child
    assert retained.raw_result is retained.helper.raw_result
    assert "child" not in vars(retained) and "raw_result" not in vars(retained)
    with pytest.raises(AttributeError):
        retained.child = SimpleNamespace()
    with pytest.raises(AttributeError):
        retained.helper = None
    with pytest.raises(ValueError, match="close_state_invalid"):
        _DepartureRetention(*args, child=child, child_closed=True)


def test_caught_outer_cleanup_reentry_cannot_report_completion(harness, monkeypatch):
    harness.fail_at = "helper.result"
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()
    retained = caught.value.retained
    original = retained.capture_result

    def capture():
        with pytest.raises(SqlClientCreateDepartureUnknown):
            retained.close(deadline=harness.now + 1000.0)
        original()

    monkeypatch.setattr(retained, "capture_result", capture)
    with pytest.raises(SqlClientCreateDepartureUnknown):
        retained.close(deadline=harness.now + 1000.0)
    monkeypatch.setattr(retained, "capture_result", original)
    with pytest.raises(SqlClientCreateDepartureUnknown):
        retained.close(deadline=harness.now + 1000.0)
