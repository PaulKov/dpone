"""Observed evidence and exact containment capability survive every late failure."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.app.mssql_tds_coordinator_request import successful_response
from dpone.app.mssql_tds_coordinator_supervision import TdsCoordinatorSupervisionUnknown, fail_coordinator
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.contracts.mssql_tds_coordinator import (
    CoordinatorResultReceived,
    TdsCoordinatorSnapshot,
    advance_coordinator_state,
    take_over_coordinator_state,
)
from dpone.contracts.mssql_tds_coordinator_evidence import TdsCoordinatorEvidenceKind as Kind
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown
from tests.test_mssql_tds_coordinator_supervisor import Harness


@pytest.mark.parametrize(
    "fault",
    [
        "result",
        "evidence:result",
        "wait",
        "child.close",
        "ack:CoordinatorResultReceived",
        "evidence:local_exit",
        "ack:CoordinatorLocalObserved",
        "evidence.close",
        "writer.close",
    ],
)
def test_validated_result_survives_every_late_failure(fault):
    h = Harness()
    h.fail_at = fault
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    retained = caught.value.retained
    assert retained.raw_result == h.received_result
    assert retained.response.evidence == h.proof
    assert retained.child is h.child and retained.writer is h.writer and retained.evidence is h.evidence
    assert retained.pool is h.pool and retained.session_nonce == h.authority.session.nonce
    assert h.events.count("result") == h.events.count("grant") == 1
    assert h.events.count("child.close") <= 1
    assert retained.current.state.remote is None and "privatepassword" not in repr(retained)
    if fault == "evidence:result":
        assert h.events.count("evidence:result") == 1 and Kind.RESULT not in retained.receipts
        assert retained.evidence_poisoned


def test_result_observation_survives_operation_expiry_before_persistence():
    h = Harness()
    h.hook = lambda label: setattr(h, "now", 101.0) if label == "result" else None
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    retained = caught.value.retained
    assert retained.response.evidence == h.proof and retained.raw_result == h.received_result
    assert retained.containment_deadline == 106.0
    index = h.events.index("result")
    assert h.events[index + 1] == "terminate"
    assert all(deadline == 106.0 for _, deadline in h.deadlines if deadline > 100)


def test_nonzero_exit_retains_proof_without_result_success_ack():
    h = Harness()
    h.exit_code = 7
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    assert caught.value.retained.local_exit.exit_code == 7
    assert caught.value.retained.response.evidence == h.proof
    assert "terminate" not in h.events and "ack:CoordinatorResultReceived" not in h.events
    assert h.current.state.remote is None


def test_wrong_reaping_identity_requires_real_containment_attempt():
    h = Harness()
    h.child.wait = lambda **kw: TdsChildExit(replace(h.startup.process, pid=999), 0, True)
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    assert "terminate" in h.events
    assert caught.value.retained.local_exit.identity == h.startup.process


def test_capture_cleanup_deadline_once_and_never_refresh_on_repeat():
    h = Harness()
    h.fail_at = "result"
    original_terminate = h.child.terminate

    def cannot_contain(**kw):
        original_terminate(**kw)
        h.now = 16.0
        raise WindowOutcomeUnknown("containment unknown")

    h.child.terminate = cannot_contain
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    retained = caught.value.retained
    assert retained.containment_deadline == 15.0
    before = h.events.count("terminate")
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as again:
        fail_coordinator(retained, RuntimeError(), termination_timeout=50.0, clock=lambda: h.now)
    assert again.value.retained is retained and retained.containment_deadline == 15.0
    assert h.events.count("terminate") == before
    assert retained.response.evidence == h.proof


def test_unknown_launch_retained_without_numeric_pid_fallback():
    h = Harness()
    calls = []

    def contain(**kw):
        calls.append(("contain", kw["deadline"]))
        raise WindowOutcomeUnknown("no pidfd")

    launch = SimpleNamespace(contain=contain, close=lambda: calls.append("close"))

    def spawn(**kw):
        h.hit("spawn")
        raise TdsLaunchUnknown(launch)

    h.launcher.spawn = spawn
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    assert caught.value.retained.unresolved_launch is launch and caught.value.retained.child is None
    assert calls == [("contain", 15.0)] and "credentials" not in h.events


def test_missing_registration_ack_cannot_attach_incompatible_local_fact():
    h = Harness()
    h.fail_at = "ack:CoordinatorProcessRegistered"
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    retained = caught.value.retained
    assert retained.current.state.process is None and retained.local_exit.identity == h.startup.process
    assert "ack:CoordinatorLocalObserved" not in h.events
    assert retained.raw_result is None and retained.journal_poisoned
    # Ambiguous registration save may exist durably, but was not acknowledged.
    assert h.durable.state.process == h.startup.process


def test_takeover_after_last_pregrant_assertion_does_not_imply_sql_revocation():
    h = Harness()
    observed_grant = []

    def takeover(label):
        if label == "assert:grant_intent" and not h.lost:
            observed_grant.append(h.current.state.grant)
            newer = replace(h.owner, fence=h.owner.fence + 1, supervisor_id="00000000-0000-0000-0000-000000000099")
            h.durable = TdsCoordinatorSnapshot(
                take_over_coordinator_state(h.current.state, newer), h.current.revision + 1
            )
            h.lost = True

    h.hook = takeover
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    retained = caught.value.retained
    assert h.grant == observed_grant[0] and h.grant.ownership == h.owner
    assert "grant" in h.events and "ack:CoordinatorResultReceived" not in h.events
    assert retained.response.evidence == h.proof and Kind.RESULT in h.saved
    assert h.durable.state.recovering and h.durable.state.execution_owner == h.owner
    with pytest.raises(ValueError):
        advance_coordinator_state(
            h.durable.state, CoordinatorResultReceived(retained.response.result), expected_phase=h.durable.state.phase
        )


@pytest.mark.parametrize("binding", ["session", "owner", "object_nonce", "columns", "database", "authority"])
def test_wrong_but_well_hashed_nested_result_is_never_persisted(binding):
    from uuid import UUID

    h = Harness()

    def wrong(response):
        proof = response.evidence
        if binding == "session":
            proof = replace(proof, session=replace(proof.session, session_id=999))
        if binding == "owner":
            proof = replace(proof, owner_binding="f" * 64)
        if binding == "object_nonce":
            proof = replace(proof, object_nonce=UUID(int=999))
        if binding == "columns":
            proof = replace(proof, columns=(replace(proof.columns[0], nullable=False),))
        if binding == "database":
            proof = replace(proof, database=replace(proof.database, database_id=999))
        if binding == "authority":
            proof = replace(proof, authority_sha256="f" * 64)
        return successful_response(proof)

    h.mutate_response = wrong
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    assert caught.value.retained.raw_result is not None and caught.value.retained.response is None
    assert Kind.RESULT not in h.saved and "ack:CoordinatorResultReceived" not in h.events


def test_malformed_nested_authority_never_reaches_evidence_gateway():
    h = Harness()
    real_observe = h.child.observe_authority

    def malformed(**kw):
        body = strict_json_object(real_observe(**kw))
        body["session"]["unexpected"] = "sensitive"
        return canonical_json_bytes(body)

    h.child.observe_authority = malformed
    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        h.run()
    assert Kind.AUTHORITY not in h.saved and "grant" not in h.events


def test_poisoned_journal_and_evidence_cannot_delay_first_stop_or_receive_new_commands():
    h = Harness()
    h.fail_at = "evidence:result"
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    index = h.events.index("evidence:result")
    assert h.events[index + 1] == "terminate"
    assert caught.value.retained.evidence_poisoned
    assert not any(label.startswith("evidence:") for label in h.events[index + 1 :])


def test_foreign_receipt_rejected_and_not_recorded_as_ack():
    h = Harness()
    real_write = h.evidence.write

    def wrong(record, **kw):
        receipt = real_write(record, **kw)
        return replace(receipt, byte_count=1)

    h.evidence.write = wrong
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    assert not caught.value.retained.receipts and "spawn" not in h.events


def test_cancel_at_result_preserves_observation_and_attempts_containment():
    h = Harness()

    def cancel(label):
        if label == "result":
            raise KeyboardInterrupt

    h.hook = cancel
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    assert caught.value.retained.response.evidence == h.proof and "terminate" in h.events


def test_failed_writer_teardown_never_submits_another_journal_command():
    h = Harness()
    h.fail_at = "writer.close"
    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        h.run()
    after = h.events[h.events.index("writer.close") + 1 :]
    assert not any(label.startswith(("assert:", "ack:")) for label in after)


@pytest.mark.parametrize("after_decode", [14.0, 16.0])
def test_failure_budget_includes_result_decode_and_retains_expired_observation(monkeypatch, after_decode):
    from dpone.app.mssql_tds_coordinator_supervision import TdsCoordinatorRetention

    h = Harness()
    h.fail_at = "result"
    original_capture = TdsCoordinatorRetention.capture_result

    def delayed_capture(retained):
        original_capture(retained)
        h.now = after_decode

    monkeypatch.setattr(TdsCoordinatorRetention, "capture_result", delayed_capture)
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    retained = caught.value.retained
    assert retained.containment_deadline == 15.0
    assert retained.response.evidence == h.proof and retained.raw_result == h.received_result
    if after_decode < 15:
        assert ("terminate", 15.0) in h.deadlines
    else:
        assert h.events[-1] == "result"  # No fresh budget or I/O after decoding used it.


@pytest.mark.parametrize("invalid_clock", [float("nan"), None])
def test_invalid_initial_failure_clock_retains_result_without_later_budget_renewal(invalid_clock):
    h = Harness()
    h.hook = lambda label: setattr(h, "now", invalid_clock) if label == "result" else None
    h.fail_at = "result"
    with pytest.raises(TdsCoordinatorSupervisionUnknown) as caught:
        h.run()
    retained = caught.value.retained
    assert retained.response.evidence == h.proof and retained.raw_result == h.received_result
    assert "terminate" not in h.events
    h.now = 12.0
    with pytest.raises(TdsCoordinatorSupervisionUnknown):
        fail_coordinator(retained, RuntimeError(), termination_timeout=100.0, clock=lambda: h.now)
    assert "terminate" not in h.events
