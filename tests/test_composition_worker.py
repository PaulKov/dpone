"""Offline lifecycle contracts; these fakes do not certify SQL execution."""

from dataclasses import replace

import pytest

from dpone.contracts.composition_control import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptReceipt
from dpone.services.composition_worker import CompositionWorker
from tests.composition_mssql_attempt_fixtures import terminal_proofs
from tests.composition_mssql_gate_helpers import attempt


class Authority:
    def __init__(self, *, failure=None, state="SUCCEEDED"):
        self.events = []
        self.failure = failure
        self.proofs = dict(zip(("close", "quiescence", "outcome"), terminal_proofs(state), strict=True))
        self.credentials = object()
        self.running = False

    def event(self, name):
        self.events.append(name)
        if self.failure == name:
            raise RuntimeError("sensitive driver detail")

    def admit_once(self, identity):
        self.event("admit")
        if self.running:
            raise CompositionAdmissionError("attempt_replay")
        self.running = True
        return CompositionAttemptReceipt(identity, "RUNNING")

    def issue_once(self, identity):
        self.event("issue")
        return self.credentials

    def close(self, identity):
        self.event("close")
        return self.proofs["close"]

    def prove_quiescence(self, identity):
        self.event("quiescence")
        return self.proofs["quiescence"]

    def observe(self, identity):
        self.event("outcome")
        return self.proofs["outcome"]

    def finalize(self, identity, *, state, outcome_evidence_sha256):
        self.event("finalize")
        assert outcome_evidence_sha256 == self.proofs["outcome"].proof_sha256
        return CompositionAttemptReceipt(identity, state, *(p.proof_sha256 for p in self.proofs.values()))

    def execute(self, credentials):
        assert credentials is self.credentials
        self.event("execute")
        return "real executor result"


def worker(authority):
    return CompositionWorker(attempts=authority, gate=authority, outcome_observer=authority)


def test_closed_gate_and_independent_quiescence_precede_outcome_and_finalize():
    authority = Authority()
    result = worker(authority).run(attempt(), execute=authority.execute)
    assert result.value == "real executor result"
    assert result.receipt.state == "SUCCEEDED"
    assert authority.events == ["admit", "issue", "execute", "close", "quiescence", "outcome", "finalize"]


def test_running_replay_cannot_issue_execute_or_close_original_gate():
    authority = Authority()
    authority.running = True
    with pytest.raises(CompositionAdmissionError, match="attempt_replay"):
        worker(authority).run(attempt(), execute=authority.execute)
    assert authority.events == ["admit"]


@pytest.mark.parametrize("boundary", ["issue", "close", "quiescence", "outcome", "finalize"])
def test_uncertain_boundary_never_reports_success(boundary):
    authority = Authority(failure=boundary)
    with pytest.raises((RuntimeError, CompositionAdmissionError)):
        worker(authority).run(attempt(), execute=authority.execute)
    assert authority.events[-1] == boundary
    assert authority.running


def test_executor_exception_still_closes_and_proves_before_terminalization():
    authority = Authority(failure="execute", state="COMMIT_UNKNOWN")
    with pytest.raises(CompositionAdmissionError):
        worker(authority).run(attempt(), execute=authority.execute)
    assert authority.events[-4:] == ["close", "quiescence", "outcome", "finalize"]


def test_unknown_outcome_remains_blocking_even_after_normal_process_return():
    authority = Authority(state="COMMIT_UNKNOWN")
    with pytest.raises(CompositionAdmissionError, match="commit_unknown"):
        worker(authority).run(attempt(), execute=authority.execute)
    assert authority.events[-1] == "finalize"


def test_wrong_attempt_proof_cannot_finalize():
    authority = Authority()
    authority.proofs["outcome"] = replace(authority.proofs["outcome"], attempt_sha256=attempt(2).attempt_sha256)
    with pytest.raises(CompositionAdmissionError):
        worker(authority).run(attempt(), execute=authority.execute)
    assert "finalize" not in authority.events


def test_interruption_closes_gate_and_preserves_interruption():
    authority = Authority(state="COMMIT_UNKNOWN")

    def interrupted(credentials):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        worker(authority).run(attempt(), execute=interrupted)
    assert authority.events[-4:] == ["close", "quiescence", "outcome", "finalize"]


def test_protected_failed_outcome_cannot_return_executor_success():
    authority = Authority(state="FAILED")
    with pytest.raises(CompositionAdmissionError, match="worker_failed"):
        worker(authority).run(attempt(), execute=authority.execute)
    assert authority.events[-1] == "finalize"


@pytest.mark.parametrize("phase", ["close", "quiescence", "outcome"])
def test_wrong_proof_kind_cannot_finalize(phase):
    authority = Authority()
    authority.proofs[phase] = authority.proofs["quiescence" if phase == "close" else "close"]
    with pytest.raises(CompositionAdmissionError):
        worker(authority).run(attempt(), execute=authority.execute)
    assert "finalize" not in authority.events


def test_wrong_terminal_receipt_cannot_return_success():
    authority = Authority()
    authority.finalize = lambda identity, **kwargs: CompositionAttemptReceipt(identity, "RUNNING")
    with pytest.raises(CompositionAdmissionError, match="terminal_readback"):
        worker(authority).run(attempt(), execute=authority.execute)


def test_wrong_admission_receipt_cannot_issue():
    authority = Authority()
    authority.admit_once = lambda identity: CompositionAttemptReceipt(attempt(2), "RUNNING")
    with pytest.raises(CompositionAdmissionError, match="admission_readback"):
        worker(authority).run(attempt(), execute=authority.execute)
    assert authority.events == []
