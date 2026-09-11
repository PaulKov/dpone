"""One admitted parent executor, followed by protected terminal observations.

The composition root supplies durable SQL adapters and an independent business
outcome observer. A process return value is never an outcome proof. Any missing
proof leaves durable RUNNING ownership blocking; COMMIT_UNKNOWN also blocks.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    CompositionAttemptReceipt,
)

Credentials = TypeVar("Credentials")
Issued = TypeVar("Issued", covariant=True)
Result = TypeVar("Result")


class CompositionWorkerAttempts(Protocol):
    """Protected atomic admission; existing RUNNING is a rejection, not a permit."""

    def admit_once(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptReceipt: ...

    def finalize(
        self, attempt: CompositionAttemptIdentity, *, state: str, outcome_evidence_sha256: str
    ) -> CompositionAttemptReceipt: ...


class CompositionWorkerGate(Protocol[Issued]):
    """Monotonic one-time issuance with independent server observations."""

    def issue_once(self, attempt: CompositionAttemptIdentity) -> Issued: ...

    def close(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof: ...

    def prove_quiescence(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof: ...


class CompositionWorkerOutcome(Protocol):
    """Reopen actual business evidence and persist a protected OUTCOME original.

    No executor result, exit status, caller-selected state or unbacked digest is
    supplied here. The attempt store independently reopens the produced proof.
    """

    def observe(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof: ...


@dataclass(frozen=True, slots=True)
class CompositionWorkerResult(Generic[Result]):
    """Executor return value paired with the exact protected terminal receipt."""

    value: Result
    receipt: CompositionAttemptReceipt


class CompositionWorker(Generic[Credentials]):
    """Coordinate one executor without changing native-v2 lifecycle semantics."""

    def __init__(
        self,
        *,
        attempts: CompositionWorkerAttempts,
        gate: CompositionWorkerGate[Credentials],
        outcome_observer: CompositionWorkerOutcome,
    ) -> None:
        self._attempts = attempts
        self._gate = gate
        self._outcomes = outcome_observer

    def run(
        self, attempt: CompositionAttemptIdentity, *, execute: Callable[[Credentials], Result]
    ) -> CompositionWorkerResult[Result]:
        """Reserve once, issue once, execute, close, observe, then seal.

        Never close on admission/issuance rejection: a duplicate invocation must
        not interfere with the original executor. The issuer owns ambiguous
        provisioning cleanup. Once credentials were delivered, even interruption
        attempts closure. Failed closure/observation never releases ownership.
        """
        attempt.__post_init__()
        admitted = self._attempts.admit_once(attempt)
        admitted.__post_init__()
        if admitted.attempt != attempt or admitted.state != "RUNNING":
            raise CompositionAdmissionError("worker_admission_readback")
        credentials = self._gate.issue_once(attempt)
        try:
            value = execute(credentials)
        except BaseException as error:
            self._finish(attempt)
            if isinstance(error, Exception):
                raise CompositionAdmissionError("worker_executor_failed") from None
            raise
        receipt = self._finish(attempt)
        if receipt.state == "COMMIT_UNKNOWN":
            raise CompositionAdmissionError("worker_commit_unknown")
        if receipt.state == "FAILED":
            raise CompositionAdmissionError("worker_failed")
        return CompositionWorkerResult(value, receipt)

    def _finish(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptReceipt:
        closed = self._gate.close(attempt).require_attempt(attempt)
        if closed.kind != "CLOSED_GATES":
            raise CompositionAdmissionError("worker_closed_gate_proof")
        quiescence = self._gate.prove_quiescence(attempt).require_attempt(attempt)
        if quiescence.kind != "QUIESCENCE" or quiescence.authorities != closed.authorities:
            raise CompositionAdmissionError("worker_quiescence_proof")
        outcome = self._outcomes.observe(attempt).require_attempt(attempt)
        if outcome.kind != "OUTCOME" or outcome.authorities != closed.authorities or outcome.outcome_state is None:
            raise CompositionAdmissionError("worker_outcome_proof")
        receipt = self._attempts.finalize(
            attempt, state=outcome.outcome_state, outcome_evidence_sha256=outcome.proof_sha256
        )
        receipt.__post_init__()
        expected = CompositionAttemptReceipt(
            attempt, outcome.outcome_state, closed.proof_sha256, quiescence.proof_sha256, outcome.proof_sha256
        )
        if receipt != expected:
            raise CompositionAdmissionError("worker_terminal_readback")
        return receipt
