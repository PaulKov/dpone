"""Per-invocation collaborators of one supervised native dbt attempt.

The parent worker owns the protected admission, issuance, closure and terminal
sequence, but a native dbt attempt has to reserve its dedicated child identity
inside that sequence and reopen the real business outcome at the end. Those two
seams are the only mutable state of one invocation, so they live here instead of
inside the composition root: the root stays a wiring script, and each collaborator
can be read, reviewed and tested as a single authority boundary.

Nothing in this module accepts a caller-supplied intent, expectation, digest or
identity. Every value is either read back from the protected store or derived by
the capture authority from the verified sources of exactly one attempt.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from dpone.adapters.composition_mssql_dbt_outcome import CompositionDbtOutcomeProofProducer
from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionAdmissionError,
)
from dpone.contracts.composition_attempt import CompositionAttemptIdentity, CompositionAttemptReceipt
from dpone.contracts.dbt_runtime import dbt_attempt_id
from dpone.runtime.dbt_run_results import parse_dbt_run_results
from dpone.services.composition_dbt_capture_authority import (
    CompositionDbtCaptureAuthority,
    CompositionDbtChildAllocation,
)
from dpone.services.composition_dbt_outcome import CompositionDbtOutcomeObserver

if TYPE_CHECKING:
    from dpone.adapters.composition_dbt_process_boundary import DbtProcessAllocation
    from dpone.app.composition_dbt_execution import (
        CompositionDbtExecutionDependencies,
        CompositionDbtExecutionRequest,
    )
    from dpone.ports.dbt_publishing import DbtExecutionOutcome


class NativeDbtAttemptStore(Protocol):
    """Protected parent attempt reservation, audit readback and terminal seal."""

    def admit_once(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptReceipt: ...

    def read_exact(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptReceipt: ...

    def finalize(
        self, attempt: CompositionAttemptIdentity, *, state: str, outcome_evidence_sha256: str
    ) -> CompositionAttemptReceipt: ...


class DbtChildProcessBoundary(Protocol):
    """Reserve one dedicated child identity and its exclusive attempt layout."""

    def allocate(
        self, attempt: CompositionAttemptIdentity, *, runtime_attempt_id: str, target_path: str = "target"
    ) -> DbtProcessAllocation: ...


@dataclass
class SupervisedDbtSession:
    """Mutable per-invocation state shared by the root's worker collaborators."""

    request: CompositionDbtExecutionRequest
    occurrence: CompositionActivationOccurrence
    attempt: CompositionAttemptIdentity
    allocation: DbtProcessAllocation | None = None
    authority: CompositionDbtCaptureAuthority | None = None
    store: Any | None = None
    outcome: DbtExecutionOutcome | None = None
    undispatched: bool = False
    unsealed: bool = False
    manifest_digests: list[str] = field(default_factory=list)


class AdmittedExecutionSlot:
    """Reserve the dedicated child identity only after protected admission."""

    def __init__(self, dependencies: CompositionDbtExecutionDependencies, session: SupervisedDbtSession) -> None:
        self._deps = dependencies
        self._session = session

    def admit_once(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptReceipt:
        session = self._session
        receipt = self._deps.attempts.admit_once(attempt)
        session.allocation = self._deps.boundary.allocate(
            attempt,
            runtime_attempt_id=dbt_attempt_id(session.request.run_identity, session.request.airflow_attempt),
            target_path=session.request.pack.target_path,
        )
        return receipt

    def finalize(
        self, attempt: CompositionAttemptIdentity, *, state: str, outcome_evidence_sha256: str
    ) -> CompositionAttemptReceipt:
        return self._deps.attempts.finalize(attempt, state=state, outcome_evidence_sha256=outcome_evidence_sha256)


class TerminalDbtOutcome:
    """Reopen the actual business outcome after the gate is closed and quiet.

    A pre-dispatch failure that still has a registered intent is recorded as a
    protected UNDISPATCHED closure, which is the only source of FAILED. A
    preflight failure so early that no intent could ever be derived is never
    sealed at all: durable RUNNING ownership blocks until reconciliation instead
    of pretending that dispatch or capture happened.
    """

    def __init__(self, dependencies: CompositionDbtExecutionDependencies, session: SupervisedDbtSession) -> None:
        self._deps = dependencies
        self._session = session

    def observe(self, attempt: CompositionAttemptIdentity) -> Any:
        session = self._session
        store, authority = session.store, session.authority
        if store is None or authority is None or session.unsealed:
            raise CompositionAdmissionError("worker_predispatch_unsealed")
        if session.undispatched:
            store.record_undispatched(attempt)
        observer = CompositionDbtOutcomeObserver(
            store,
            result_parser=parse_dbt_run_results,
            materialization_observer=self._deps.materializations(authority.materialization_contracts),
        )
        return CompositionDbtOutcomeProofProducer(
            transaction=self._deps.outcome_transaction,
            expected_service_id=self._deps.expected_service_id,
            intent_reader=store.load_intent,
            outcome_observer=observer,
            persist=self._deps.outcome_proof_writer,
        ).observe(attempt)


def observed_child_allocation(allocation: DbtProcessAllocation, profile_path: Path) -> CompositionDbtChildAllocation:
    """Observe the identity actually applied to the child-writable directories.

    The reserved pair is read back from the supervisor mount instead of trusting
    a returned value, so a mis-provisioned allocation cannot widen the child.
    """
    target = os.stat(allocation.target, follow_symlinks=False)
    logs = os.stat(allocation.logs, follow_symlinks=False)
    if (target.st_uid, target.st_gid) != (logs.st_uid, logs.st_gid):
        raise CompositionAdmissionError("worker_child_identity")
    return CompositionDbtChildAllocation(
        allocation.output_directory,
        allocation.preflight_target,
        allocation.preflight_logs,
        allocation.target,
        allocation.logs,
        profile_path,
        target.st_uid,
        target.st_gid,
    )


__all__ = [
    "AdmittedExecutionSlot",
    "DbtChildProcessBoundary",
    "NativeDbtAttemptStore",
    "SupervisedDbtSession",
    "TerminalDbtOutcome",
    "observed_child_allocation",
]
