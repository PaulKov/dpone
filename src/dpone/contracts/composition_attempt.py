"""Pure parent attempt fencing rules, enforced by protected admission adapters.

These records contain no credentials and grant no execution authority by their
shape alone. Admission must evaluate the complete protected ledger in the same
transaction as reservation; terminal evidence must come from trusted producers.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionAdmissionError,
    require_digest,
    require_ordered_unique,
    require_text,
)


@dataclass(frozen=True, slots=True)
class CompositionAttemptIdentity:
    """One real scheduler attempt, pinned to the parent and selected workload.

    The parent request binds activation UUID, release, deployment and runtime
    context. Plan identity is separately supplied by the verified worker plan.
    ``try_number`` and ``map_index`` come from the actual Airflow task attempt.
    """

    activation_request_sha256: str
    workload_id: str
    constituent_id: str
    pack_sha256: str
    plan_sha256: str
    dag_run_id: str
    task_id: str
    try_number: int
    map_index: int
    guard_epochs: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        for value in (self.activation_request_sha256, self.pack_sha256, self.plan_sha256):
            require_digest(value)
        for value in (self.workload_id, self.dag_run_id, self.task_id):
            require_text(value)
        if self.constituent_id not in {"native", "standalone"}:
            raise CompositionAdmissionError("attempt_constituent")
        if (
            type(self.try_number) is not int
            or self.try_number < 1
            or type(self.map_index) is not int
            or self.map_index < -1
        ):
            raise CompositionAdmissionError("scheduler_attempt")
        if not isinstance(self.guard_epochs, tuple) or any(
            type(pair) is not tuple or len(pair) != 2 for pair in self.guard_epochs
        ):
            raise CompositionAdmissionError("attempt_guard_epochs")
        require_ordered_unique(tuple(guard for guard, _ in self.guard_epochs))
        for guard, epoch in self.guard_epochs:
            require_digest(guard)
            if type(epoch) is not int or epoch <= 0:
                raise CompositionAdmissionError("attempt_guard_epochs")

    @property
    def attempt_sha256(self) -> str:
        """Canonical identity; changing a try creates a new fenced attempt."""
        return canonical_fingerprint({"schema": "dpone.composition-attempt.v1", **asdict(self)})


@dataclass(frozen=True, slots=True)
class CompositionAttemptReceipt:
    """Protected outcome with independent closed-gate and quiescence evidence.

    A quiescent COMMIT_UNKNOWN remains blocking: stopped sessions do not prove
    whether SQL effects committed. Only explicit reconciliation may resolve it.
    """

    attempt: CompositionAttemptIdentity
    state: str
    closed_gates_sha256: str | None = None
    quiescence_sha256: str | None = None
    outcome_evidence_sha256: str | None = None

    def __post_init__(self) -> None:
        self.attempt.__post_init__()
        if self.state not in {"RUNNING", "SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"}:
            raise CompositionAdmissionError("attempt_state")
        evidence = (self.closed_gates_sha256, self.quiescence_sha256, self.outcome_evidence_sha256)
        if self.state in {"SUCCEEDED", "FAILED"} and any(value is None for value in evidence):
            raise CompositionAdmissionError("terminal_evidence")
        for value in evidence:
            if value is not None:
                require_digest(value)


def require_composition_attempt_admission(
    occurrence: CompositionActivationOccurrence,
    attempt: CompositionAttemptIdentity,
    existing: tuple[CompositionAttemptReceipt, ...],
) -> None:
    """Reject stale parents, missing scopes, replays and overlapping unknown work.

    The caller must also bind ``plan_sha256`` to the actual verified runtime
    plan and supply the complete durable attempt ledger, including other
    occurrences holding any requested guard. No caller-provided subset is proof.
    """
    occurrence.require_state("ACTIVE")
    attempt.__post_init__()
    if attempt.activation_request_sha256 != occurrence.request.request_sha256:
        raise CompositionAdmissionError("attempt_parent")
    workloads = {row.workload_id: row for row in occurrence.request.workloads}
    workload = workloads.get(attempt.workload_id)
    if workload is None or (workload.constituent_id, workload.pack_sha256) != (
        attempt.constituent_id,
        attempt.pack_sha256,
    ):
        raise CompositionAdmissionError("attempt_workload")
    subjects = set(workload.write_subjects)
    guards = {row.guard_id for row in occurrence.request.resources if subjects.intersection(row.write_subjects)}
    expected = tuple(pair for pair in occurrence.receipt.guard_epochs if pair[0] in guards)
    if attempt.guard_epochs != expected:
        raise CompositionAdmissionError("attempt_guard_epochs")
    for receipt in existing:
        receipt.__post_init__()
        if receipt.attempt.attempt_sha256 == attempt.attempt_sha256:
            raise CompositionAdmissionError("attempt_replay")
        if receipt.state in {"RUNNING", "COMMIT_UNKNOWN"} and guards.intersection(
            guard for guard, _ in receipt.attempt.guard_epochs
        ):
            raise CompositionAdmissionError("attempt_conflict")
