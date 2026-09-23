"""Retained CREATE observations and containment-first failure handling.

All backend access is through deadline-bounded gateways. Local exit, durable
result, cleanup and SQL settlement are independent; no remote proof is produced.
Retention objects never contain credential material or credential envelope bytes.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from hashlib import sha256

from dpone.adapters.mssql_coordinator_worker_capabilities import TdsActorPool
from dpone.app.mssql_tds_coordinator_request import TdsCreateResponse, decode_create_response
from dpone.contracts.mssql_coordinator_worker_capabilities import (
    TdsAttemptError,
    TdsChildExit,
    TdsCoordinatorAuthority,
    TdsCoordinatorEvent,
    TdsCoordinatorEvidenceReceipt,
    TdsCoordinatorEvidenceRecord,
    TdsCoordinatorGrant,
    TdsCoordinatorLocalExit,
    TdsCoordinatorSnapshot,
    TdsCoordinatorStartup,
    TdsCreateRequest,
    TdsProcessIdentity,
    WindowOutcomeUnknown,
    advance_coordinator_state,
    coordinator_identity_digest,
)
from dpone.contracts.mssql_coordinator_worker_capabilities import (
    TdsCoordinatorEvidenceKind as Kind,
)
from dpone.ports.mssql_coordinator_worker_capabilities import (
    AdvanceCoordinator,
    AssertCoordinatorAuthority,
    TdsCoordinatorEvidenceGateway,
    TdsCoordinatorGateway,
    TdsManagedCoordinatorProcess,
    TdsUnresolvedLaunch,
)


def before(deadline: float, clock: Callable[[], float]) -> None:
    now = clock()
    if type(deadline) not in (int, float) or not math.isfinite(deadline) or not math.isfinite(now) or now >= deadline:
        raise TimeoutError("mssql_native.tds_coordinator_deadline")


def checked_exit(value: TdsChildExit, process: TdsProcessIdentity | None) -> TdsChildExit:
    if type(value) is not TdsChildExit or value.identity != process or value.reaped is not True:
        raise ValueError("mssql_native.tds_coordinator_exit_invalid")
    return value


@dataclass
class _CreateLocalCleanup:
    """Original CREATE capabilities and one-way local cleanup progress."""

    child: TdsManagedCoordinatorProcess | None
    unresolved: TdsUnresolvedLaunch | None
    process: TdsProcessIdentity | None
    exit: TdsChildExit | None
    close_attempted: bool
    closed: bool
    unresolved_attempted: bool
    unresolved_closed: bool
    budget_captured: bool
    deadline: float | None
    stop_unknown: bool = False


@dataclass
class TdsCoordinatorRetention:
    """Operation-owned capabilities and observations; preserve on every ambiguity."""

    writer: TdsCoordinatorGateway
    evidence: TdsCoordinatorEvidenceGateway
    pool: TdsActorPool
    current: TdsCoordinatorSnapshot
    request: TdsCreateRequest
    admission: bytes
    session_nonce: bytes
    child: TdsManagedCoordinatorProcess | None = None
    unresolved_launch: TdsUnresolvedLaunch | None = None
    process: TdsProcessIdentity | None = None
    startup: TdsCoordinatorStartup | None = None
    registration: bytes | None = None
    authority: TdsCoordinatorAuthority | None = None
    grant: TdsCoordinatorGrant | None = None
    raw_result: bytes | None = field(default=None, repr=False)
    response: TdsCreateResponse | None = None
    local_exit: TdsChildExit | None = None
    receipts: dict[Kind, TdsCoordinatorEvidenceReceipt] = field(default_factory=dict)
    code: TdsAttemptError = TdsAttemptError.FENCING
    containment_deadline: float | None = None
    containment_budget_captured: bool = False
    child_close_attempted: bool = False
    child_closed: bool = False
    evidence_poisoned: bool = False
    journal_poisoned: bool = False
    evidence_closed: bool = False
    writer_closed: bool = False
    parent_assertion: Callable[[float], None] | None = field(default=None, repr=False)
    parent_poisoned: bool = False
    parent_asserting: bool = False
    unresolved_close_attempted: bool = False
    unresolved_closed: bool = False

    _local_cleanup: _CreateLocalCleanup | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.child is not None or self.unresolved_launch is not None:
            self._bind_local_cleanup()

    def _bind_local_cleanup(self) -> _CreateLocalCleanup:
        if self._local_cleanup is None:
            if (
                self.child_closed
                and not self.child_close_attempted
                or self.unresolved_closed
                and not self.unresolved_close_attempted
                or self.child is not None
                and self.unresolved_launch is not None
            ):
                raise ValueError("mssql_native.tds_coordinator_cleanup_invalid")
            if self.local_exit is not None:
                checked_exit(self.local_exit, self.process)
            self._local_cleanup = _CreateLocalCleanup(
                self.child,
                self.unresolved_launch,
                self.process,
                self.local_exit,
                self.child_close_attempted,
                self.child_closed,
                self.unresolved_close_attempted,
                self.unresolved_closed,
                self.containment_budget_captured,
                self.containment_deadline,
            )
        return self._local_cleanup

    def capture_cleanup_budget(self, timeout: float, clock: Callable[[], float]) -> float:
        """CREATE and departure share this capture, including a failed clock call."""
        progress = self._bind_local_cleanup()
        if not progress.budget_captured:
            progress.budget_captured = self.containment_budget_captured = True
            if progress.deadline is None:
                value = clock() + timeout
                if math.isfinite(value) and math.isfinite(timeout) and timeout > 0:
                    progress.deadline = self.containment_deadline = value
        if progress.deadline is None:
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_cleanup_budget_unknown")
        return progress.deadline

    def cleanup_local(self, deadline: float, clock: Callable[[], float], *, close: bool = True) -> None:
        """Only CREATE decides how its child is stopped; no SQL or actor effects."""
        progress = self._bind_local_cleanup()
        if not progress.budget_captured:
            progress.budget_captured = self.containment_budget_captured = True
            progress.deadline = deadline
        if progress.deadline is None:
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_cleanup_budget_unknown")
        progress.deadline = self.containment_deadline = min(deadline, progress.deadline)
        before(progress.deadline, clock)
        if progress.stop_unknown:
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_containment_unknown")
        if progress.child is not None:
            if progress.exit is None:
                progress.stop_unknown = True
                progress.exit = checked_exit(progress.child.terminate(deadline=progress.deadline), progress.process)
                before(progress.deadline, clock)
                progress.stop_unknown = False
            self.local_exit = progress.exit
            if close:
                self.close_child()
        elif progress.unresolved is not None:
            if not progress.unresolved_closed:
                progress.stop_unknown = True
                progress.unresolved.contain(deadline=progress.deadline)
                before(progress.deadline, clock)
                progress.stop_unknown = False
                self.close_unresolved()
        else:
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_containment_unknown")

    def assert_parent(self, deadline: float) -> None:
        """Repeat the composition-owned attempt assertion; failure is permanent."""
        if self.parent_poisoned or self.parent_asserting:
            self.parent_poisoned = True
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_parent_unknown")
        if self.parent_assertion is None:
            return
        self.parent_asserting = True
        try:
            self.parent_assertion(deadline)
            if self.parent_poisoned:
                raise WindowOutcomeUnknown("mssql_native.tds_coordinator_parent_unknown")
        except BaseException:
            self.parent_poisoned = True
            raise
        finally:
            self.parent_asserting = False

    @property
    def operation_sha256(self) -> str:
        return coordinator_identity_digest(self.current.state.identity)

    def _execute(self, request, deadline: float) -> TdsCoordinatorSnapshot:
        self.assert_parent(deadline)
        if self.journal_poisoned:
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_journal_poisoned")
        try:
            return self.writer.execute(request, deadline=deadline)
        except BaseException:
            self.journal_poisoned = True
            raise

    def assert_authority(self, deadline: float) -> None:
        observed = self._execute(AssertCoordinatorAuthority(), deadline)
        if (
            type(observed) is not TdsCoordinatorSnapshot
            or observed != self.current
            or self.writer.observation.snapshot != observed
        ):
            self.journal_poisoned = True
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_ack_unknown")

    def advance(self, event: TdsCoordinatorEvent, deadline: float) -> None:
        previous = self.current
        expected = advance_coordinator_state(previous.state, event, expected_phase=previous.state.phase)
        observed = self._execute(AdvanceCoordinator(event, previous.state.phase), deadline)
        if (
            type(observed) is not TdsCoordinatorSnapshot
            or observed.state != expected
            or (expected != previous.state and observed.revision <= previous.revision)
            or (expected == previous.state and observed != previous)
            or self.writer.observation.snapshot != observed
        ):
            self.journal_poisoned = True
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_ack_unknown")
        self.current = observed

    def persist(self, kind: Kind, payload: bytes, deadline: float) -> TdsCoordinatorEvidenceReceipt:
        self.assert_parent(deadline)
        record = TdsCoordinatorEvidenceRecord(self.operation_sha256, kind, payload)
        expected = record.receipt
        if kind in self.receipts:
            if self.receipts[kind] != expected:
                raise WindowOutcomeUnknown("mssql_native.tds_coordinator_evidence_changed")
            return expected
        if self.evidence_poisoned:
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_evidence_poisoned")
        try:
            received = self.evidence.write(record, deadline=deadline)
            if type(received) is not TdsCoordinatorEvidenceReceipt or received != expected:
                raise WindowOutcomeUnknown("mssql_native.tds_coordinator_evidence_ack_unknown")
        except BaseException:
            self.evidence_poisoned = True
            raise
        self.receipts[kind] = received
        return received

    def capture_result(self) -> None:
        """Pure bounded decode; observed bytes survive close/decode failures."""
        if self.raw_result is None and self.child is not None:
            self.raw_result = self.child.received_result
        if (
            self.response is None
            and self.raw_result is not None
            and self.grant is not None
            and self.authority is not None
        ):
            self.response = decode_create_response(
                self.raw_result,
                request=self.request,
                identity=self.current.state.identity,
                grant=self.grant,
                authority=self.authority,
            )

    def close_child(self) -> None:
        progress = self._bind_local_cleanup()
        if progress.child is None or progress.closed:
            return
        if progress.close_attempted:
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_close_unknown")
        progress.close_attempted = self.child_close_attempted = True
        progress.child.close()
        progress.closed = self.child_closed = True

    def close_unresolved(self) -> None:
        """Compatibility entrypoint delegates to the original CREATE progress."""
        progress = self._bind_local_cleanup()
        if progress.unresolved is None or progress.unresolved_closed:
            return
        if progress.unresolved_attempted:
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_close_unknown")
        progress.unresolved_attempted = self.unresolved_close_attempted = True
        progress.unresolved.close()
        progress.unresolved_closed = self.unresolved_closed = True

    def close_evidence(self, deadline: float) -> None:
        if self.evidence_closed:
            return
        self.evidence_poisoned = True
        self.evidence.close(deadline=deadline)
        self.evidence_closed = True

    def close_writer(self, deadline: float) -> None:
        if self.writer_closed:
            return
        self.journal_poisoned = True
        self.writer.close(deadline=deadline)
        self.writer_closed = True

    def local_proof(self) -> TdsCoordinatorLocalExit:
        if self.registration is None or self.process is None or self.local_exit is None:
            raise WindowOutcomeUnknown("mssql_native.tds_coordinator_local_binding_unknown")
        return TdsCoordinatorLocalExit(
            self.operation_sha256, self.process, self.local_exit, sha256(self.registration).hexdigest()
        )


from dpone.app.mssql_tds_coordinator_lifecycle import (  # noqa: E402
    TdsCoordinatorFailure as TdsCoordinatorFailure,
)
from dpone.app.mssql_tds_coordinator_lifecycle import (  # noqa: E402
    TdsCoordinatorSupervisionUnknown as TdsCoordinatorSupervisionUnknown,
)
from dpone.app.mssql_tds_coordinator_lifecycle import (  # noqa: E402
    fail_coordinator as fail_coordinator,
)

TdsCoordinatorFailure.__module__ = __name__
TdsCoordinatorSupervisionUnknown.__module__ = __name__
fail_coordinator.__module__ = __name__
