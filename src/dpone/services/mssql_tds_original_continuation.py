"""Two finite original journal continuations; the attempt retains outer authority.

CREATE captures pending provenance before its legacy context exits and activates
that same association afterwards. Preparation retains its own phase history and
terminal receipt. Neither policy owns resource teardown or creates a new journal.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from time import monotonic
from typing import Any

from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.ports.mssql_tds_directory import (
    AssertDirectoryAuthority,
    RecordDirectoryContainment,
    RecordDirectorySettlement,
)
from dpone.services.mssql_tds_original_continuation_host import AttemptHost as _AttemptHost
from dpone.services.mssql_tds_preparation_origin import PreparationOrigin, PreparationWriterInputs
from dpone.services.mssql_tds_writer_contracts import (
    PreparationObservation,
    PreparationReceipt,
    Prepared,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    TdsDirectorySnapshot,
    WindowOutcomeUnknown,
    advance_state,
    record_local_containment,
    record_remote_settlement,
    validate_original_record,
)

CREATE_ERROR = "mssql_native.sqlclient_create_settlement_unknown"
PREPARATION_ERROR = "mssql_native.sqlclient_preparation_transition_unknown"
_original = validate_original_record


class CreateSettlement:
    """Registered only by successful original CREATE composition, never a setter."""

    def __init__(
        self,
        attempt: _AttemptHost,
        outcome: object,
        retained: Any,
        factory: object,
        parent: TdsAttemptSnapshot,
        directory: TdsDirectorySnapshot,
        deadline: float,
    ):
        """Capture pending originals inside the legacy context; install nothing."""
        baseline = attempt._departure_baseline
        if baseline is None or parent is not baseline[0] or directory is not baseline[1] or deadline != baseline[3]:
            raise WindowOutcomeUnknown(CREATE_ERROR)
        for original in (parent, directory, outcome, baseline):
            validate_original_record(original)
        self.attempt, self.outcome, self.retained, self.factory = attempt, outcome, retained, factory
        self.pool, self.deadline = retained.pool, deadline
        self.parent, self.directory = deepcopy(parent), deepcopy(directory)
        self.original_outcome = deepcopy(outcome)
        self._context = baseline
        self._captured = (attempt, outcome, retained, factory, baseline)
        self._activation_attempted = False
        self.used = self.complete = self.failed = False
        self.local_ack: TdsDirectorySnapshot | None = None
        self.remote_ack: TdsDirectorySnapshot | None = None

    @property
    def registered_outcome(self) -> object:
        return self._captured[1]

    def _same_origin(self) -> bool:
        return (
            self.attempt is self._captured[0]
            and self.outcome is self._captured[1]
            and self.retained is self._captured[2]
            and self.factory is self._captured[3]
            and self._context is self._captured[4]
            and type(self.deadline) is float
            and self.deadline == self._context[3]
            and self.pool is self.retained.pool
        )

    def activate_after_context(self) -> object:
        """Install this same pending object after the unchanged trusted with-exit."""
        self.attempt._owned()
        self.attempt._require_unpoisoned()
        if self._activation_attempted and self.attempt._create_settlement is self:
            raise WindowOutcomeUnknown(CREATE_ERROR)
        try:
            if (
                self._activation_attempted
                or self.used
                or self.failed
                or not self._same_origin()
                or self.attempt._departure_baseline is not None
                or self.attempt._create_settlement is not None
            ):
                raise WindowOutcomeUnknown(CREATE_ERROR)
            self._activation_attempted = True
            for original in (self.outcome, self.original_outcome, self._context):
                validate_original_record(original)
            if self.outcome != self.original_outcome:
                raise WindowOutcomeUnknown(CREATE_ERROR)
            self.attempt._create_settlement = self
            return self.outcome
        except BaseException:
            self.failed = self.attempt._poisoned = True
            raise

    def _return_registered(self) -> object:
        return self.activate_after_context()

    def assert_current(self, deadline: float) -> None:
        attempt = self.attempt
        attempt._require_unpoisoned()
        if self.failed or attempt._create_settlement is not self or not self._same_origin():
            raise WindowOutcomeUnknown(CREATE_ERROR)
        validate_original_record(self.outcome)
        if self.outcome != self.original_outcome:
            raise WindowOutcomeUnknown(CREATE_ERROR)
        attempt._lifecycle.assert_authority(deadline=deadline)
        validate_original_record(attempt._lifecycle.snapshot)
        if attempt._lifecycle.snapshot != self.parent:
            raise WindowOutcomeUnknown(CREATE_ERROR)
        observed = attempt._directory.execute(AssertDirectoryAuthority(), deadline=deadline)
        validate_original_record(observed)
        if observed != self.directory or attempt._directory.observation.snapshot != self.directory:
            raise WindowOutcomeUnknown(CREATE_ERROR)
        self.pool.assert_deadline(deadline=deadline)
        attempt._require_unpoisoned()

    def execute(self, local: Any, remote: Any, *, deadline: float) -> TdsDirectorySnapshot:
        attempt = self.attempt
        if self.used:
            attempt._poisoned = True
            raise WindowOutcomeUnknown(CREATE_ERROR)
        self.used = True
        end = min(self.deadline, deadline)
        try:
            with attempt._sequence(end):
                self.assert_current(end)
                index = self.retained.create_identity.slot_index
                for request, reducer, proof, name in (
                    (RecordDirectoryContainment(index, local), record_local_containment, local, "local_ack"),
                    (RecordDirectorySettlement(index, remote), record_remote_settlement, remote, "remote_ack"),
                ):
                    predicted = reducer(self.directory.state, index, proof)
                    previous = self.directory
                    observed = attempt._directory.execute(request, deadline=end)
                    if type(observed) is not TdsDirectorySnapshot:
                        raise WindowOutcomeUnknown(CREATE_ERROR)
                    validate_original_record(observed)
                    if (
                        observed.state != predicted
                        or observed.ownership != previous.ownership
                        or observed.revision <= previous.revision
                        or attempt._directory.observation.snapshot != observed
                    ):
                        raise WindowOutcomeUnknown(CREATE_ERROR)
                    self.directory = deepcopy(observed)
                    setattr(self, name, observed)
                    self.assert_current(end)
                self.complete = True
            return replace(self.directory)
        except BaseException:
            self.failed = attempt._poisoned = True
            raise WindowOutcomeUnknown(CREATE_ERROR) from None


class PreparationTransition:
    """Private composition mechanism; inputs never themselves confer admission."""

    def __init__(self, attempt: _AttemptHost, handle: Any) -> None:
        self.attempt, self.handle = attempt, handle
        self.deadline = handle.operation_deadline
        self.factory, self.pool, self.helper_id = handle.factory, handle.pool, handle.helper_id
        self.identity, self.startup, self.authority = handle.identity, handle.startup_receipt, handle.authority
        self.coordinator = handle.writer.observation.snapshot
        self.coordinator_snapshot = deepcopy(self.coordinator)
        self.registration = handle.evidence.observation if handle.evidence is not None else None
        self.registration_snapshot = deepcopy(self.registration)
        self.management_admission = handle.request.management_admission
        self.management_incarnation: Any = None
        self.opening: Any = None
        self.closing: Any = None
        self.parent = deepcopy(attempt._lifecycle.snapshot)
        self.directory = deepcopy(attempt._directory.observation.snapshot)
        self.expected: TdsAttemptSnapshot | None = None
        self.attempted = self.consumed = self.failed = self.cleaned = False
        self.policy: Any = None
        self.build: Any = None
        self.input: Any = None
        self.evidence: Any = None
        self.evidence_receipt: PreparationReceipt | None = None
        self._evidence_close_attempted = False
        self._evidence_closed = False
        self._handle_close_attempted = self._handle_closed = False
        self._origin = PreparationOrigin(self, attempt_identity_digest(self.parent.state.identity))
        self._captured_origin = self._origin

    @property
    def original_evidence(self) -> tuple:
        return self._origin.evidence

    @original_evidence.setter
    def original_evidence(self, value: tuple) -> None:
        self._origin.evidence = value

    @property
    def preparation_payload(self) -> bytes | None:
        return self._origin.payload

    @preparation_payload.setter
    def preparation_payload(self, value: bytes | None) -> None:
        self._origin.payload = value

    @property
    def _preparation_capture(self) -> tuple | None:
        return self._origin.capture

    @property
    def contained_exit(self) -> Any:
        return self._origin.exit

    def capture_management(self) -> None:
        self._origin.capture_management()

    def capture_writer_inputs(
        self,
        policy: object,
        input_descriptor: object,
        installation: object,
        *,
        policy_snapshot: bytes,
        input_snapshot: bytes,
        build_sha256: str,
        operation_deadline_ns: int,
        installation_type: type,
        content_expectation: object,
    ) -> None:
        self._origin.capture_writer_inputs(
            policy,
            input_descriptor,
            installation,
            policy_snapshot=policy_snapshot,
            input_snapshot=input_snapshot,
            build_sha256=build_sha256,
            operation_deadline_ns=operation_deadline_ns,
            installation_type=installation_type,
            content_expectation=content_expectation,
        )

    def writer_inputs(self) -> PreparationWriterInputs:
        return self._origin.writer_inputs()

    def begin_preparation(self, payload: bytes) -> None:
        self._origin.begin(payload)

    def capture_preparation(self, payload: bytes, receipt: PreparationReceipt) -> None:
        self._origin.acknowledge(payload, receipt)

    def validate_origin(self) -> None:
        if self.attempt._preparation is not self and self.attempt._prepared_origin is not self:
            raise WindowOutcomeUnknown(PREPARATION_ERROR)
        self._origin.validate()

    def _check(self) -> None:
        self._origin.assert_references()
        self.attempt._require_unpoisoned()
        if (
            self.failed
            or self.attempt._preparation is not self
            or self.handle.attempt is not self.attempt
            or monotonic() >= self.deadline
        ):
            raise WindowOutcomeUnknown(PREPARATION_ERROR)

    def assert_current(self, *, include_sql: bool = True) -> None:
        self._check()
        if not self.attempted:
            self.attempt._assert_observe(self.handle.helper_id, deadline=self.deadline)
            self.handle.continuation.assert_current()
            return
        if self.expected is None:
            raise WindowOutcomeUnknown(PREPARATION_ERROR)
        gateway = self.attempt._lifecycle
        gateway.assert_authority(deadline=self.deadline)
        _original(gateway.snapshot)
        if gateway.snapshot != self.expected:
            raise WindowOutcomeUnknown(PREPARATION_ERROR)
        observed = self.attempt._directory.execute(AssertDirectoryAuthority(), deadline=self.deadline)
        _original(observed)
        if observed != self.directory or self.attempt._directory.observation.snapshot != self.directory:
            raise WindowOutcomeUnknown(PREPARATION_ERROR)
        self.handle.continuation._assert_preparation_terminal(self)
        if self.handle.child is not None:
            self.handle.child.assert_current()
        if include_sql and self.handle.catalog is not None:
            self.handle.catalog.guard()
        self._check()

    @contextmanager
    def sequence(self) -> Iterator[None]:
        self.attempt._owned()
        self._check()
        if self.consumed:
            raise WindowOutcomeUnknown(PREPARATION_ERROR)
        self.consumed = True
        self.attempt._busy = True
        try:
            self.assert_current()
            yield
            if not self.attempted or self.expected is None:
                raise WindowOutcomeUnknown(PREPARATION_ERROR)
            # Explicit PREPARED predicate, never the CREATION_INTENT exit check.
            self.assert_current()
        except BaseException:
            self.failed = self.attempt._poisoned = True
            raise
        finally:
            self.attempt._busy = False

    def advance(self, object_identity: Any, receipt: PreparationReceipt) -> None:
        """One internal busy suboperation; caller cannot revive a consumed CAS."""
        if not self.attempt._busy or self.attempted or type(receipt) is not PreparationReceipt:
            raise WindowOutcomeUnknown(PREPARATION_ERROR)
        receipt.__post_init__()
        if self.evidence is None:
            raise WindowOutcomeUnknown(PREPARATION_ERROR)
        observed_receipt = self.evidence.snapshot
        if (
            type(observed_receipt) is not PreparationObservation
            or observed_receipt.receipt is not receipt
            or receipt.attempt_sha256 != attempt_identity_digest(self.parent.state.identity)
        ):
            raise WindowOutcomeUnknown(PREPARATION_ERROR)
        observed_receipt.__post_init__()
        self.assert_current()
        event = Prepared(object_identity, receipt.payload_sha256)
        predicted = advance_state(self.parent.state, event, expected_phase=TdsAttemptPhase.CREATION_INTENT)
        self.attempted = True
        observed = self.attempt._lifecycle.advance(
            event, expected_phase=TdsAttemptPhase.CREATION_INTENT, deadline=self.deadline
        )
        if type(observed) is not TdsAttemptSnapshot:
            raise WindowOutcomeUnknown(PREPARATION_ERROR)
        _original(observed)
        if (
            observed.state != predicted
            or observed.revision <= self.parent.revision
            or self.attempt._lifecycle.snapshot != observed
        ):
            raise WindowOutcomeUnknown(PREPARATION_ERROR)
        self.expected = deepcopy(observed)
        self.evidence_receipt = receipt
        self._origin.capture_terminal()
        self.assert_current()

    def finish_cleanup(self) -> None:
        """Use closure ACKs only; never reread actors after they were closed."""
        if (
            self.failed
            or self.expected is None
            or not self.handle._closed
            or self.attempt._preparation is not self
            or (self.evidence is not None and not self._evidence_closed)
        ):
            raise WindowOutcomeUnknown(PREPARATION_ERROR)
        self._origin.capture_exit()
        self.validate_origin()
        self.cleaned = True
        self.attempt._prepared_origin = self
        self.attempt._preparation = None
