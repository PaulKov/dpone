"""Finite parent-local producer capture; no CAS, actor reads or forward authority.

Mutable lifecycle policy remains on the original PreparationTransition. This
record pins its original capabilities and captures immutable nonsecret bytes at
producer boundaries. UNKNOWN observations are retained separately from success.
"""

from copy import deepcopy
from typing import Any

from dpone.services.mssql_tds_writer_contracts import (
    PreparationObservation,
    PreparationReceipt,
    WindowOutcomeUnknown,
    validate_original_record,
)

_ERROR = "mssql_native.sqlclient_preparation_transition_unknown"
_WRITER_INPUT_TOKEN = object()


class PreparationWriterInputs(tuple):
    """Immutable producer-owned references and snapshots for P10 admission."""

    def __new__(
        cls,
        token: object,
        transition: object,
        policy: object,
        input_descriptor: object,
        installation: object,
        policy_snapshot: bytes,
        input_snapshot: bytes,
        build_sha256: str,
        operation_deadline_ns: int,
        installation_type: type,
        content_expectation: object,
    ):
        if token is not _WRITER_INPUT_TOKEN:
            raise ValueError(_ERROR)
        return tuple.__new__(
            cls,
            (
                transition,
                policy,
                input_descriptor,
                installation,
                policy_snapshot,
                input_snapshot,
                build_sha256,
                operation_deadline_ns,
                installation_type,
                content_expectation,
            ),
        )

    transition = property(lambda self: self[0])
    policy = property(lambda self: self[1])
    input_descriptor = property(lambda self: self[2])
    installation = property(lambda self: self[3])
    policy_snapshot = property(lambda self: self[4])
    input_snapshot = property(lambda self: self[5])
    build_sha256 = property(lambda self: self[6])
    operation_deadline_ns = property(lambda self: self[7])
    installation_type = property(lambda self: self[8])
    content_expectation = property(lambda self: self[9])

    def __repr__(self) -> str:
        return "PreparationWriterInputs(<opaque>)"


class PreparationOrigin:
    """One capture record tied to the original transition, never caller proof."""

    def __init__(self, transition: Any, attempt_sha256: str) -> None:
        self._transition = transition
        if type(attempt_sha256) is not str or len(attempt_sha256) != 64:
            raise ValueError(_ERROR)
        self._attempt_sha256 = attempt_sha256
        self._references = self._reference_state()
        self._deadline = transition.deadline
        self._initial_values = deepcopy(self._initial_state())
        self._management: tuple | None = None
        self.evidence = transition.handle.continuation._evidence_acks
        self._original_evidence = self.evidence
        self.payload: bytes | None = None
        self.capture: tuple | None = None
        self._terminal_values: tuple | None = None
        self._writer_inputs: PreparationWriterInputs | None = None
        self.exit: Any = None
        self._exit_snapshot: Any = None

    def _reference_state(self) -> tuple:
        t = self._transition
        return (
            t.attempt,
            t.handle,
            t.handle.continuation,
            t.factory,
            t.pool,
            t.attempt._lifecycle,
            t.attempt._directory,
            t.handle.writer,
            t.handle.evidence,
            t.factory._factory,
        )

    def assert_references(self) -> None:
        t = self._transition
        PreparationOrigin._assert_management(self)
        initial = PreparationOrigin._initial_state(self)
        validate_original_record((initial, self._initial_values))
        if (
            initial != self._initial_values
            or t._captured_origin is not self
            or t._origin is not self
            or any(a is not b for a, b in zip(PreparationOrigin._reference_state(self), self._references, strict=True))
            or (
                t.handle.factory is not t.factory
                or t.handle.pool is not t.pool
                or type(t.factory._record.revision) is not int
                or type(t.factory._record.payload) is not str
                or type(t.deadline) is not float
                or t.deadline != self._deadline
                or t.handle.operation_deadline != self._deadline
            )
        ):
            raise WindowOutcomeUnknown(_ERROR)

    def capture_management(self) -> None:
        """Pin the actual opening inventory incarnation at its producer boundary."""
        t = self._transition
        if self._management is not None or t.opening is None:
            raise WindowOutcomeUnknown(_ERROR)
        incarnation = t.opening.inventory.management_before
        validate_original_record(incarnation)
        self._management = (t.opening, incarnation, deepcopy(incarnation))
        t.management_incarnation = incarnation

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
        """Pin producer-owned P10 inputs before preparation performs effects."""
        t = self._transition
        if (
            self._writer_inputs is not None
            or t.policy is not policy
            or t.input is not input_descriptor
            or t.build is not installation
            or type(policy_snapshot) is not bytes
            or type(input_snapshot) is not bytes
            or type(build_sha256) is not str
            or len(build_sha256) != 64
            or type(operation_deadline_ns) is not int
            or operation_deadline_ns <= 0
            or type(installation_type) is not type
            or type(installation) is not installation_type
        ):
            raise WindowOutcomeUnknown(_ERROR)
        from dpone.services.mssql_tds_writer_contracts import SqlClientStageContentExpectation

        if type(content_expectation) is not SqlClientStageContentExpectation:
            raise WindowOutcomeUnknown(_ERROR)
        content_expectation.__post_init__()
        self._writer_inputs = PreparationWriterInputs(
            _WRITER_INPUT_TOKEN,
            t,
            policy,
            input_descriptor,
            installation,
            policy_snapshot,
            input_snapshot,
            build_sha256,
            operation_deadline_ns,
            installation_type,
            content_expectation,
        )

    def writer_inputs(self) -> PreparationWriterInputs:
        """Return the exact captured references and immutable producer snapshots."""
        PreparationOrigin.assert_references(self)
        if self._writer_inputs is None:
            raise WindowOutcomeUnknown(_ERROR)
        captured = self._writer_inputs
        t = self._transition
        if (
            captured.transition is not t
            or t.policy is not captured.policy
            or t.input is not captured.input_descriptor
            or t.build is not captured.installation
        ):
            raise WindowOutcomeUnknown(_ERROR)
        return self._writer_inputs

    def _assert_management(self) -> None:
        t = self._transition
        if self._management is None:
            if t.opening is not None or t.management_incarnation is not None:
                raise WindowOutcomeUnknown(_ERROR)
            return
        opening, incarnation, snapshot = self._management
        validate_original_record((incarnation, snapshot, t.management_incarnation))
        if (
            t.opening is not opening
            or opening.inventory.management_before is not incarnation
            or t.management_incarnation is not incarnation
            or incarnation != snapshot
        ):
            raise WindowOutcomeUnknown(_ERROR)

    def begin(self, payload: bytes) -> None:
        """Retain bounded attempted bytes before a write can lose its ACK."""
        if self.capture is not None:
            raise WindowOutcomeUnknown(_ERROR)
        PreparationReceipt.for_payload(self._attempt_sha256, payload)
        self.payload = payload
        self.capture = (payload, None, None, None, self._transition.evidence)

    def acknowledge(self, payload: bytes, receipt: PreparationReceipt) -> None:
        """Capture the actual return before observation and deep ACK validation."""
        t = self._transition
        if self.capture != (payload, None, None, None, t.evidence) or self.payload is not payload:
            raise WindowOutcomeUnknown(_ERROR)
        expected = PreparationReceipt.for_payload(self._attempt_sha256, payload)
        self.capture = (payload, receipt, None, None, t.evidence)
        observed = t.evidence.snapshot
        self.capture = (payload, receipt, observed, deepcopy(receipt), t.evidence)
        validate_original_record((receipt, observed))
        if type(observed) is not PreparationObservation or observed.receipt is not receipt or receipt != expected:
            raise WindowOutcomeUnknown(_ERROR)

    def _initial_state(self) -> tuple:
        t = self._transition
        return (
            t.identity,
            t.startup,
            t.authority,
            t.management_admission,
            t.parent,
            t.directory,
            t.factory._record,
            t.factory._domain_id,
            self._attempt_sha256,
        )

    def _terminal_state(self) -> tuple:
        t = self._transition
        return (*self._initial_state(), t.coordinator, t.registration, t.management_incarnation, t.expected)

    def capture_terminal(self) -> None:
        """Capture value observations after the original Prepared CAS ACK."""
        self.assert_references()
        if self._terminal_values is not None:
            raise WindowOutcomeUnknown(_ERROR)
        self._terminal_values = deepcopy(self._terminal_state())

    def capture_exit(self) -> None:
        """Retain the actual containment result, without inventing successful exit0."""
        if self.exit is not None:
            raise WindowOutcomeUnknown(_ERROR)
        self.exit = self._transition.handle._contained_exit
        validate_original_record(self.exit)
        self._exit_snapshot = deepcopy(self.exit)

    def validate(self) -> None:
        """Read retained values only; the transition decides terminal activation."""
        t = self._transition
        self.assert_references()
        if t.failed or t.expected is None or self.capture is None:
            raise WindowOutcomeUnknown(_ERROR)
        payload, receipt, observed, snapshot, gateway = self.capture
        values = self._terminal_state()
        validate_original_record(
            (receipt, observed, snapshot, values, self._terminal_values, self.exit, self._exit_snapshot)
        )
        if (
            values != self._terminal_values
            or t.identity is not t.handle.identity
            or t.startup is not t.handle.startup_receipt
            or t.authority is not t.handle.authority
            or t.management_admission is not t.handle.request.management_admission
            or self.payload is not payload
            or t.evidence_receipt is not receipt
            or t.evidence is not gateway
            or receipt != snapshot
            or type(observed) is not PreparationObservation
            or observed.receipt is not receipt
            or receipt != PreparationReceipt.for_payload(self._attempt_sha256, payload)
            or self.evidence is not self._original_evidence
            or self.evidence is not t.handle.continuation.retained_evidence()
            or t.startup is None
            or self.exit is None
            or self.exit is not t.handle._contained_exit
            or self.exit != self._exit_snapshot
            or self.exit.identity != t.startup.process
            or self.exit.reaped is not True
        ):
            raise WindowOutcomeUnknown(_ERROR)
