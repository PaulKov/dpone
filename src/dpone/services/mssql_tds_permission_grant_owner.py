"""Custody and evidence state retained by a live SQL permission grant."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Never, Protocol

from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.ports.mssql_tds_coordinator import (
    AdvanceCoordinator,
    AssertCoordinatorAuthority,
    TdsCoordinatorGateway,
)
from dpone.services.mssql_tds_writer_contracts import (
    PermissionGrantEvidenceSubject,
    PermissionGrantParentEvidenceReceipt,
    PermissionGrantParentEvidenceRecord,
    PermissionWireBinding,
    TdsCoordinatorAuthority,
    TdsCoordinatorGrant,
    TdsCoordinatorPhase,
    TdsCoordinatorSnapshot,
    advance_coordinator_state,
    coordinator_identity_digest,
)
from dpone.services.mssql_tds_writer_contracts import (
    PermissionGrantParentEvidenceKind as EvidenceKind,
)

ERROR = "mssql_native.sqlclient_permission_held_unknown"


def permission_evidence_subject(binding: PermissionWireBinding) -> PermissionGrantEvidenceSubject:
    """Derive the evidence identity from one validated wire binding."""
    binding.snapshot()
    return PermissionGrantEvidenceSubject(
        attempt_identity_digest(binding.request.parent), coordinator_identity_digest(binding.operation)
    )


class PermissionAssociation(Protocol):
    def assert_ready(self) -> None: ...

    @property
    def identity(self): ...

    @property
    def reservation(self): ...


class PermissionProcess(Protocol):
    def receive_public(self, kind, ordinal, *, deadline=None): ...

    def send_public(self, kind, ordinal, body, *, deadline=None): ...

    def send_credentials(self, payload: bytes, *, deadline=None) -> None: ...


class PermissionLauncher(Protocol):
    def launch(self, inputs) -> PermissionProcess: ...


class PermissionEvidence(Protocol):
    observation: object

    def write(self, record, *, deadline: float): ...


class PermissionGrantHeldUnknown(ValueError):
    def __init__(self, owner: PermissionGrantHeldOwner) -> None:
        super().__init__(ERROR)
        self.owner = owner


@dataclass(slots=True, repr=False)
class PermissionGrantHeldOwner:
    """Exact live capabilities retained at HELD_READY; no settlement authority."""

    association: PermissionAssociation
    process: PermissionProcess | None
    coordinator: TdsCoordinatorGateway
    evidence: PermissionEvidence | None
    binding: PermissionWireBinding | None = field(default=None, repr=False)
    authority: TdsCoordinatorAuthority | None = field(default=None, repr=False)
    grant: TdsCoordinatorGrant | None = field(default=None, repr=False)
    result: object | None = field(default=None, repr=False)
    result_receipt: PermissionGrantParentEvidenceReceipt | None = field(default=None, repr=False)
    receipts: tuple[PermissionGrantParentEvidenceReceipt, ...] = field(default=(), repr=False)
    phase: str = "INTENT"
    _identity: object = field(default=None, repr=False)
    _reservation: object = field(default=None, repr=False)
    _current: TdsCoordinatorSnapshot | None = field(default=None, repr=False)
    _busy: bool = field(default=False, repr=False)
    _unknown: bool = field(default=False, repr=False)
    _association_ref: object = field(default=None, repr=False)
    _coordinator_ref: object = field(default=None, repr=False)
    _process_ref: object = field(default=None, repr=False)
    _evidence_ref: object = field(default=None, repr=False)
    _binding_snapshot: bytes | None = field(default=None, repr=False)
    _binding_ref: object = field(default=None, repr=False)
    _startup_ref: object = field(default=None, repr=False)
    _execution_owner_ref: object = field(default=None, repr=False)
    _operation_ref: object = field(default=None, repr=False)
    _authority_ref: object = field(default=None, repr=False)
    _grant_ref: object = field(default=None, repr=False)
    _result_ref: object = field(default=None, repr=False)
    _result_receipt_ref: object = field(default=None, repr=False)
    _receipt_refs: tuple[object, ...] = field(default=(), repr=False)
    _evidence_observation_ref: object = field(default=None, repr=False)

    def __post_init__(self) -> None:
        self._association_ref, self._coordinator_ref = self.association, self.coordinator

    def _fail(self) -> Never:
        self._unknown = True
        self.phase = "UNKNOWN"
        raise PermissionGrantHeldUnknown(self) from None

    def _check_refs(self) -> None:
        parent_changed = self.association is not self._association_ref or self.coordinator is not self._coordinator_ref
        process_changed = self._process_ref is not None and self.process is not self._process_ref
        evidence_changed = self._evidence_ref is not None and self.evidence is not self._evidence_ref
        if self._unknown or parent_changed or process_changed or evidence_changed:
            self._fail()

    def _read(self, callback):
        value = callback()
        self._check_refs()
        return value

    def _effect(self, callback, deadline: float, clock: Callable[[], float]):
        self._before(deadline, clock)
        value = callback()
        self._check_refs()
        return value

    def _before(self, deadline: float, clock: Callable[[], float]) -> None:
        self._check_refs()
        if not self._busy:
            self._fail()
        now = clock()
        self._check_refs()
        if type(deadline) is not float or not math.isfinite(deadline) or type(now) is not float or now >= deadline:
            self._fail()
        try:
            self.association.assert_ready()
            self._check_refs()
            identity = self._read(lambda: self.association.identity)
            reservation = self._read(lambda: self.association.reservation)
            if identity != self._identity or reservation != self._reservation:
                self._fail()
            observed = self.coordinator.execute(AssertCoordinatorAuthority(), deadline=deadline)
            self._check_refs()
            observation = self._read(lambda: self.coordinator.observation)
            snapshot = self._read(lambda: observation.snapshot)
            if (
                type(observed) is not TdsCoordinatorSnapshot
                or observed is not self._current
                or snapshot is not observed
            ):
                self._fail()
        except BaseException:
            self._fail()

    def _advance(self, event, deadline: float, clock: Callable[[], float]) -> None:
        self._before(deadline, clock)
        previous = self._current
        assert previous is not None
        try:
            expected = advance_coordinator_state(previous.state, event, expected_phase=previous.state.phase)
            observed = self.coordinator.execute(AdvanceCoordinator(event, previous.state.phase), deadline=deadline)
            self._check_refs()
            observation = self._read(lambda: self.coordinator.observation)
            snapshot = self._read(lambda: observation.snapshot)
            if (
                type(observed) is not TdsCoordinatorSnapshot
                or observed.state != expected
                or observed.revision <= previous.revision
                or snapshot is not observed
            ):
                self._fail()
            self._current = observed
        except BaseException:
            self._fail()

    def _persist(
        self, kind: EvidenceKind, payload: bytes, deadline: float, clock
    ) -> PermissionGrantParentEvidenceReceipt:
        self._before(deadline, clock)
        assert self.binding is not None and self.evidence is not None
        try:
            subject = permission_evidence_subject(self.binding)
            record = PermissionGrantParentEvidenceRecord(subject, kind, payload, binding=self.binding)
            expected = self._read(lambda: record.receipt)
            received = self.evidence.write(record, deadline=deadline)
            self._check_refs()
            observation = self._read(lambda: self.evidence.observation)
            observed_receipt = self._read(lambda: observation.receipt)
            valid_receipts = all(
                type(value) is PermissionGrantParentEvidenceReceipt for value in (received, observed_receipt)
            )
            if not valid_receipts or received != expected or observed_receipt != expected:
                self._fail()
            self._check_refs()
            self.receipts += (received,)
            self._receipt_refs += (received,)
            self._evidence_observation_ref = observed_receipt
            return received
        except BaseException:
            self._fail()

    def assert_held_ready(self) -> None:
        self._check_refs()
        if self._busy or self.phase != "HELD_READY" or len(self.receipts) != 8:
            self._fail()
        try:
            self.association.assert_ready()
            self._check_refs()
            identity = self._read(lambda: self.association.identity)
            reservation = self._read(lambda: self.association.reservation)
            assert self.binding is not None and self._current is not None and self.authority is not None
            assert self.process is not None and self.evidence is not None
            assert self.grant is not None and self.result_receipt is not None and self.result is not None
            for receipt in self.receipts:
                self._read(receipt.__post_init__)
            state = self._current.state
            coordinator_observation = self._read(lambda: self.coordinator.observation)
            coordinator_snapshot = self._read(lambda: coordinator_observation.snapshot)
            evidence_observation = self._read(lambda: self.evidence.observation)
            evidence_receipt = self._read(lambda: evidence_observation.receipt)
            binding_snapshot = self._read(self.binding.snapshot)
            if (
                identity != self._identity
                or reservation != self._reservation
                or binding_snapshot != self._binding_snapshot
                or self.binding is not self._binding_ref
                or self.binding.operation is not self._operation_ref
                or self.binding.startup is not self._startup_ref
                or self.binding.execution_owner is not self._execution_owner_ref
                or self.authority is not self._authority_ref
                or self.grant is not self._grant_ref
                or self.result is not self._result_ref
                or self.result_receipt is not self._result_receipt_ref
                or len(self._receipt_refs) != 8
                or any(
                    value is not reference for value, reference in zip(self.receipts, self._receipt_refs, strict=True)
                )
                or coordinator_snapshot is not self._current
                or evidence_receipt is not self._evidence_observation_ref
                or state.phase is not TdsCoordinatorPhase.RESULT_RECEIVED
                or state.identity != self.binding.operation
                or state.execution_owner != self.binding.execution_owner
                or state.process != self.binding.startup.process
                or state.session != self.authority.session
                or state.grant != self.grant
                or state.result is None
                or state.result.proof_sha256 != self.result_receipt.payload_sha256
            ):
                self._fail()
        except BaseException:
            self._fail()
