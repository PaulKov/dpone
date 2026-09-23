"""Release one exact P7 permission hold through proven local child exit."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Never, Protocol

from dpone.services.mssql_tds_permission_grant import PermissionGrantHeldOwner
from dpone.services.mssql_tds_writer_contracts import (
    RELEASE_KIND,
    RELEASED_KIND,
    PermissionGrantSettlementEvidenceContext,
    PermissionGrantSettlementEvidenceReceipt,
    PermissionGrantSettlementEvidenceRecord,
    PermissionGrantSettlementEvidenceSubject,
    encode_observed_permission_message,
    encode_permission_grant_local_exit,
    encode_permission_grant_release,
    permission_grant_settlement_subject,
    permission_grant_settlement_subject_snapshot,
    require_permission_grant_local_exit,
)
from dpone.services.mssql_tds_writer_contracts import (
    PermissionGrantSettlementEvidenceKind as EvidenceKind,
)

ERROR = "mssql_native.sqlclient_permission_local_release_unknown"


def permission_grant_settlement_evidence_context(subject, binding):
    """Build the validated actor context without coupling the adapter to contracts."""
    return PermissionGrantSettlementEvidenceContext(subject, binding)


class _Evidence(Protocol):
    observation: object

    def write(self, record, *, deadline: float): ...


class PermissionGrantLocalReleaseUnknown(ValueError):
    def __init__(self, owner: PermissionGrantLocallyReleased) -> None:
        super().__init__(ERROR)
        self.owner = owner


@dataclass(slots=True, repr=False)
class PermissionGrantLocallyReleased:
    """Retained local result; remote SQL settlement remains unproven."""

    held_owner: PermissionGrantHeldOwner
    settlement_evidence: _Evidence | None = None
    subject: PermissionGrantSettlementEvidenceSubject | None = None
    local_exit: object | None = None
    receipts: tuple[PermissionGrantSettlementEvidenceReceipt, ...] = field(default=(), repr=False)
    phase: str = "HELD_READY"
    _busy: bool = field(default=True, repr=False)
    _unknown: bool = field(default=False, repr=False)
    _held_ref: object = field(default=None, repr=False)
    _association_ref: object = field(default=None, repr=False)
    _coordinator_ref: object = field(default=None, repr=False)
    _process_ref: object = field(default=None, repr=False)
    _binding_ref: object = field(default=None, repr=False)
    _result_receipt_ref: object = field(default=None, repr=False)
    _held_receipt_refs: tuple[object, ...] = field(default=(), repr=False)
    _subject_ref: object = field(default=None, repr=False)
    _subject_snapshot: bytes | None = field(default=None, repr=False)
    _local_exit_ref: object = field(default=None, repr=False)
    _evidence_ref: object = field(default=None, repr=False)
    _receipt_refs: tuple[object, ...] = field(default=(), repr=False)
    _observation_ref: object = field(default=None, repr=False)

    def __post_init__(self) -> None:
        self._held_ref = self.held_owner
        self._association_ref = self.held_owner.association
        self._coordinator_ref = self.held_owner.coordinator
        self._process_ref = self.held_owner.process
        self._binding_ref = self.held_owner.binding
        self._result_receipt_ref = self.held_owner.result_receipt
        self._held_receipt_refs = tuple(self.held_owner.receipts)

    @property
    def association(self):
        return self.held_owner.association

    @property
    def coordinator(self):
        return self.held_owner.coordinator

    @property
    def process(self):
        return self.held_owner.process

    @property
    def binding(self):
        return self.held_owner.binding

    def _fail(self) -> Never:
        self._unknown = True
        self.phase = "UNKNOWN"
        raise PermissionGrantLocalReleaseUnknown(self) from None

    def _check_refs(self) -> None:
        try:
            held = self.held_owner
            if self._unknown:
                self._fail()
            held.assert_held_ready()
            if (
                held is not self._held_ref
                or held.association is not self._association_ref
                or held.coordinator is not self._coordinator_ref
                or held.process is not self._process_ref
                or held.binding is not self._binding_ref
                or held.result_receipt is not self._result_receipt_ref
                or len(held.receipts) != 8
                or len(self._held_receipt_refs) != 8
                or any(
                    value is not reference
                    for value, reference in zip(held.receipts, self._held_receipt_refs, strict=True)
                )
                or (
                    self._subject_ref is not None
                    and (
                        self.subject is not self._subject_ref
                        or permission_grant_settlement_subject_snapshot(self.subject) != self._subject_snapshot
                    )
                )
                or (self._local_exit_ref is not None and self.local_exit is not self._local_exit_ref)
                or (self._evidence_ref is not None and self.settlement_evidence is not self._evidence_ref)
            ):
                self._fail()
        except PermissionGrantLocalReleaseUnknown:
            raise
        except BaseException:
            self._fail()

    def _read(self, callback):
        try:
            value = callback()
            self._check_refs()
            return value
        except PermissionGrantLocalReleaseUnknown:
            raise
        except BaseException:
            self._fail()

    def _before(self, deadline: float, clock: Callable[[], float]) -> None:
        self._check_refs()
        if not self._busy:
            self._fail()
        try:
            now = clock()
            self._check_refs()
            if type(deadline) is not float or not math.isfinite(deadline) or type(now) is not float or now >= deadline:
                self._fail()
            if self._read(lambda: getattr(self.association, "_held_ready_owner", None)) is not self.held_owner:
                self._fail()
            if self._read(lambda: getattr(self.association, "_local_release_owner", None)) is not self:
                self._fail()
        except BaseException:
            self._fail()

    def _effect(self, callback, deadline: float, clock: Callable[[], float]):
        self._before(deadline, clock)
        try:
            value = callback()
            self._check_refs()
            return value
        except BaseException:
            self._fail()

    def _persist(self, kind: EvidenceKind, payload: bytes, deadline: float, clock) -> None:
        self._before(deadline, clock)
        evidence = self.settlement_evidence
        binding = self.binding
        if evidence is None or binding is None or self.subject is None:
            self._fail()
        try:
            record = PermissionGrantSettlementEvidenceRecord(self.subject, kind, payload, binding=binding)
            expected = self._read(lambda: record.receipt)
            received = evidence.write(record, deadline=deadline)
            self._check_refs()
            observation = self._read(lambda: evidence.observation)
            observed = self._read(lambda: observation.receipt)
            if (
                type(received) is not PermissionGrantSettlementEvidenceReceipt
                or type(observed) is not PermissionGrantSettlementEvidenceReceipt
                or received != expected
                or observed != expected
            ):
                self._fail()
            self.receipts += (received,)
            self._receipt_refs += (received,)
            self._observation_ref = observed
        except BaseException:
            self._fail()

    def assert_local_exit(self) -> None:
        self._check_refs()
        if self._busy or self.phase != "LOCAL_EXIT" or len(self.receipts) != 3:
            self._fail()
        try:
            if self._read(lambda: getattr(self.association, "_local_release_owner", None)) is not self:
                self._fail()
            for value, reference in zip(self.receipts, self._receipt_refs, strict=True):
                self._read(value.__post_init__)
                if value is not reference:
                    self._fail()
            evidence = self.settlement_evidence
            if evidence is None:
                self._fail()
            observation = self._read(lambda: evidence.observation)
            if self._read(lambda: observation.receipt) is not self._observation_ref:
                self._fail()
            exit_value = self.local_exit
            binding = self.binding
            if (
                binding is None
                or require_permission_grant_local_exit(exit_value, binding, exact_identity=True) is not exit_value
            ):
                self._fail()
        except BaseException:
            self._fail()


def release_permission_grant_locally(
    held_owner: PermissionGrantHeldOwner,
    evidence_factory: Callable[[PermissionGrantSettlementEvidenceSubject, object], _Evidence],
    *,
    deadline: float,
    containment_deadline: float | None,
    clock: Callable[[], float],
) -> PermissionGrantLocallyReleased:
    """Release ordinal five once and retain exact local settlement authority."""
    if type(held_owner) is not PermissionGrantHeldOwner:
        raise ValueError(ERROR)
    owner = PermissionGrantLocallyReleased(held_owner)
    try:
        held_owner.assert_held_ready()
        owner._check_refs()
        previous = owner._read(lambda: getattr(owner.association, "_local_release_owner", None))
        if previous is not None:
            if type(previous) is PermissionGrantLocallyReleased:
                previous._fail()
            owner._fail()
        setattr(owner.association, "_local_release_owner", owner)
        owner._before(deadline, clock)
        binding = owner.binding
        result_receipt = held_owner.result_receipt
        held_receipt = held_owner.receipts[-1]
        if binding is None or result_receipt is None:
            owner._fail()
        subject = permission_grant_settlement_subject(held_receipt, result_receipt, binding)
        owner.subject = owner._subject_ref = subject
        owner._subject_snapshot = permission_grant_settlement_subject_snapshot(subject)
        parent_evidence = held_owner.evidence
        if parent_evidence is None:
            owner._fail()
        close_parent_evidence = owner._read(lambda: getattr(parent_evidence, "close"))
        owner._effect(lambda: close_parent_evidence(deadline=deadline), deadline, clock)
        settlement_evidence = owner._effect(lambda: evidence_factory(subject, binding), deadline, clock)
        owner.settlement_evidence = owner._evidence_ref = settlement_evidence
        body = {"evidence_sha256": result_receipt.payload_sha256}
        release_payload = encode_permission_grant_release(binding, result_receipt.payload_sha256)
        owner._persist(EvidenceKind.RELEASE_INTENT, release_payload, deadline, clock)
        process = owner.process
        if process is None:
            owner._fail()
        sent = owner._effect(lambda: process.send_public(RELEASE_KIND, 5, body, deadline=deadline), deadline, clock)
        sent_payload = encode_observed_permission_message(binding, sent, kind=RELEASE_KIND)
        if sent_payload != release_payload:
            owner._fail()
        released = owner._effect(lambda: process.receive_public(RELEASED_KIND, 5, deadline=deadline), deadline, clock)
        released_payload = encode_observed_permission_message(binding, released, kind=RELEASED_KIND)
        owner._persist(EvidenceKind.RELEASED, released_payload, deadline, clock)
        owner._effect(lambda: process.observe_eof(deadline=deadline), deadline, clock)
        exit_value = owner._effect(
            lambda: process.settle(
                natural_deadline=deadline,
                containment_deadline=containment_deadline,
            ),
            deadline,
            clock,
        )
        # The process adapter retains the pre-wire identity instance, while the
        # held owner retains the canonically decoded STARTUP identity.  They are
        # necessarily distinct objects in a real process even though they bind
        # the same pidfd-observed process.  Validate that immutable value first,
        # then rebind the retained local receipt to this owner's exact identity.
        require_permission_grant_local_exit(exit_value, binding, exact_identity=False)
        if exit_value.identity is not binding.startup.process:
            exit_value = type(exit_value)(binding.startup.process, exit_value.exit_code, exit_value.reaped)
        if require_permission_grant_local_exit(exit_value, binding, exact_identity=True) is not exit_value:
            owner._fail()
        owner.local_exit = owner._local_exit_ref = exit_value
        owner._persist(EvidenceKind.LOCAL_EXIT, encode_permission_grant_local_exit(exit_value), deadline, clock)
        owner.phase, owner._busy = "LOCAL_EXIT", False
        owner.assert_local_exit()
        return owner
    except PermissionGrantLocalReleaseUnknown:
        raise
    except BaseException:
        owner._fail()
