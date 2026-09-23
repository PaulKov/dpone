"""Create-only actor for the closed permission parent evidence sequence.

Durable bytes are not SQL, process, message-delivery, or session-held proof.
The caller must never retry an admitted submission whose ACK is uncertain.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.contracts.mssql_tds_api import (
    PermissionGrantEvidenceSubject,
    PermissionGrantParentEvidenceContext,
    PermissionGrantParentEvidenceKind,
    PermissionGrantParentEvidenceObservation,
    PermissionGrantParentEvidenceReceipt,
    PermissionGrantParentEvidenceRecord,
    authority_bytes,
    held_ready_bindings,
    registration_admission_sha256,
)
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1

K = PermissionGrantParentEvidenceKind
ERROR = "mssql_native.sqlclient_permission_parent_evidence_invalid"


class SqlClientPermissionGrantEvidenceActor(
    _ActorCore[CreateOnlyEvidenceWriterV1, PermissionGrantParentEvidenceObservation]
):
    """Persist exactly eight ordered records on one actor-owned writer."""

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]],
        subject: PermissionGrantEvidenceSubject,
        binding: object,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        self._evidence_context = PermissionGrantParentEvidenceContext(subject, binding)
        self._subject = self._evidence_context.subject
        self._attempted_index = 0
        self._persisted_index = 0
        self._persisted_receipts: dict[K, PermissionGrantParentEvidenceReceipt] = {}
        self._authority_ack: bytes | None = None
        self._writing = False
        super().__init__(factory, deadline, clock, PermissionGrantParentEvidenceObservation)

    @property
    def observation(self) -> PermissionGrantParentEvidenceObservation:
        observation = self.snapshot
        PermissionGrantParentEvidenceObservation(observation.subject, observation.receipt)
        return observation

    def _record(self, value: object) -> PermissionGrantParentEvidenceRecord:
        return self._evidence_context.record(value)

    def _initial(self, backend: CreateOnlyEvidenceWriterV1) -> PermissionGrantParentEvidenceObservation:
        return PermissionGrantParentEvidenceObservation(self._subject)

    def _poison(self, previous: PermissionGrantParentEvidenceObservation | None = None) -> None:
        if previous is not None:
            self._snapshot = previous
        self._writing = True
        self._shutdown()

    def write(
        self, record: PermissionGrantParentEvidenceRecord, *, deadline: float
    ) -> PermissionGrantParentEvidenceReceipt:
        self._owned()
        if self._writing or self._stop.is_set():
            self._poison()
            raise TdsJournalActorUnknown(self)
        record = self._record(record)
        kinds = tuple(K)
        if self._attempted_index >= len(kinds) or record.kind is not kinds[self._attempted_index]:
            raise ValueError(ERROR)
        previous = self.snapshot
        self._attempted_index += 1
        self._writing = True
        try:
            expected = record.receipt
            observed = self._call(_ActorCommand("evidence", deadline, record))
            checked = PermissionGrantParentEvidenceObservation(observed.subject, observed.receipt)
            if (
                type(observed) is not PermissionGrantParentEvidenceObservation
                or checked != PermissionGrantParentEvidenceObservation(self._subject, expected)
                or checked != self.snapshot
            ):
                raise TdsJournalActorUnknown(self)
            if record.kind is K.AUTHORITY:
                self._authority_ack = authority_bytes(record)
            self._writing = False
            return expected
        except BaseException as exc:
            self._poison(previous)
            if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit, TdsJournalActorUnknown)):
                raise
            raise TdsJournalActorUnknown(self) from None

    def _dispatch(
        self,
        backend: CreateOnlyEvidenceWriterV1,
        command: _ActorCommand[PermissionGrantParentEvidenceObservation],
    ) -> PermissionGrantParentEvidenceObservation:
        if command.kind != "evidence":
            raise TdsJournalActorUnknown(self)
        record = self._record(command.event)
        kinds = tuple(K)
        if self._persisted_index >= len(kinds) or record.kind is not kinds[self._persisted_index]:
            raise TdsJournalActorUnknown(self)
        if record.kind is K.REGISTRATION:
            admission = self._persisted_receipts[K.ADMISSION].payload_sha256
            if registration_admission_sha256(record) != admission:
                raise TdsJournalActorUnknown(self)
        if record.kind is K.HELD_READY:
            result_digest = self._persisted_receipts[K.RESULT].payload_sha256
            claimed, authority = held_ready_bindings(record)
            if claimed != result_digest or self._authority_ack is None or authority != self._authority_ack:
                raise TdsJournalActorUnknown(self)
        receipt = record.receipt
        self._persisted_index += 1
        self._persisted_receipts[record.kind] = receipt
        backend.write(receipt.relative_name, record.payload)
        return PermissionGrantParentEvidenceObservation(self._subject, receipt)
