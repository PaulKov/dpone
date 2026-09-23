"""Coordinator evidence persistence through the shared bounded actor owner.

Only the actor enters the injected writer context and invokes synchronous write.
The trusted app validates nested result/authority semantics first; this component
checks closed outer shape, canonical bytes, operation binding and exact receipt.
It never imports app codecs, releases actor capacity early or proves SQL facts.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.contracts.mssql_tds_api import (
    TdsCoordinatorEvidenceKind,
    TdsCoordinatorEvidenceObservation,
    TdsCoordinatorEvidenceReceipt,
    TdsCoordinatorEvidenceRecord,
)
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1, ExactEvidenceReaderV1


class TdsCoordinatorEvidenceActor(_ActorCore[CreateOnlyEvidenceWriterV1, TdsCoordinatorEvidenceObservation]):
    """At most one acknowledged payload per kind; use the run's TdsActorPool.

    Constructor is allocation-only. The factory must construct its concrete
    writer inside context entry, including any filesystem root validation.
    Failed writes remain unknown; another actor may verify identical durable
    bytes during explicit recovery, but this gateway never retries delivery.
    """

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]],
        operation_sha256: str,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        observation = TdsCoordinatorEvidenceObservation(operation_sha256)
        self._operation_sha256 = observation.operation_sha256
        self._attempted: set[TdsCoordinatorEvidenceKind] = set()
        self._persisted: set[TdsCoordinatorEvidenceKind] = set()
        super().__init__(factory, deadline, clock, TdsCoordinatorEvidenceObservation)

    @property
    def observation(self) -> TdsCoordinatorEvidenceObservation:
        return self.snapshot

    def _validate(self, record: object) -> TdsCoordinatorEvidenceRecord:
        if type(record) is not TdsCoordinatorEvidenceRecord or record.operation_sha256 != self._operation_sha256:
            raise ValueError("mssql_native.tds_evidence_operation_invalid")
        # Recheck even frozen values at each trust boundary, before backend work.
        return TdsCoordinatorEvidenceRecord(record.operation_sha256, record.kind, record.payload)

    def _initial(self, backend: CreateOnlyEvidenceWriterV1) -> TdsCoordinatorEvidenceObservation:
        return TdsCoordinatorEvidenceObservation(self._operation_sha256)

    def write(self, record: TdsCoordinatorEvidenceRecord, *, deadline: float) -> TdsCoordinatorEvidenceReceipt:
        self._owned()
        record = self._validate(record)
        if record.kind in self._attempted:
            raise ValueError("mssql_native.tds_evidence_kind_consumed")
        self._attempted.add(record.kind)
        expected = record.receipt
        previous = self.snapshot
        observed = self._call(_ActorCommand("evidence", deadline, record))
        if observed != TdsCoordinatorEvidenceObservation(self._operation_sha256, expected):
            self._snapshot = previous
            self._shutdown()
            raise TdsJournalActorUnknown(self)
        return expected

    def _dispatch(
        self, backend: CreateOnlyEvidenceWriterV1, command: _ActorCommand[TdsCoordinatorEvidenceObservation]
    ) -> TdsCoordinatorEvidenceObservation:
        if command.kind != "evidence":
            raise TdsJournalActorUnknown()
        record = self._validate(command.event)
        if record.kind in self._persisted:
            raise TdsJournalActorUnknown()
        receipt = record.receipt
        backend.write(receipt.relative_name, record.payload)
        self._persisted.add(record.kind)
        return TdsCoordinatorEvidenceObservation(self._operation_sha256, receipt)


class TdsCoordinatorEvidenceReadActor(_ActorCore[ExactEvidenceReaderV1, tuple]):
    """Six exact reads in one pinned context on the existing reserved pool slot."""

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[ExactEvidenceReaderV1]],
        receipts: tuple[TdsCoordinatorEvidenceReceipt, ...],
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        if type(receipts) is not tuple or len(receipts) != len(TdsCoordinatorEvidenceKind):
            raise ValueError("mssql_native.tds_evidence_read_receipts_invalid")
        copied = []
        for receipt, kind in zip(receipts, TdsCoordinatorEvidenceKind, strict=True):
            if type(receipt) is not TdsCoordinatorEvidenceReceipt or receipt.kind is not kind:
                raise ValueError("mssql_native.tds_evidence_read_receipts_invalid")
            copied.append(
                TdsCoordinatorEvidenceReceipt(
                    receipt.operation_sha256,
                    receipt.kind,
                    receipt.relative_name,
                    receipt.payload_sha256,
                    receipt.byte_count,
                )
            )
        if len({r.operation_sha256 for r in copied}) != 1:
            raise ValueError("mssql_native.tds_evidence_read_receipts_invalid")
        self._receipts = tuple(copied)
        super().__init__(factory, deadline, clock, tuple)

    def _initial(self, backend: ExactEvidenceReaderV1) -> tuple:
        payloads = []
        for receipt in self._receipts:
            payload = backend.read(receipt.relative_name, receipt.byte_count, receipt.payload_sha256)
            if TdsCoordinatorEvidenceRecord(receipt.operation_sha256, receipt.kind, payload).receipt != receipt:
                raise ValueError("mssql_native.tds_evidence_read_mismatch")
            payloads.append(payload)
        return tuple(payloads)

    def _dispatch(self, backend: ExactEvidenceReaderV1, command: _ActorCommand[tuple]) -> tuple:
        raise TdsJournalActorUnknown(self)
