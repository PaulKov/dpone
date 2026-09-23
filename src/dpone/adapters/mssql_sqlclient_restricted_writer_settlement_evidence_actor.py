"""Create-only one-record evidence actor for P9b remote settlement."""

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Protocol, cast

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1

ERROR = "mssql_native.sqlclient_restricted_writer_settlement_invalid"


class SettlementEvidenceOperations(Protocol):
    record_type: type

    def validate_record(self, value: object) -> None: ...
    def receipt(self, value: object) -> object: ...


@dataclass(frozen=True, slots=True)
class RestrictedWriterSettlementObservation:
    attempt_sha256: str
    operation_sha256: str
    receipt: object | None = None


class RestrictedWriterSettlementEvidenceActor(
    _ActorCore[CreateOnlyEvidenceWriterV1, RestrictedWriterSettlementObservation]
):
    """Persist one exact REMOTE_SETTLEMENT and permanently consume the attempt."""

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]],
        attempt_sha256: str,
        operation_sha256: str,
        operations: SettlementEvidenceOperations,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        self._attempt_sha256, self._operation_sha256, self._operations = attempt_sha256, operation_sha256, operations
        self._attempted = self._persisted = self._writing = False
        super().__init__(factory, deadline, clock, RestrictedWriterSettlementObservation)

    @property
    def observation(self) -> RestrictedWriterSettlementObservation:
        return self.snapshot

    def _initial(self, backend: CreateOnlyEvidenceWriterV1) -> RestrictedWriterSettlementObservation:
        return RestrictedWriterSettlementObservation(self._attempt_sha256, self._operation_sha256)

    def _checked(self, value: object) -> Any:
        if type(value) is not self._operations.record_type:
            raise ValueError(ERROR)
        self._operations.validate_record(value)
        checked = cast(Any, value)
        if (checked.attempt_sha256, checked.operation_sha256) != (self._attempt_sha256, self._operation_sha256):
            raise ValueError(ERROR)
        return checked

    def write(self, record: object, *, deadline: float) -> object:
        self._owned()
        if self._attempted or self._writing:
            self._shutdown()
            raise TdsJournalActorUnknown(self)
        self._attempted = self._writing = True
        previous = self.snapshot
        try:
            checked = self._checked(record)
            expected = self._operations.receipt(checked)
            observed = self._call(_ActorCommand("remote_settlement", deadline, checked))
            if (
                type(observed) is not RestrictedWriterSettlementObservation
                or observed
                != RestrictedWriterSettlementObservation(self._attempt_sha256, self._operation_sha256, expected)
                or self.snapshot != observed
            ):
                raise TdsJournalActorUnknown(self)
            self._writing = False
            return expected
        except BaseException as error:
            self._snapshot = previous
            self._shutdown()
            if isinstance(error, (KeyboardInterrupt, SystemExit, GeneratorExit, TdsJournalActorUnknown)):
                raise
            raise TdsJournalActorUnknown(self) from None

    def _dispatch(
        self,
        backend: CreateOnlyEvidenceWriterV1,
        command: _ActorCommand[RestrictedWriterSettlementObservation],
    ) -> RestrictedWriterSettlementObservation:
        if command.kind != "remote_settlement" or self._persisted:
            raise TdsJournalActorUnknown(self)
        record = self._checked(command.event)
        receipt = cast(Any, self._operations.receipt(record))
        record = cast(Any, record)
        self._persisted = True
        backend.write(receipt.relative_name, record.payload)
        return RestrictedWriterSettlementObservation(self._attempt_sha256, self._operation_sha256, receipt)
