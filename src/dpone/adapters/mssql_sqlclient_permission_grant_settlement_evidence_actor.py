"""Create-only actor for the three-record local GRANT settlement chain."""

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1

ERROR = "mssql_native.sqlclient_permission_settlement_evidence_invalid"
KINDS = ("release_intent", "released", "local_exit", "remote_settlement")


@dataclass(frozen=True, slots=True)
class _SettlementObservation:
    subject: object
    receipt: object | None = None


class SqlClientPermissionGrantSettlementEvidenceActor(_ActorCore[CreateOnlyEvidenceWriterV1, _SettlementObservation]):
    """Persist RELEASE_INTENT, RELEASED and LOCAL_EXIT on one actor."""

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]],
        context: object,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        self._context_value = context
        self._subject = getattr(context, "subject")
        self._attempted_index = self._persisted_index = 0
        self._persisted: dict[str, Any] = {}
        self._writing = False
        super().__init__(factory, deadline, clock, _SettlementObservation)

    @property
    def observation(self) -> _SettlementObservation:
        value = self.snapshot
        return value

    def _initial(self, backend: CreateOnlyEvidenceWriterV1) -> _SettlementObservation:
        return _SettlementObservation(self._subject)

    def _record(self, value: object) -> Any:
        return getattr(self._context_value, "record")(value)

    def _poison(self, previous: _SettlementObservation | None = None) -> None:
        if previous is not None:
            self._snapshot = previous
        self._writing = True
        self._shutdown()

    def write(self, record: object, *, deadline: float) -> object:
        self._owned()
        if self._writing or self._stop.is_set():
            self._poison()
            raise TdsJournalActorUnknown(self)
        checked_record = self._record(record)
        kind = checked_record.kind.value
        if self._attempted_index >= len(KINDS) or kind != KINDS[self._attempted_index]:
            raise ValueError(ERROR)
        previous = self.snapshot
        self._attempted_index += 1
        self._writing = True
        try:
            expected = checked_record.receipt
            observed = self._call(_ActorCommand("settlement", deadline, checked_record))
            if (
                type(observed) is not _SettlementObservation
                or observed != _SettlementObservation(self._subject, expected)
                or observed != self.snapshot
            ):
                raise TdsJournalActorUnknown(self)
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
        command: _ActorCommand[_SettlementObservation],
    ) -> _SettlementObservation:
        if command.kind != "settlement":
            raise TdsJournalActorUnknown(self)
        record = self._record(command.event)
        kind = record.kind.value
        if self._persisted_index >= len(KINDS) or kind != KINDS[self._persisted_index]:
            raise TdsJournalActorUnknown(self)
        previous = None if self._persisted_index == 0 else self._persisted[KINDS[self._persisted_index - 1]]
        getattr(self._context_value, "transition")(previous, record)
        receipt = record.receipt
        self._persisted_index += 1
        self._persisted[kind] = record
        backend.write(receipt.relative_name, record.payload)
        return _SettlementObservation(self._subject, receipt)
