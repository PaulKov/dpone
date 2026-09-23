"""SqlClient worker evidence persistence through the shared bounded actor owner.

Only the actor enters the injected writer context and invokes synchronous write.
Typed codecs revalidate full nested payloads and original result context at both
boundaries. Referenced ACKs and actual authority remain supervisor obligations.
It never imports app codecs, releases actor capacity early or proves SQL facts.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.contracts.mssql_tds_api import (
    SqlClientEvidenceKind,
    SqlClientEvidenceObservation,
    SqlClientEvidenceReceipt,
    SqlClientEvidenceRecord,
)
from dpone.contracts.mssql_tds_validation import _hash
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1


class SqlClientEvidenceActor(_ActorCore[CreateOnlyEvidenceWriterV1, SqlClientEvidenceObservation]):
    """At most one acknowledged payload per kind; use the run's TdsActorPool.

    Constructor is allocation-only. The factory must construct its concrete
    writer inside context entry, including any filesystem root validation.
    Failed writes remain unknown; another actor may verify identical durable
    bytes during explicit recovery, but this gateway never retries delivery.
    """

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]],
        attempt_sha256: str,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        _hash(attempt_sha256)
        self._attempt_sha256 = attempt_sha256
        self._attempted: set[SqlClientEvidenceKind] = set()
        self._persisted: set[SqlClientEvidenceKind] = set()
        super().__init__(factory, deadline, clock, SqlClientEvidenceObservation)

    @property
    def observation(self) -> SqlClientEvidenceObservation:
        return self.snapshot

    def _validate(self, record: object) -> SqlClientEvidenceRecord:
        if type(record) is not SqlClientEvidenceRecord or record.attempt_sha256 != self._attempt_sha256:
            raise ValueError("mssql_native.tds_evidence_attempt_invalid")
        # Recheck even frozen values at each trust boundary, before backend work.
        return SqlClientEvidenceRecord(record.attempt_sha256, record.kind, record.payload, record.result_context)

    def _initial(self, backend: CreateOnlyEvidenceWriterV1) -> SqlClientEvidenceObservation:
        return SqlClientEvidenceObservation(self._attempt_sha256)

    def write(self, record: SqlClientEvidenceRecord, *, deadline: float) -> SqlClientEvidenceReceipt:
        self._owned()
        record = self._validate(record)
        if record.kind in self._attempted:
            raise ValueError("mssql_native.tds_evidence_kind_consumed")
        self._attempted.add(record.kind)
        expected = record.receipt
        previous = self.snapshot
        observed = self._call(_ActorCommand("evidence", deadline, record))
        try:
            # Dataclass equality accepts scalar aliases (for example int/float).
            # The core snapshot is provisional until this full typed ACK check.
            if type(observed) is not SqlClientEvidenceObservation:
                raise ValueError
            observed.__post_init__()
            if observed != SqlClientEvidenceObservation(self._attempt_sha256, expected):
                raise ValueError
        except (ValueError, TypeError, OverflowError, RecursionError):
            self._snapshot = previous
            self._shutdown()
            raise TdsJournalActorUnknown(self) from None
        acknowledged = observed.receipt
        if type(acknowledged) is not SqlClientEvidenceReceipt:
            self._snapshot = previous
            self._shutdown()
            raise TdsJournalActorUnknown(self)
        return acknowledged

    def _dispatch(
        self, backend: CreateOnlyEvidenceWriterV1, command: _ActorCommand[SqlClientEvidenceObservation]
    ) -> SqlClientEvidenceObservation:
        if command.kind != "evidence":
            raise TdsJournalActorUnknown()
        record = self._validate(command.event)
        if record.kind in self._persisted:
            raise TdsJournalActorUnknown()
        receipt = record.receipt
        backend.write(receipt.relative_name, record.payload)
        self._persisted.add(record.kind)
        return SqlClientEvidenceObservation(self._attempt_sha256, receipt)
