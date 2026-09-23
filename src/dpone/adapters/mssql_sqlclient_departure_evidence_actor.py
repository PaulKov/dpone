"""Fixed-subject helper evidence persistence on the shared bounded actor core.

The actor alone enters the injected writer context. Typed validation precedes
enqueue and persistence; acknowledged snapshots survive later uncertainty.
Content consistency never authenticates a producer or an ordered parent chain.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager
from uuid import UUID

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.contracts.mssql_sqlclient_departure_evidence import (
    ERROR,
    SqlClientDepartureEvidenceKind,
    SqlClientDepartureEvidenceObservation,
    SqlClientDepartureEvidenceReceipt,
    SqlClientDepartureEvidenceRecord,
)
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1

_FAILURES = (ValueError, TypeError, AttributeError, OverflowError, RecursionError, UnicodeError)


class SqlClientDepartureEvidenceActor(_ActorCore[CreateOnlyEvidenceWriterV1, SqlClientDepartureEvidenceObservation]):
    """Allocation-only gateway; admit through the run's existing TdsActorPool.

    Factory construction and context entry must create the concrete writer on
    the actor thread. Attempts consume a kind permanently, even after timeout.
    A late durable write is unknown, not a new ACK or permission to retry.
    Inherited close waits for this actor without closing the shared pool.
    """

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]],
        helper_id: UUID,
        attempt_sha256: str,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        self._empty = SqlClientDepartureEvidenceObservation(helper_id, attempt_sha256)
        self._helper_id, self._attempt_sha256 = helper_id, attempt_sha256
        self._attempted: set[SqlClientDepartureEvidenceKind] = set()
        self._persisted: set[SqlClientDepartureEvidenceKind] = set()
        self._writing = False
        super().__init__(factory, deadline, clock, SqlClientDepartureEvidenceObservation)

    @property
    def observation(self) -> SqlClientDepartureEvidenceObservation:
        """Expose only the last fully checked ACK, never a late backend result."""
        return self.snapshot

    def _checked_observation(self, value: object) -> SqlClientDepartureEvidenceObservation:
        if type(value) is not SqlClientDepartureEvidenceObservation:
            raise ValueError(ERROR)
        return SqlClientDepartureEvidenceObservation(value.helper_id, value.attempt_sha256, value.receipt)

    def _await_ready(self) -> None:
        super()._await_ready()
        try:
            if self._checked_observation(self.snapshot) != self._empty:
                raise ValueError(ERROR)
        except _FAILURES:
            self._snapshot = None
            self._shutdown()
            raise TdsJournalActorUnknown(self) from None

    def _initial(self, backend: CreateOnlyEvidenceWriterV1) -> SqlClientDepartureEvidenceObservation:
        return self._empty

    def _validate(self, value: object) -> SqlClientDepartureEvidenceRecord:
        try:
            if type(value) is not SqlClientDepartureEvidenceRecord:
                raise ValueError(ERROR)
            record = SqlClientDepartureEvidenceRecord(
                value.helper_id,
                value.attempt_sha256,
                value.kind,
                value.payload,
                value.result_context,
                observe_plan=value.observe_plan,
                observe_request=value.observe_request,
                observer_admission=value.observer_admission,
                permission_grant_context=value.permission_grant_context,
                restricted_writer_context=value.restricted_writer_context,
            )
            if record.helper_id != self._helper_id or record.attempt_sha256 != self._attempt_sha256:
                raise ValueError(ERROR)
            return record
        except _FAILURES:
            raise ValueError(ERROR) from None

    def write(self, record: SqlClientDepartureEvidenceRecord, *, deadline: float) -> SqlClientDepartureEvidenceReceipt:
        """Revalidate both boundaries and retain the prior ACK on any uncertainty."""
        self._owned()
        if self._writing:
            self._shutdown()
            raise TdsJournalActorUnknown(self)
        self._writing = True
        try:
            record = self._validate(record)
            if record.kind in self._attempted:
                raise ValueError("mssql_native.sqlclient_departure_evidence_kind_consumed")
            self._attempted.add(record.kind)
            expected = record.receipt
            previous = self.snapshot
            try:
                observed = self._call(_ActorCommand("evidence", deadline, record))
                try:
                    checked = self._checked_observation(observed)
                    if (
                        checked
                        != SqlClientDepartureEvidenceObservation(self._helper_id, self._attempt_sha256, expected)
                        or self._checked_observation(self.snapshot) != checked
                    ):
                        raise ValueError(ERROR)
                except _FAILURES:
                    raise TdsJournalActorUnknown(self) from None
            except BaseException:
                self._snapshot = previous
                self._shutdown()
                raise
            return expected
        finally:
            self._writing = False

    def _dispatch(
        self, backend: CreateOnlyEvidenceWriterV1, command: _ActorCommand[SqlClientDepartureEvidenceObservation]
    ) -> SqlClientDepartureEvidenceObservation:
        if type(command.kind) is not str or command.kind != "evidence":
            raise TdsJournalActorUnknown(self)
        record = self._validate(command.event)
        if record.kind in self._persisted:
            raise TdsJournalActorUnknown(self)
        receipt = record.receipt
        self._persisted.add(record.kind)
        backend.write(receipt.relative_name, record.payload)
        return SqlClientDepartureEvidenceObservation(self._helper_id, self._attempt_sha256, receipt)


def restricted_writer_departure_record(helper_id, attempt_sha256, kind, payload, context):
    """Build a P9b record without coupling its app producer to contracts."""
    return SqlClientDepartureEvidenceRecord(
        helper_id,
        attempt_sha256,
        kind,
        payload,
        restricted_writer_context=context,
    )
