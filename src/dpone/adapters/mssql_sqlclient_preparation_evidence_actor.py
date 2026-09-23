"""One preparation artifact ACK on the original bounded actor mechanism."""

from collections.abc import Callable
from contextlib import AbstractContextManager

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.contracts.mssql_tds_api import (
    PreparationObservation,
    PreparationReceipt,
    TdsAttemptIdentity,
    preparation_bytes,
    strict_json_object,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1


class PreparationEvidenceActor(_ActorCore[CreateOnlyEvidenceWriterV1, PreparationObservation]):
    """Consume before enqueue; no ACK adoption or retry after uncertainty."""

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]],
        attempt_sha256: str,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        self.subject = attempt_sha256
        self._attempted = self._persisted = False
        PreparationObservation(self.subject)
        super().__init__(factory, deadline, clock, PreparationObservation)

    def _initial(self, backend: CreateOnlyEvidenceWriterV1) -> PreparationObservation:
        return PreparationObservation(self.subject)

    def write(self, payload: bytes, *, deadline: float) -> PreparationReceipt:
        self._owned()
        if self._attempted:
            raise ValueError("mssql_native.sqlclient_preparation_consumed")
        value = strict_json_object(payload)
        if attempt_identity_digest(TdsAttemptIdentity(**value["attempt"])) != self.subject:
            raise ValueError("mssql_native.sqlclient_preparation_subject")
        if preparation_bytes(value) != payload:
            raise ValueError("mssql_native.sqlclient_preparation_invalid")
        self._attempted = True
        expected = PreparationReceipt.for_payload(self.subject, payload)
        previous = self.snapshot
        try:
            observed = self._call(_ActorCommand("preparation", deadline, payload))
            if type(observed) is not PreparationObservation:
                raise ValueError
            observed.__post_init__()
            if observed != PreparationObservation(self.subject, expected):
                raise ValueError
        except BaseException:
            self._snapshot = previous
            self._shutdown()
            raise TdsJournalActorUnknown(self) from None
        assert observed.receipt is not None
        return observed.receipt

    def _dispatch(
        self, backend: CreateOnlyEvidenceWriterV1, command: _ActorCommand[PreparationObservation]
    ) -> PreparationObservation:
        if command.kind != "preparation" or self._persisted or type(command.event) is not bytes:
            raise TdsJournalActorUnknown(self)
        payload = preparation_bytes(strict_json_object(command.event))
        receipt = PreparationReceipt.for_payload(self.subject, payload)
        self._persisted = True
        backend.write(receipt.relative_name, payload)
        return PreparationObservation(self.subject, receipt)
