"""Original OBSERVE containment persistence, separate from six helper phases.

The owner supplies actual original operation/process facts and authorizes the
submission. Local file persistence establishes neither provenance nor remote
SQL settlement. An uncertain write consumes the sole attempt permanently.
"""

from collections.abc import Callable
from contextlib import AbstractContextManager
from copy import deepcopy

from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.contracts.mssql_sqlclient_observe_departure import (
    ERROR,
    FAILURES,
    SqlClientObserveContainmentObservation,
    SqlClientObserveContainmentReceipt,
    _operation,
)
from dpone.contracts.mssql_tds_api import (
    TdsProcessIdentity,
    _typed,
    decode_observe_containment,
    observe_containment_receipt,
)
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorIdentity, coordinator_identity_digest
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1


class SqlClientObserveContainmentEvidenceActor(
    _ActorCore[CreateOnlyEvidenceWriterV1, SqlClientObserveContainmentObservation]
):
    """Allocation-only gateway admitted through the original run's TdsActorPool.

    Backend construction, writes and teardown run only on its actor thread.
    Actual nonzero reaped exits are valid; inherited close confirms local
    teardown only and keeps a stalled actor in its original pool.
    """

    def __init__(
        self,
        factory: Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]],
        operation: TdsCoordinatorIdentity,
        process: TdsProcessIdentity,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        _operation(operation)
        _typed(process, TdsProcessIdentity)
        self._operation, self._process = operation, process
        self._operation_snapshot, self._process_snapshot = deepcopy(operation), deepcopy(process)
        self._validate_held()
        self._subject = coordinator_identity_digest(operation)
        self._empty = SqlClientObserveContainmentObservation(self._subject, None)
        self._attempted = self._persisted = self._writing = False
        super().__init__(factory, deadline, clock, SqlClientObserveContainmentObservation)

    def _validate_held(self) -> None:
        for operation in (self._operation, self._operation_snapshot):
            _operation(operation)
        for process in (self._process, self._process_snapshot):
            _typed(process, TdsProcessIdentity)
        if self._operation != self._operation_snapshot or self._process != self._process_snapshot:
            raise ValueError(ERROR)

    @property
    def observation(self) -> SqlClientObserveContainmentObservation:
        """Expose only the last checked ACK, never late durable completion."""
        return self.snapshot

    def _checked_observation(self, value: object) -> SqlClientObserveContainmentObservation:
        if type(value) is not SqlClientObserveContainmentObservation:
            raise ValueError(ERROR)
        return SqlClientObserveContainmentObservation(value.operation_sha256, value.receipt)

    def _await_ready(self) -> None:
        super()._await_ready()
        try:
            if self._checked_observation(self.snapshot) != self._empty:
                raise ValueError(ERROR)
        except FAILURES:
            self._snapshot = None
            self._shutdown()
            raise TdsJournalActorUnknown(self) from None

    def _initial(self, backend: CreateOnlyEvidenceWriterV1) -> SqlClientObserveContainmentObservation:
        self._validate_held()
        return self._empty

    def _validate(self, payload: object) -> SqlClientObserveContainmentReceipt:
        try:
            self._validate_held()
            if type(payload) is not bytes:
                raise ValueError(ERROR)
            value = decode_observe_containment(payload, process=self._process)
            if value.observe_operation != self._operation:
                raise ValueError(ERROR)
            receipt = observe_containment_receipt(payload, process=self._process)
            if receipt.operation_sha256 != self._subject:
                raise ValueError(ERROR)
            return receipt
        except FAILURES:
            raise ValueError(ERROR) from None

    def write(self, payload: bytes, *, deadline: float) -> SqlClientObserveContainmentReceipt:
        """Consume one attempt before enqueue and retain the prior ACK on failure."""
        self._owned()
        if self._writing:
            self._shutdown()
            raise TdsJournalActorUnknown(self)
        self._writing = True
        try:
            expected = self._validate(payload)
            if self._attempted:
                raise ValueError("mssql_native.sqlclient_observe_containment_consumed")
            self._attempted = True
            previous = self.snapshot
            try:
                observed = self._call(_ActorCommand("containment", deadline, payload))
                try:
                    checked = self._checked_observation(observed)
                    if (
                        checked != SqlClientObserveContainmentObservation(self._subject, expected)
                        or self._checked_observation(self.snapshot) != checked
                    ):
                        raise ValueError(ERROR)
                except FAILURES:
                    raise TdsJournalActorUnknown(self) from None
            except BaseException:
                self._snapshot = previous
                self._shutdown()
                raise
            return expected
        finally:
            self._writing = False

    def _dispatch(
        self, backend: CreateOnlyEvidenceWriterV1, command: _ActorCommand[SqlClientObserveContainmentObservation]
    ) -> SqlClientObserveContainmentObservation:
        if type(command.kind) is not str or command.kind != "containment" or self._persisted:
            raise TdsJournalActorUnknown(self)
        payload = command.event
        receipt = self._validate(payload)
        if type(payload) is not bytes:
            raise ValueError(ERROR)
        self._persisted = True
        backend.write(receipt.relative_name, payload)
        return SqlClientObserveContainmentObservation(self._subject, receipt)
