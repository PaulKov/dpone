"""Operation-owned CREATE/helper capabilities and bounded containment-only teardown.

Failure retains original observations without replaying CREATE journal actions or
manufacturing helper evidence. A retained close never renews the cleanup budget.
"""

from __future__ import annotations

import math
import os
from collections.abc import Callable
from dataclasses import replace
from threading import current_thread
from typing import cast
from uuid import UUID

from dpone.adapters.mssql_sqlclient_departure_process import SqlClientDepartureProcess
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.app.mssql_sqlclient_create_evidence_composition import seal_sqlclient_create
from dpone.app.mssql_sqlclient_departure_custody import DepartureHelperCustody
from dpone.app.mssql_sqlclient_departure_lifecycle import DepartureCreateValidationMixin
from dpone.app.mssql_sqlclient_departure_lifecycle import (
    SqlClientCreateDepartureOutcome as SqlClientCreateDepartureOutcome,
)
from dpone.app.mssql_sqlclient_departure_lifecycle import (
    SqlClientDepartureOutcome as SqlClientDepartureOutcome,
)
from dpone.app.mssql_sqlclient_departure_lifecycle import (
    strict_exit as strict_exit,
)
from dpone.app.mssql_sqlclient_stage_locator_composition import _AdmittedSqlClientStoreFactory
from dpone.app.mssql_tds_coordinator_supervision import TdsCoordinatorRetention, before
from dpone.app.mssql_tds_coordinator_supervisor import TdsCoordinatorCreateOutcome
from dpone.contracts.mssql_sqlclient_departure_models import (
    SqlClientDepartureEvidenceKind as Kind,
)
from dpone.contracts.mssql_sqlclient_departure_models import (
    SqlClientDepartureEvidenceObservation,
    SqlClientDepartureEvidenceReceipt,
    SqlClientDepartureEvidenceRecord,
    SqlClientDeparturePlan,
    SqlClientDeparturePlanV2,
    SqlClientDepartureRequest,
    SqlClientDepartureRequestV2,
    SqlClientDepartureResult,
    SqlClientDepartureResultV2,
    SqlClientObserverAdmission,
    SqlClientStageLocator,
    TdsAttemptOwnership,
    TdsChildExit,
    TdsCoordinatorIdentity,
    TdsCoordinatorStartup,
    TdsCreateRequest,
    TdsProcessIdentity,
    WindowLease,
    WindowOutcomeUnknown,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.ports.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceGateway
from dpone.ports.mssql_tds_coordinator import TdsCoordinatorGateway
from dpone.ports.mssql_tds_coordinator_evidence import TdsCoordinatorEvidenceGateway
from dpone.ports.mssql_tds_worker import TdsUnresolvedLaunch
from dpone.services.mssql_tds_attempt import TdsAttempt, TdsAttemptUnknown, _ShutdownCapability

SqlClientDepartureOutcome.__module__ = __name__
SqlClientCreateDepartureOutcome.__module__ = __name__
strict_exit.__module__ = __name__


class _DepartureRetention(DepartureCreateValidationMixin):
    """One local owner; no credential body, supplier, exception or traceback fields."""

    def __init__(
        self,
        attempt: TdsAttempt,
        pool: TdsActorPool,
        helper_id: UUID,
        original_request: TdsCreateRequest,
        create_identity: TdsCoordinatorIdentity,
        clock: Callable[[], float],
        observer_admission: SqlClientObserverAdmission | None = None,
        attempt_shutdown: TdsAttemptUnknown | None = None,
        locator_gateway: _ShutdownCapability | None = None,
        locator_gateway_closed: bool = False,
        seal_gateway: _ShutdownCapability | None = None,
        seal_gateway_closed: bool = False,
        create_writer: TdsCoordinatorGateway | None = None,
        create_evidence: TdsCoordinatorEvidenceGateway | None = None,
        create_retained: TdsCoordinatorRetention | None = None,
        create_writer_closed: bool = False,
        create_evidence_closed: bool = False,
        create_outcome: TdsCoordinatorCreateOutcome | None = None,
        owner: TdsAttemptOwnership | None = None,
        create_admission: bytes = b"",
        create_source: str = "",
        create_root: str = "",
        helper_evidence: SqlClientDepartureEvidenceGateway | None = None,
        helper_evidence_closed: bool = False,
        initial_observation: SqlClientDepartureEvidenceObservation | None = None,
        plan: SqlClientDeparturePlan | SqlClientDeparturePlanV2 | None = None,
        request: SqlClientDepartureRequest | SqlClientDepartureRequestV2 | None = None,
        child: SqlClientDepartureProcess | None = None,
        unresolved_launch: TdsUnresolvedLaunch | None = None,
        process: TdsProcessIdentity | None = None,
        startup: TdsCoordinatorStartup | None = None,
        declared_startup: TdsCoordinatorStartup | None = None,
        raw_result: bytes | None = None,
        result: SqlClientDepartureResult | SqlClientDepartureResultV2 | None = None,
        local_exit: TdsChildExit | None = None,
        receipts: dict[Kind, SqlClientDepartureEvidenceReceipt] | None = None,
        expected: dict[Kind, SqlClientDepartureEvidenceReceipt] | None = None,
        faulted: bool = False,
        evidence_poisoned: bool = False,
        child_close_attempted: bool = False,
        child_closed: bool = False,
        unresolved_close_attempted: bool = False,
        unresolved_closed: bool = False,
        containment_deadline: float | None = None,
        containment_budget_captured: bool = False,
        _pid: int | None = None,
        _thread: object | None = None,
        _asserting: bool = False,
        _closing: bool = False,
    ) -> None:
        """Preserve constructor inputs, transferring resources once to their owner."""
        self.attempt, self.pool, self.helper_id = attempt, pool, helper_id
        self.original_request, self.create_identity, self.clock = original_request, create_identity, clock
        self.observer_admission, self.attempt_shutdown = observer_admission, attempt_shutdown
        self.locator_gateway, self.locator_gateway_closed = locator_gateway, locator_gateway_closed
        self.seal_gateway, self.seal_gateway_closed = seal_gateway, seal_gateway_closed
        self.create_writer, self.create_evidence, self.create_retained = create_writer, create_evidence, create_retained
        self.create_writer_closed, self.create_evidence_closed = create_writer_closed, create_evidence_closed
        self.create_outcome, self.owner = create_outcome, owner
        self.create_admission, self.create_source, self.create_root = create_admission, create_source, create_root
        self.initial_observation, self.plan, self.request, self.result = initial_observation, plan, request, result
        self.receipts, self.expected = {} if receipts is None else receipts, {} if expected is None else expected
        self.faulted, self.evidence_poisoned = faulted, evidence_poisoned
        self._pid, self._thread = (
            os.getpid() if _pid is None else _pid,
            current_thread() if _thread is None else _thread,
        )
        self._asserting, self._closing, self._cleanup_reentered = _asserting, _closing, False
        self._helper = DepartureHelperCustody(clock, self._pid, self._thread)
        self.helper.initialize_retained(
            helper_evidence=helper_evidence,
            child=child,
            unresolved_launch=unresolved_launch,
            process=process,
            declared_startup=declared_startup,
            startup=startup,
            local_exit=local_exit,
            raw_result=raw_result,
            containment_budget_captured=containment_budget_captured,
            containment_deadline=containment_deadline,
            helper_evidence_closed=helper_evidence_closed,
            child_close_attempted=child_close_attempted,
            child_closed=child_closed,
            unresolved_close_attempted=unresolved_close_attempted,
            unresolved_closed=unresolved_closed,
        )

    @property
    def helper(self) -> DepartureHelperCustody:
        return self._helper

    def guard(self, deadline: float) -> None:
        """Latch caught nested entry, owner drift, deadline or journal uncertainty."""
        if os.getpid() != self._pid or current_thread() is not self._thread or self._asserting or self.faulted:
            self.faulted = True
            raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_owner_unknown")
        self._asserting = True
        try:
            before(deadline, self.clock)
            self.attempt._assert_create_departure(self.helper_id, deadline=deadline)
            before(deadline, self.clock)
            if self.faulted:
                raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_faulted")
        except BaseException:
            self.faulted = True
            raise
        finally:
            self._asserting = False

    def seal_create(
        self,
        factory: _AdmittedSqlClientStoreFactory,
        locator: SqlClientStageLocator,
        lease: WindowLease,
        deadline: float,
    ) -> None:
        """Retain the actual failed seal gateway; never proceed to departure."""
        assert self.create_outcome is not None
        try:
            seal_sqlclient_create(
                admitted_factory=factory,
                locator=locator,
                created=self.create_outcome,
                lease=lease,
                pool=self.pool,
                deadline=deadline,
            )
        except TdsJournalActorUnknown as error:
            self.seal_gateway = cast(_ShutdownCapability | None, error.gateway)
            raise
        self.guard(deadline)

    def persist(self, kind: Kind, payload: bytes) -> SqlClientDepartureEvidenceReceipt:
        """Consume an evidence kind once, requiring exact receipt and actor ACK."""
        assert self.plan is not None and self.helper_evidence is not None
        if self.evidence_poisoned or kind in self.expected:
            raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_evidence_poisoned")
        record = SqlClientDepartureEvidenceRecord(
            self.helper_id,
            attempt_identity_digest(self.plan.attempt),
            kind,
            payload,
            self.request if kind is Kind.RESULT else None,
        )
        expected = self.expected[kind] = record.receipt
        try:
            self.guard(self.plan.operation_deadline)
            received = self.helper_evidence.write(record, deadline=self.plan.operation_deadline)
            if type(received) is not SqlClientDepartureEvidenceReceipt:
                raise ValueError("mssql_native.sqlclient_departure_ack_invalid")
            replace(received)
            observation = self.helper_evidence.observation
            if type(observation) is not SqlClientDepartureEvidenceObservation:
                raise ValueError("mssql_native.sqlclient_departure_ack_invalid")
            replace(observation)
            if received != expected or observation != SqlClientDepartureEvidenceObservation(
                self.helper_id, expected.attempt_sha256, expected
            ):
                raise ValueError("mssql_native.sqlclient_departure_ack_invalid")
            self.receipts[kind] = received
            self.guard(self.plan.operation_deadline)
            return received
        except BaseException:
            self.evidence_poisoned = self.faulted = True
            raise

    def capture_result(self) -> None:
        self.helper.capture_result()

    def close_child(self) -> None:
        self.helper.close_child()

    def capture_budget(self, termination_timeout: float) -> None:
        """Reuse CREATE's captured cleanup deadline, including failed clock capture."""
        if not self.helper.begin_cleanup_deadline_capture():
            return
        value = None
        try:
            if self.create_retained is not None:
                value = self.create_retained.capture_cleanup_budget(termination_timeout, self.clock)
            elif type(termination_timeout) is float and math.isfinite(termination_timeout) and termination_timeout > 0:
                candidate = self.clock() + termination_timeout
                if math.isfinite(candidate):
                    value = candidate
        finally:
            self.helper.finish_cleanup_deadline_capture(value)

    def close(self, *, deadline: float) -> None:
        """Contain before actor waits; attempt all gateways, never the shared pool."""
        if os.getpid() != self._pid or current_thread() is not self._thread or self._closing:
            self._cleanup_reentered |= self._closing
            self.faulted = True
            raise SqlClientCreateDepartureUnknown(self)
        self.faulted = True
        if type(deadline) is not float or not math.isfinite(deadline) or self.containment_deadline is None:
            raise SqlClientCreateDepartureUnknown(self)
        deadline = min(deadline, self.containment_deadline)
        self._closing = True
        failed = False

        def attempt(action: Callable[[], None]) -> None:
            nonlocal failed
            try:
                action()
            except BaseException:
                failed = True

        def close_gateway(name: str, flag: str) -> None:
            gateway = getattr(self, name)
            if gateway is not None and not getattr(self, flag):
                gateway.close(deadline=deadline)
                setattr(self, flag, True)

        try:
            attempt(self.capture_result)
            attempt(lambda: self.helper.contain(deadline))
            retained_create = self.create_retained
            if retained_create is not None:
                attempt(lambda: retained_create.cleanup_local(deadline, self.clock))
                attempt(lambda: retained_create.close_evidence(deadline))
                attempt(lambda: retained_create.close_writer(deadline))
            else:
                attempt(lambda: close_gateway("create_evidence", "create_evidence_closed"))
                attempt(lambda: close_gateway("create_writer", "create_writer_closed"))
            attempt(lambda: close_gateway("locator_gateway", "locator_gateway_closed"))
            attempt(lambda: close_gateway("seal_gateway", "seal_gateway_closed"))
            attempt(lambda: self.helper.close_evidence(deadline))
            attempt(lambda: (self.attempt_shutdown or self.attempt).close(deadline=deadline))
            attempt(lambda: before(deadline, self.clock))
        finally:
            self._closing = False
        if failed or self._cleanup_reentered:
            raise SqlClientCreateDepartureUnknown(self)

    @property
    def helper_evidence(self) -> SqlClientDepartureEvidenceGateway | None:
        return self.helper.helper_evidence

    @property
    def helper_evidence_closed(self) -> bool:
        return self.helper.helper_evidence_closed

    @property
    def child(self) -> SqlClientDepartureProcess | None:
        return self.helper.child

    @property
    def unresolved_launch(self) -> TdsUnresolvedLaunch | None:
        return self.helper.unresolved_launch

    @property
    def process(self) -> TdsProcessIdentity | None:
        return self.helper.process

    @property
    def startup(self) -> TdsCoordinatorStartup | None:
        return self.helper.startup

    @property
    def declared_startup(self) -> TdsCoordinatorStartup | None:
        return self.helper.declared_startup

    @property
    def raw_result(self) -> bytes | None:
        return self.helper.raw_result

    @property
    def local_exit(self) -> TdsChildExit | None:
        return self.helper.local_exit

    @property
    def child_close_attempted(self) -> bool:
        return self.helper.child_close_attempted

    @property
    def child_closed(self) -> bool:
        return self.helper.child_closed

    @property
    def unresolved_close_attempted(self) -> bool:
        return self.helper.unresolved_close_attempted

    @property
    def unresolved_closed(self) -> bool:
        return self.helper.unresolved_closed

    @property
    def containment_deadline(self) -> float | None:
        return self.helper.containment_deadline

    @property
    def containment_budget_captured(self) -> bool:
        return self.helper.containment_budget_captured


class SqlClientCreateDepartureUnknown(WindowOutcomeUnknown):
    """Uncertain component execution with bounded teardown, never retry permission."""

    def __init__(self, retained: _DepartureRetention) -> None:
        self.retained = retained
        super().__init__("mssql_native.sqlclient_create_departure_unknown")

    def close(self, *, deadline: float) -> None:
        self.retained.close(deadline=deadline)
