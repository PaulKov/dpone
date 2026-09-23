"""Actual helper custody for the original PREPARED continuation.

The settlement service owns journal decisions and evidence acknowledgements.
This application owner binds those decisions to the concrete helper producers;
cleanup keeps the same capabilities and consumes one additional local budget.
"""

from __future__ import annotations

import math
import os
from collections.abc import Callable
from threading import current_thread

from dpone.app.mssql_sqlclient_departure_custody import DepartureHelperCustody
from dpone.app.mssql_sqlclient_observe_departure_projection import (
    ObserveDepartureSettlement,
    ObserveDepartureSnapshot,
    observe_field,
)
from dpone.app.mssql_tds_coordinator_supervision import before
from dpone.contracts.mssql_sqlclient_departure_models import (
    SqlClientDepartureEvidenceKind as Kind,
)
from dpone.contracts.mssql_sqlclient_departure_models import (
    SqlClientDepartureEvidenceReceipt,
    SqlClientObserveDepartureRequest,
    SqlClientObserveDepartureResult,
    SqlClientObserverAdmission,
    TdsChildExit,
    WindowOutcomeUnknown,
)
from dpone.ports.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceGateway
from dpone.services.mssql_tds_observe_settlement import ObserveSettlement

ERROR = "mssql_native.sqlclient_observe_departure_unknown"


class _ObserveDepartureRetention:
    """One concrete helper and original service owner; never a second ACK ledger."""

    owner = observe_field(lambda value: value.owner)
    helper = observe_field(lambda value: value.helper)
    pool = observe_field(lambda value: value.pool)
    helper_id = observe_field(lambda value: value.helper_id)
    plan = observe_field(lambda value: value.plan)
    request = observe_field(lambda value: value.request)
    result = observe_field(lambda value: value.result)
    receipts = observe_field(lambda value: dict(value.receipts))
    expected = observe_field(lambda value: dict(value.expected))
    child = observe_field(lambda value: value.child)
    process = observe_field(lambda value: value.process)
    startup = observe_field(lambda value: value.startup)
    raw_result = observe_field(lambda value: value.raw_result)
    local_exit = observe_field(lambda value: value.local_exit)
    containment_deadline = observe_field(lambda value: value.containment_deadline)
    helper_evidence = observe_field(lambda value: value.helper_evidence)

    def __init__(
        self,
        owner: ObserveSettlement,
        helper: DepartureHelperCustody,
        observer_admission: SqlClientObserverAdmission,
        clock: Callable[[], float],
    ) -> None:
        if type(owner) is not ObserveSettlement or type(helper) is not DepartureHelperCustody:
            raise WindowOutcomeUnknown(ERROR)
        self._owner, self._helper, self.observer_admission, self.clock = owner, helper, observer_admission, clock
        self._bindings = owner, helper, owner.origin, owner.attempt, owner.origin.pool, clock, observer_admission
        self._pid, self._thread = os.getpid(), current_thread()
        self._asserting = self._closing = self._reentered = self.faulted = False
        self._settlement = ObserveDepartureSettlement(observer_admission)
        owner.bind_helper_custody(helper)

    def snapshot(self) -> ObserveDepartureSnapshot:
        return ObserveDepartureSnapshot.capture(self._bindings[0], self._bindings[1])

    def _owned(self) -> None:
        if os.getpid() != self._pid or current_thread() is not self._thread:
            self.faulted = True
            raise WindowOutcomeUnknown(ERROR)

    def _custody_current(self) -> None:
        if (
            self._owner is not self._bindings[0]
            or self._helper is not self._bindings[1]
            or self.owner.origin is not self._bindings[2]
            or self.owner.attempt is not self._bindings[3]
            or self.pool is not self._bindings[4]
            or self.clock is not self._bindings[5]
            or self.observer_admission is not self._bindings[6]
        ):
            raise WindowOutcomeUnknown(ERROR)

    def guard(self, deadline: float) -> None:
        """Check phase-available producers around all callbacks; caught reentry sticks."""
        self._owned()
        if self._asserting or self.faulted:
            self.faulted = True
            raise WindowOutcomeUnknown(ERROR)
        self._asserting = True
        try:
            self._custody_current()
            before(deadline, self.clock)
            self._custody_current()
            self.owner.assert_current(deadline=deadline)
            self._custody_current()
            if self.faulted:
                raise WindowOutcomeUnknown(ERROR)
        except BaseException:
            self.faulted = True
            raise
        finally:
            self._asserting = False

    def bind_helper_evidence(self, gateway: SqlClientDepartureEvidenceGateway | None) -> None:
        """Retain the returned actor before any metadata or owner validation."""
        self._owned()
        self.helper.retain_evidence(gateway)
        self.owner.bind_helper_evidence(gateway)

    def capture_helper_child(self) -> None:
        self._owned()
        self.owner.capture_helper_child()

    def capture_helper_process(self) -> None:
        self._owned()
        self.owner.capture_helper_process()

    def capture_helper_startup(self) -> None:
        self._owned()
        self.owner.capture_helper_startup()

    def capture_helper_request(self, request: SqlClientObserveDepartureRequest) -> None:
        self._owned()
        self.owner.capture_helper_request(request)

    def capture_result(self) -> None:
        """Retain complete natural EOF before receive/decode/post-effect failure."""
        self._owned()
        self.helper.capture_result()
        self.owner.capture_helper_raw_result()

    def capture_helper_result(self, result: SqlClientObserveDepartureResult) -> None:
        self._owned()
        self.owner.capture_helper_result(result)

    def capture_helper_exit(self, exit: TdsChildExit) -> None:
        self._owned()
        self.owner.capture_helper_exit(exit)

    def validate_observe(self) -> None:
        self._custody_current()
        self._settlement.validate(self.snapshot())

    def persist(self, kind: Kind, payload: bytes) -> SqlClientDepartureEvidenceReceipt:
        return self._settlement.persist(self.snapshot(), kind, payload, self.guard)

    def capture_budget(self, termination_timeout: float) -> None:
        """Consume before clock invocation; never renew the original completed cleanup."""
        if not self.helper.begin_cleanup_deadline_capture():
            return
        value = None
        try:
            if type(termination_timeout) is float and math.isfinite(termination_timeout) and termination_timeout > 0:
                candidate = self._bindings[5]() + termination_timeout
                if math.isfinite(candidate):
                    value = candidate
        finally:
            self.helper.finish_cleanup_deadline_capture(value)

    def close(self, *, deadline: float) -> None:
        """Contain helper, then bounded actor waits, preserving the original attempt."""
        self._owned()
        if self._closing:
            self._reentered = self.faulted = True
            raise SqlClientObserveDepartureUnknown(self)
        self.faulted = True
        if type(deadline) is not float or not math.isfinite(deadline) or self.containment_deadline is None:
            raise SqlClientObserveDepartureUnknown(self)
        deadline = min(deadline, self.containment_deadline)
        self._closing = True
        failed = False
        try:
            for action in (
                self.helper.capture_result,
                lambda: self.helper.contain(deadline),
                lambda: self._bindings[0].close_containment(deadline=deadline),
                lambda: self.helper.close_evidence(deadline),
            ):
                try:
                    action()
                except BaseException:
                    failed = True
        finally:
            self._closing = False
        if failed or self._reentered:
            raise SqlClientObserveDepartureUnknown(self)


class SqlClientObserveDepartureUnknown(WindowOutcomeUnknown):
    """Retained cleanup only; matching later observations cannot retry settlement."""

    def __init__(self, retained: _ObserveDepartureRetention) -> None:
        self.retained = retained
        super().__init__(ERROR)

    def close(self, *, deadline: float) -> None:
        self.retained.close(deadline=deadline)
