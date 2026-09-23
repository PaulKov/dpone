"""Private helper transport custody, without CREATE or OBSERVE authority.

The process wrapper remains the sole OS resource owner. This object retains that
wrapper before validation, and distinguishes ambiguous raw close from an actor's
repeatable bounded shutdown wait. It never decodes a result or grants settlement.
"""

from __future__ import annotations

import math
import os
from collections.abc import Callable
from dataclasses import replace
from threading import current_thread

from dpone.adapters.mssql_sqlclient_departure_process import SqlClientDepartureProcess
from dpone.app.mssql_sqlclient_departure_observation import DepartureCustodySnapshot, snapshot_field
from dpone.contracts.mssql_sqlclient_departure_models import (
    TdsChildExit,
    TdsCoordinatorStartup,
    TdsProcessIdentity,
    WindowOutcomeUnknown,
)
from dpone.contracts.mssql_tds_api import _startup
from dpone.contracts.mssql_tds_coordinator_ipc import decode_startup, encode_startup
from dpone.ports.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceGateway
from dpone.ports.mssql_tds_worker import TdsUnresolvedLaunch


def strict_exit(value: TdsChildExit, process: TdsProcessIdentity | None) -> TdsChildExit:
    """Validate actual reaped identity independently of the success exit-code gate."""
    if type(value) is not TdsChildExit or type(value.identity) is not TdsProcessIdentity:
        raise ValueError("mssql_native.sqlclient_departure_exit_invalid")
    replace(value.identity)
    replace(value)
    if value.identity != process or value.reaped is not True:
        raise ValueError("mssql_native.sqlclient_departure_exit_invalid")
    return value


def _restore_retained(custody: DepartureHelperCustody, values: tuple[object, ...]) -> None:
    """Restore legacy constructor inputs through the normal custody transitions."""
    (
        helper_evidence,
        child,
        unresolved_launch,
        process,
        declared_startup,
        startup,
        local_exit,
        raw_result,
        containment_budget_captured,
        containment_deadline,
        helper_evidence_closed,
        child_close_attempted,
        child_closed,
        unresolved_close_attempted,
        unresolved_closed,
    ) = values
    if helper_evidence is not None:
        custody.retain_evidence(helper_evidence)  # type: ignore[arg-type]
    if child is not None:
        custody.retain_child(child)  # type: ignore[arg-type]
    if unresolved_launch is not None:
        custody.retain_unresolved(unresolved_launch)  # type: ignore[arg-type]
    if process is not None:
        custody.record_process(process)  # type: ignore[arg-type]
    if declared_startup is not None:
        custody.record_declared_startup(declared_startup)  # type: ignore[arg-type]
    if startup is not None:
        custody.record_startup(startup)  # type: ignore[arg-type]
    if local_exit is not None:
        custody.record_exit(local_exit)  # type: ignore[arg-type]
    if raw_result is not None:
        custody.capture_result()
        if raw_result != custody.raw_result:
            raise ValueError("mssql_native.sqlclient_departure_result_transport_binding")
    if containment_budget_captured or containment_deadline is not None:
        custody.bind_cleanup_deadline_once(containment_deadline)  # type: ignore[arg-type]
    if any(
        (helper_evidence_closed, child_close_attempted, child_closed, unresolved_close_attempted, unresolved_closed)
    ):
        raise ValueError("mssql_native.sqlclient_departure_close_state_invalid")


class DepartureHelperCustody:
    """One captured helper continuation; no authority, credential or callback registry."""

    def __init__(self, clock: Callable[[], float], pid: int, thread: object) -> None:
        self._clock, self._pid, self._thread = clock, pid, thread
        self._child: SqlClientDepartureProcess | None = None
        self._unresolved_launch: TdsUnresolvedLaunch | None = None
        self._helper_evidence: SqlClientDepartureEvidenceGateway | None = None
        self._process: TdsProcessIdentity | None = None
        self._declared_startup: TdsCoordinatorStartup | None = None
        self._startup: TdsCoordinatorStartup | None = None
        self._local_exit: TdsChildExit | None = None
        self._raw_result: bytes | None = None
        self._child_close_attempted = self._child_closed = False
        self._unresolved_close_attempted = self._unresolved_closed = False
        self._helper_evidence_closed = self._containment_budget_captured = False
        self._containment_deadline: float | None = None
        self._budget_capture_pending = False
        self._active = self._reentered = self._evidence_retained = False
        self._evidence_closing = self._evidence_reentered = False

    def initialize_retained(
        self,
        *,
        helper_evidence: SqlClientDepartureEvidenceGateway | None,
        child: SqlClientDepartureProcess | None,
        unresolved_launch: TdsUnresolvedLaunch | None,
        process: TdsProcessIdentity | None,
        declared_startup: TdsCoordinatorStartup | None,
        startup: TdsCoordinatorStartup | None,
        local_exit: TdsChildExit | None,
        raw_result: bytes | None,
        containment_budget_captured: bool,
        containment_deadline: float | None,
        helper_evidence_closed: bool,
        child_close_attempted: bool,
        child_closed: bool,
        unresolved_close_attempted: bool,
        unresolved_closed: bool,
    ) -> None:
        """Transfer legacy constructor inputs once; flags cannot invent completed effects."""
        _restore_retained(
            self,
            (
                helper_evidence,
                child,
                unresolved_launch,
                process,
                declared_startup,
                startup,
                local_exit,
                raw_result,
                containment_budget_captured,
                containment_deadline,
                helper_evidence_closed,
                child_close_attempted,
                child_closed,
                unresolved_close_attempted,
                unresolved_closed,
            ),
        )

    def _owner(self) -> None:
        if os.getpid() != self._pid or current_thread() is not self._thread:
            raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_owner_unknown")

    def _before(self, deadline: float) -> None:
        now = self._clock()
        if not math.isfinite(now) or now >= deadline:
            raise TimeoutError("mssql_native.tds_coordinator_deadline")
        if self._reentered:
            raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_close_unknown")

    def retain_child(self, child: SqlClientDepartureProcess) -> None:
        """Take the returned capability before touching any child metadata."""
        self._owner()
        if self._child is not None or self._unresolved_launch is not None:
            raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_resource_replaced")
        self._child = child

    def retain_unresolved(self, launch: TdsUnresolvedLaunch) -> None:
        """Keep the exact allocation-UNKNOWN capability, without reconstructing it."""
        self._owner()
        if self._child is not None or self._unresolved_launch is not None:
            raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_resource_replaced")
        self._unresolved_launch = launch

    def retain_evidence(self, gateway: SqlClientDepartureEvidenceGateway | None) -> None:
        self._owner()
        if self._evidence_retained:
            raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_resource_replaced")
        self._evidence_retained = True
        self._helper_evidence = gateway

    def record_process(self, process: TdsProcessIdentity) -> None:
        self._owner()
        if type(process) is not TdsProcessIdentity:
            raise ValueError("mssql_native.sqlclient_departure_process_invalid")
        observed = replace(process)
        if self._child is None or self._child.identity != observed or self._process is not None:
            raise ValueError("mssql_native.sqlclient_departure_process_invalid")
        self._process = observed

    def record_declared_startup(self, declared: TdsCoordinatorStartup) -> None:
        self._owner()
        _startup(declared)
        if self._child is None or declared != self._child.declared_startup or declared.process != self._process:
            raise ValueError("mssql_native.sqlclient_departure_startup_binding")
        if self._declared_startup is not None:
            raise ValueError("mssql_native.sqlclient_departure_startup_binding")
        self._declared_startup = replace(declared, process=replace(declared.process))

    def record_startup(self, startup: TdsCoordinatorStartup) -> None:
        self._owner()
        _startup(startup)
        if self._child is None:
            raise ValueError("mssql_native.sqlclient_departure_startup_binding")
        _startup(self._child.declared_startup)
        receipt = self._child.startup_receipt
        if receipt is None:
            raise ValueError("mssql_native.sqlclient_departure_startup_receipt_missing")
        _startup(receipt)
        detached = decode_startup(encode_startup(startup))
        if (
            self._startup is not None
            or detached != self._declared_startup
            or detached != self._child.declared_startup
            or detached != receipt
            or detached.process != self._process
        ):
            raise ValueError("mssql_native.sqlclient_departure_startup_binding")
        self._startup = detached

    def record_exit(self, result: TdsChildExit) -> None:
        self._owner()
        actual = strict_exit(result, self._process)
        if self._local_exit is not None and actual is not self._local_exit:
            raise ValueError("mssql_native.sqlclient_departure_exit_invalid")
        self._local_exit = actual

    def capture_result(self) -> None:
        """Retain only bounded transport-complete EOF bytes, without success promotion."""
        self._owner()
        if self._child is not None and self._raw_result is None:
            raw = self._child.received_result
            if type(raw) is bytes and 0 < len(raw) <= 32768:
                self._raw_result = raw

    def close_child(self) -> None:
        self._owner()
        if self._child is None or self._child_closed:
            return
        if self._child_close_attempted:
            self._reentered = True
            raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_close_unknown")
        self._child_close_attempted = True
        self._child.close()
        if self._reentered:
            raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_close_unknown")
        self._child_closed = True

    def bind_cleanup_deadline_once(self, value: float | None) -> None:
        """A failed original budget capture consumes the binding just like success."""
        if self.begin_cleanup_deadline_capture():
            self.finish_cleanup_deadline_capture(value)

    def begin_cleanup_deadline_capture(self) -> bool:
        """Consume the capture before an external clock or budget producer runs."""
        self._owner()
        if self._containment_budget_captured:
            return False
        self._containment_budget_captured = True
        self._budget_capture_pending = True
        return True

    def finish_cleanup_deadline_capture(self, value: float | None) -> None:
        """Finish the original capture; failure cannot open a second capture."""
        self._owner()
        if not self._budget_capture_pending:
            raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_close_unknown")
        self._budget_capture_pending = False
        if value is not None:
            if type(value) is not float or not math.isfinite(value):
                raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_close_unknown")
            self._containment_deadline = value

    def contain(self, deadline: float) -> None:
        """Contain then close the original raw resource, within the captured budget."""
        self._owner()
        if self._active:
            self._reentered = True
            raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_close_unknown")
        if type(deadline) is not float or not math.isfinite(deadline) or self._containment_deadline is None:
            raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_close_unknown")
        deadline = min(deadline, self._containment_deadline)
        self._active = True
        try:
            self._before(deadline)
            if self._child is not None:
                if self._local_exit is None:
                    self.record_exit(self._child.terminate(deadline=deadline))
                self._before(deadline)
                self.close_child()
            elif self._unresolved_launch is not None and not self._unresolved_closed:
                self._unresolved_launch.contain(deadline=deadline)
                self._before(deadline)
                if self._unresolved_close_attempted:
                    raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_close_unknown")
                self._unresolved_close_attempted = True
                self._unresolved_launch.close()
                if self._reentered:
                    raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_close_unknown")
                self._unresolved_closed = True
        finally:
            self._active = False

    def close_evidence(self, deadline: float) -> None:
        """A repeated actor wait is safe; only its acknowledged completion is latched."""
        self._owner()
        if self._helper_evidence is not None and not self._helper_evidence_closed:
            if self._containment_budget_captured:
                if self._containment_deadline is None:
                    raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_close_unknown")
                deadline = min(deadline, self._containment_deadline)
            if self._evidence_closing:
                self._evidence_reentered = True
                raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_close_unknown")
            self._evidence_closing = True
            try:
                self._helper_evidence.close(deadline=deadline)
                if self._evidence_reentered:
                    raise WindowOutcomeUnknown("mssql_native.sqlclient_departure_close_unknown")
                self._helper_evidence_closed = True
            finally:
                self._evidence_closing = False

    def snapshot(self) -> DepartureCustodySnapshot:
        """Return a stable read-only view without transferring resource authority."""
        return DepartureCustodySnapshot(
            helper_evidence=self._helper_evidence,
            helper_evidence_closed=self._helper_evidence_closed,
            child=self._child,
            unresolved_launch=self._unresolved_launch,
            process=self._process,
            startup=self._startup,
            declared_startup=self._declared_startup,
            raw_result=self._raw_result,
            local_exit=self._local_exit,
            child_close_attempted=self._child_close_attempted,
            child_closed=self._child_closed,
            unresolved_close_attempted=self._unresolved_close_attempted,
            unresolved_closed=self._unresolved_closed,
            containment_deadline=self._containment_deadline,
            containment_budget_captured=self._containment_budget_captured,
        )

    helper_evidence = snapshot_field(lambda value: value.helper_evidence)
    helper_evidence_closed = snapshot_field(lambda value: value.helper_evidence_closed)
    child = snapshot_field(lambda value: value.child)
    unresolved_launch = snapshot_field(lambda value: value.unresolved_launch)
    process = snapshot_field(lambda value: value.process)
    startup = snapshot_field(lambda value: value.startup)
    declared_startup = snapshot_field(lambda value: value.declared_startup)
    raw_result = snapshot_field(lambda value: value.raw_result)
    local_exit = snapshot_field(lambda value: value.local_exit)
    child_close_attempted = snapshot_field(lambda value: value.child_close_attempted)
    child_closed = snapshot_field(lambda value: value.child_closed)
    unresolved_close_attempted = snapshot_field(lambda value: value.unresolved_close_attempted)
    unresolved_closed = snapshot_field(lambda value: value.unresolved_closed)
    containment_deadline = snapshot_field(lambda value: value.containment_deadline)
    containment_budget_captured = snapshot_field(lambda value: value.containment_budget_captured)
