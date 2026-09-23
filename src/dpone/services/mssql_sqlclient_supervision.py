"""Retain worker observations and bound local teardown after uncertain execution.

This owner contains no jobs, credential suppliers or encoded credential bodies.
Closing it stops local resources only: SQL settlement, evidence reconciliation
and permission to retry remain responsibilities of the attempt's recovery owner.
"""

from __future__ import annotations

import math
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import current_thread
from typing import Protocol

from dpone.ports.mssql_sqlclient_process import SqlClientProcess
from dpone.ports.mssql_tds_worker import TdsUnresolvedLaunch
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientEvidenceKind,
    SqlClientEvidenceReceipt,
    SqlClientLaunch,
    SqlClientResult,
    TdsChildExit,
    TdsInputReceipt,
    TdsProcessIdentity,
    WindowContractError,
    WindowOutcomeUnknown,
    evidence_limit,
)


class _BoundedClose(Protocol):
    def close(self, *, deadline: float) -> None: ...


def before(deadline: float, clock: Callable[[], float]) -> None:
    """Check a finite absolute deadline without granting a new time budget."""
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise ValueError("mssql_native.sqlclient_supervision_deadline_invalid")
    now = clock()
    if type(now) not in (int, float) or not math.isfinite(now) or now >= deadline:
        raise TimeoutError("mssql_native.sqlclient_supervision_deadline")


def validate_local_exit(observed: TdsChildExit, expected: TdsProcessIdentity | None) -> None:
    """Require the independently held process incarnation and actual reaping."""
    if (
        type(observed) is not TdsChildExit
        or type(expected) is not TdsProcessIdentity
        or observed.identity != expected
        or observed.reaped is not True
    ):
        raise ValueError("mssql_native.sqlclient_supervision_exit_invalid")
    # Frozen dataclasses can still be forged through object.__setattr__.
    TdsChildExit(observed.identity, observed.exit_code, observed.reaped)
    for identity in (expected, observed.identity):
        TdsProcessIdentity(identity.host_sha256, identity.boot_id, identity.pid, identity.start_ticks)


@dataclass(repr=False)
class _SqlClientExecution:
    """Private mutable observations held by one supervisor, including on failure.

    Gateways are the original admitted evidence/journal/directory capabilities;
    retaining them keeps their pool reservations reachable. Teardown must not
    close the shared pool, which may own other attempts' still-running actors.
    """

    expected_input: TdsInputReceipt
    termination_timeout: float
    clock: Callable[[], float]
    gateways: tuple[_BoundedClose, ...]
    process: SqlClientProcess | None = None
    unresolved_launch: TdsUnresolvedLaunch | None = None
    expected_process: TdsProcessIdentity | None = None
    launch: SqlClientLaunch | None = None
    raw_result: bytes | None = field(default=None, repr=False)
    result: SqlClientResult | None = None
    local_exit: TdsChildExit | None = None
    receipts: dict[SqlClientEvidenceKind, SqlClientEvidenceReceipt] = field(default_factory=dict)
    _containment_deadline: float | None = field(default=None, init=False)
    _deadline_captured: bool = field(default=False, init=False)
    _process_closed: bool = field(default=False, init=False)
    _process_close_attempted: bool = field(default=False, init=False)
    _launch_close_attempted: bool = field(default=False, init=False)
    _launch_contained: bool = field(default=False, init=False)
    _launch_closed: bool = field(default=False, init=False)
    _pid: int = field(default_factory=os.getpid, init=False)
    _thread: object = field(default_factory=current_thread, init=False)
    _busy: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if (
            type(self.expected_input) is not TdsInputReceipt
            or type(self.termination_timeout) not in (int, float)
            or not math.isfinite(self.termination_timeout)
            or self.termination_timeout <= 0
            or not callable(self.clock)
            or type(self.gateways) is not tuple
        ):
            raise ValueError("mssql_native.sqlclient_retention_invalid")

    def _owned(self) -> None:
        if os.getpid() != self._pid or current_thread() is not self._thread or self._busy:
            raise WindowContractError("mssql_native.sqlclient_retention_owner_mismatch")

    def capture_result(self) -> None:
        """Keep only a bounded EOF observation; never decode, hash or persist it.

        The transport guarantees actual EOF. Its retained bytes are still
        untrusted, including when a later descriptor close or deadline failed.
        An already retained body cannot be replaced by a subsequent observation.
        """
        if self.process is None:
            return
        raw = self.process.received_result
        if raw is None:
            return
        if type(raw) is not bytes or not 0 < len(raw) <= evidence_limit(SqlClientEvidenceKind.RESULT):
            raise ValueError("mssql_native.sqlclient_retained_result_invalid")
        if self.raw_result is not None and self.raw_result != raw:
            raise ValueError("mssql_native.sqlclient_retained_result_changed")
        self.raw_result = raw

    def capture_containment_deadline(self) -> float:
        """Capture once before fallible cleanup; an invalid clock cannot be retried."""
        if not self._deadline_captured:
            self._deadline_captured = True
            try:
                now = self.clock()
            except BaseException:
                raise SqlClientSupervisionUnknown(self) from None
            if type(now) in (int, float) and math.isfinite(now):
                deadline = now + self.termination_timeout
                if math.isfinite(deadline):
                    self._containment_deadline = deadline
        if self._containment_deadline is None:
            raise SqlClientSupervisionUnknown(self)
        return self._containment_deadline

    def _close_process(self, deadline: float) -> None:
        if self.process is not None and not self._process_closed:
            if self.local_exit is None:
                before(deadline, self.clock)
                observed = self.process.terminate(deadline=deadline)
                validate_local_exit(observed, self.expected_process)
                self.local_exit = observed
            validate_local_exit(self.local_exit, self.expected_process)
            before(deadline, self.clock)
            if self._process_close_attempted:
                raise SqlClientSupervisionUnknown(self)
            self._process_close_attempted = True
            self.process.close()
            self._process_closed = True
        if self.unresolved_launch is not None and not self._launch_closed:
            if not self._launch_contained:
                before(deadline, self.clock)
                self.unresolved_launch.contain(deadline=deadline)
                self._launch_contained = True
            before(deadline, self.clock)
            if self._launch_close_attempted:
                raise SqlClientSupervisionUnknown(self)
            self._launch_close_attempted = True
            self.unresolved_launch.close()
            self._launch_closed = True

    def close(self, *, deadline: float) -> None:
        """Attempt every teardown using the one captured, never renewed deadline."""
        self._owned()
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("mssql_native.sqlclient_retention_deadline_invalid")
        self._busy = True
        failed = False
        try:
            try:
                deadline = min(deadline, self.capture_containment_deadline())
            except BaseException:
                # No trustworthy time budget remains. Still signal every actor
                # with a zero-budget close; do not invent a process stop budget.
                deadline = min(deadline, 0.0)
                failed = True
            try:
                self.capture_result()
            except BaseException:
                failed = True
            if self._containment_deadline is not None:
                try:
                    self._close_process(deadline)
                except BaseException:
                    failed = True
            # Actor close signals shutdown even if its bounded wait has expired.
            # A poisoned journal can never prevent the preceding local stop.
            for gateway in self.gateways:
                try:
                    gateway.close(deadline=deadline)
                except BaseException:
                    failed = True
            try:
                before(deadline, self.clock)
            except BaseException:
                failed = True
        finally:
            self._busy = False
        if failed:
            raise SqlClientSupervisionUnknown(self) from None


class SqlClientSupervisionUnknown(WindowOutcomeUnknown):
    """Uncertain execution with bounded teardown only, never resend/retry access.

    Private ownership preserves nonsecret originals, EOF bytes and acknowledged
    evidence. Neither a successful close nor a reaped exit settles SQL writes.
    """

    def __init__(self, retained: _SqlClientExecution) -> None:
        self._retained = retained
        super().__init__("mssql_native.sqlclient_supervision_unknown")

    def close(self, *, deadline: float) -> None:
        """Stop retained local resources, clamping to the original cleanup budget."""
        self._retained.close(deadline=deadline)
