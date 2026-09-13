"""One execution deadline and a separate, nonrenewable cleanup allowance.

Expiry or shutdown forbids admitting new effects; it does not cancel effects
already admitted or release their resource slots. Callers must propagate the
remaining time to actual I/O. Cleanup is restricted by the application to
journal persistence, read-only observation and credential revocation/closure.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from threading import Event, Lock


class ExecutionBudgetExpired(RuntimeError):
    """No admission authority remains; already running I/O is not cancelled."""


class ExecutionStopSignal:
    """First published stop time, without Python locks in the signal callback.

    A single dict.setdefault publishes the immutable timestamp. Reentrant or
    repeated notification cannot replace it, and no Event condition or budget
    lock is acquired. Normal coordinator code owns listener shutdown and joins.
    """

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._notice: dict[str, float] = {}

    def notify(self) -> None:
        self._notice.setdefault("stopped_at", self._clock())

    def is_set(self) -> bool:
        return bool(self._notice)

    @property
    def stopped_at(self) -> float | None:
        return self._notice.get("stopped_at")


class CompositionExecutionBudget:
    """Shared-stop-aware execution with one explicit irreversible cleanup phase."""

    def __init__(
        self,
        execution_timeout_seconds: int,
        *,
        stop_event: Event | ExecutionStopSignal,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if type(execution_timeout_seconds) is not int or not 1 <= execution_timeout_seconds <= 900:
            raise ValueError("composition_execution_timeout")
        if not isinstance(stop_event, (Event, ExecutionStopSignal)) or not callable(clock):
            raise ValueError("composition_execution_budget_configuration")
        self._clock, self._stop = clock, stop_event
        self._execution_deadline = clock() + execution_timeout_seconds
        self._cleanup_deadline: float | None = None
        self._lock = Lock()

    @property
    def execution_deadline(self) -> float:
        return self._execution_deadline

    @property
    def execution_expired(self) -> bool:
        with self._lock:
            self._observe_stop()
            return self._stop.is_set() or self._clock() >= self._execution_deadline

    def require_effect(self) -> None:
        """Check immediately before a new business effect; this is not a permit."""
        self.remaining_execution()

    def remaining_execution(self) -> float:
        with self._lock:
            self._observe_stop()
            remaining = self._execution_deadline - self._clock()
            if self._stop.is_set() or self._cleanup_deadline is not None or remaining <= 0:
                raise ExecutionBudgetExpired("composition_execution_closed")
            return remaining

    def begin_cleanup(self) -> float:
        """Close admission and establish at most sixty seconds exactly once."""
        with self._lock:
            self._observe_stop()
            if self._cleanup_deadline is None:
                self._cleanup_deadline = min(self._clock(), self._execution_deadline) + 60
            return self._cleanup_deadline

    def io_deadline(self) -> float:
        """Return the current absolute I/O bound without granting effect authority.

        Normal expiry never opens cleanup implicitly. Explicit cleanup or the
        first shutdown notification selects the fixed cleanup deadline; callers
        must still check require_effect before every new business mutation.
        """
        with self._lock:
            self._observe_stop()
            deadline = self._execution_deadline if self._cleanup_deadline is None else self._cleanup_deadline
            if self._clock() >= deadline:
                raise ExecutionBudgetExpired("composition_io_expired")
            return deadline

    def _observe_stop(self) -> None:
        if self._stop.is_set() and self._cleanup_deadline is None:
            stopped_at = self._stop.stopped_at if isinstance(self._stop, ExecutionStopSignal) else self._clock()
            assert stopped_at is not None
            self._cleanup_deadline = min(stopped_at, self._execution_deadline) + 60

    def stop(self) -> None:
        """Normal coordinator API; signal handlers notify ExecutionStopSignal only."""
        if isinstance(self._stop, ExecutionStopSignal):
            self._stop.notify()
        else:
            self._stop.set()
        with self._lock:
            self._observe_stop()

    def remaining_cleanup(self) -> float:
        """Never start or extend cleanup implicitly, including after shutdown."""
        with self._lock:
            if self._cleanup_deadline is None:
                raise ExecutionBudgetExpired("composition_cleanup_not_started")
            remaining = self._cleanup_deadline - self._clock()
            if remaining <= 0:
                raise ExecutionBudgetExpired("composition_cleanup_expired")
            return remaining
