"""Exclusive operation custody and bounded complete-result cursor acquisition.

One owner is explicitly injected into facades that share an actual cursor. Its
lock covers state transitions only: clocks, authority checks and driver calls
run outside the lock. A rejected overlap invalidates the active lease even when
its caller catches the rejection. This does not interrupt synchronous drivers;
external process containment remains required.
"""

import os
import threading
from collections.abc import Callable
from typing import Any


class ObservationOwnerMismatch(ValueError):
    """The cursor's original process or thread no longer owns this operation."""


class ObservationCursor:
    """Private SQL adapter owner; statements and limits are chosen by adapters."""

    def __init__(
        self,
        cursor: Any,
        *,
        deadline: int | float,
        clock: Callable[[], int] | None = None,
        check_deadline: Callable[[], None] | None = None,
    ) -> None:
        self.cursor, self.clock = cursor, clock
        self._deadline = deadline
        self._check_deadline = check_deadline
        self.owner = (os.getpid(), threading.current_thread())
        self.lock = threading.Lock()
        self._active: object | None = None
        self._faulted = False

    @property
    def deadline(self) -> int | float:
        return self._deadline

    @property
    def busy(self) -> bool:
        return self._active is not None

    @property
    def faulted(self) -> bool:
        return self._faulted

    def _verify(self, lease: object | None, *, begin: bool = False) -> None:
        # Caller holds the lock. Never invoke user code here.
        if self.owner != (os.getpid(), threading.current_thread()):
            self._faulted, self._active = True, None
            raise ObservationOwnerMismatch("owner_mismatch")
        if self._faulted or (self._active is not None if begin else lease is None or lease is not self._active):
            self._faulted, self._active = True, None
            raise ValueError("faulted")

    def begin(self) -> object:
        with self.lock:
            self._verify(None, begin=True)
            self._active = object()
            return self._active

    def healthy(self, lease: object | None) -> None:
        with self.lock:
            self._verify(lease)

    def current(self, lease: object | None) -> None:
        try:
            self.healthy(lease)
            if self.clock is not None:
                now = self.clock()
                if type(now) is not int or now < 0 or now >= self._deadline:
                    raise ValueError("expired")
            else:
                assert self._check_deadline is not None
                self._check_deadline()
            self.healthy(lease)
        except BaseException:
            self.fail(lease)
            raise

    def finish(self, lease: object | None) -> None:
        """Atomic success point after the algorithm's final current check."""
        with self.lock:
            self._verify(lease)
            self._active = None

    def fail(self, lease: object | None = None) -> None:
        """Any ambiguity poisons every facade; the connection owner closes it."""
        with self.lock:
            self._faulted, self._active = True, None

    def rows(
        self,
        lease: object | None,
        statement: str,
        parameters: tuple[Any, ...] = (),
        *,
        max_rows: int,
        nextset_required: bool,
        detach_rows: bool,
        current: Callable[[], None] | None = None,
    ) -> list[Any]:
        """Require EOF before returning; never truncate a sentinel row or set."""
        check = current if current is not None else lambda: self.current(lease)
        try:
            if type(max_rows) is not int or not 1 <= max_rows <= 8194:
                raise ValueError("cardinality")
            check()
            self.cursor.execute(statement, *parameters)
            check()
            rows: list[Any] = []
            for _ in range(max_rows + 1):
                check()
                row = self.cursor.fetchone()
                check()
                if row is None:
                    nextset = getattr(self.cursor, "nextset", None)
                    if nextset is None and nextset_required:
                        raise ValueError("missing_nextset")
                    if nextset is not None:
                        check()
                        extra = nextset()
                        check()
                        if extra is not None and extra is not False:
                            raise ValueError("extra_result_set")
                    return rows
                rows.append(tuple(row) if detach_rows else row)
            raise ValueError("cardinality")
        except BaseException:
            self.fail(lease)
            raise
