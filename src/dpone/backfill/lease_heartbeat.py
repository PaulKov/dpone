"""Small DI heartbeat primitive for long-running backfill chunk leases."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from threading import Event, Thread


class BackfillLeaseHeartbeatError(RuntimeError):
    """The current chunk owner could no longer renew its durable lease."""

    code = "DPONE_BACKFILL_CHUNK_LEASE_HEARTBEAT_LOST"

    def __init__(self) -> None:
        super().__init__(self.code)


class BackfillLeaseHeartbeat:
    """Renew a lease on an isolated daemon thread and retain the first error."""

    def __init__(
        self,
        renew: Callable[[], bool],
        *,
        initial_expiry: datetime,
        now: Callable[[], datetime],
    ) -> None:
        ttl_seconds = max(0.0, (initial_expiry - now()).total_seconds())
        self._interval = min(30.0, max(0.1, ttl_seconds / 3.0))
        self._renew = renew
        self._stop = Event()
        self._failed = Event()
        self._thread = Thread(target=self._run, name="dpone-backfill-lease", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def prove_ownership(self) -> None:
        """Synchronously renew once so short chunks cannot bypass health proof."""

        if not self._renew_once():
            self._failed.set()
        self.assert_healthy()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(1.0, self._interval + 1.0))

    def assert_healthy(self) -> None:
        if self._failed.is_set():
            raise BackfillLeaseHeartbeatError()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            if not self._renew_once():
                self._failed.set()
                return

    def _renew_once(self) -> bool:
        try:
            return bool(self._renew())
        except Exception:  # The caller observes only the stable typed boundary.
            return False


__all__ = ["BackfillLeaseHeartbeat", "BackfillLeaseHeartbeatError"]
