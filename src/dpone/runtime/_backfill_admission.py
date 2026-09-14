"""Private invocation-local admission for threaded backfill execution."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from threading import Event, Lock


class _ChunkAdmission:
    """Order durable claims and closure without locking running chunks.

    A successful claim linearizes at the store's lease transition while holding
    the same lock as OPEN -> CLOSED. Already admitted peers may finish after
    closure. The lock never spans runner, heartbeat, terminal persistence, or
    reporting. Closure latency follows the provider's blocking behavior; this
    gate cannot bound or interrupt a store call already in progress.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._closed = Event()

    @property
    def closed(self) -> bool:
        """Provide a cheap scheduling hint; actual admission checks under lock."""
        return self._closed.is_set()

    def close(self) -> None:
        """Close once, ordered after any acquisition already inside the gate."""
        with self._lock:
            self._closed.set()

    def acquire(self, claim: Callable[[], bool]) -> bool | None:
        """Return acquired/rejected, or None for closed admission without I/O.

        Rejection and any escaping exception close before releasing the lock.
        An exceptional store return does not prove whether a lease was persisted.
        """
        with self._lock:
            if self._closed.is_set():
                return None
            try:
                acquired = claim()
            except BaseException:
                self._closed.set()
                raise
            if not acquired:
                self._closed.set()
            return acquired

    @contextmanager
    def on_failure(self) -> Iterator[None]:
        """Close on a synchronously observed failure, preserving propagation.

        Place this boundary inside enclosing cleanup so that cleanup cannot
        delay notification. It does not propagate failures from daemon threads.
        """
        try:
            yield
        except BaseException:
            self.close()
            raise
