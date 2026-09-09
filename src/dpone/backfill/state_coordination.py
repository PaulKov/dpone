"""Process-local coordination for file-backed backfill state."""

from __future__ import annotations

import threading


class LocalInitializationCoordinator:
    """Serialize campaign creation before a ledger exists."""

    def __init__(self) -> None:
        self._owners: dict[str, str] = {}
        self._lock = threading.RLock()

    def acquire(self, run_key: str, *, owner: str) -> bool:
        with self._lock:
            if run_key in self._owners:
                return False
            self._owners[run_key] = owner
            return True

    def release(self, run_key: str, *, owner: str) -> None:
        with self._lock:
            if self._owners.get(run_key) == owner:
                self._owners.pop(run_key, None)


__all__ = ["LocalInitializationCoordinator"]
