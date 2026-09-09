"""Distributed coordination primitives for SQL-backed backfill state."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, TypeVar

_ResultT = TypeVar("_ResultT")


class ExternalStateLock(Protocol):
    """Minimal adapter shared by PostgreSQL and SQL Server lock implementations."""

    def acquire(self, run_key: str) -> bool: ...

    def release(self, run_key: str) -> None: ...


class SQLStateLockCoordinator:
    """Apply external locks around per-chunk and initialization transitions."""

    def __init__(self, *, chunk_lock: ExternalStateLock, initialization_lock: ExternalStateLock) -> None:
        self._chunk_lock = chunk_lock
        self._initialization_lock = initialization_lock
        self._held_initialization_locks: set[tuple[str, str]] = set()

    def run_chunk_transition(self, run_key: str, index: int, operation: Callable[[], _ResultT]) -> _ResultT | bool:
        key = f"{run_key}:chunk:{index}"
        if not self._chunk_lock.acquire(key):
            return False
        try:
            return operation()
        finally:
            self._chunk_lock.release(key)

    def acquire_initialization(self, run_key: str, *, owner: str) -> bool:
        if not self._initialization_lock.acquire(run_key):
            return False
        self._held_initialization_locks.add((run_key, owner))
        return True

    def release_initialization(self, run_key: str, *, owner: str) -> None:
        key = (run_key, owner)
        if key not in self._held_initialization_locks:
            return
        self._held_initialization_locks.remove(key)
        self._initialization_lock.release(run_key)


__all__ = ["ExternalStateLock", "SQLStateLockCoordinator"]
