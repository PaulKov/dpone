"""Process-safe local SQLite CAS journal; does not fence external database writers.

Use a durable local path under runtime.storage.work_dir. Network filesystems are
unsupported. Callers inject the clock; expiration uses its Unix timestamp scale.
"""

from __future__ import annotations

import math
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

from dpone.contracts.bounded_window import WindowLease, WindowRecord
from dpone.contracts.process_errors import WindowContractError, WindowLeaseLost


class SQLiteWindowStore:
    """Short BEGIN IMMEDIATE transactions serialize journal and lease ownership."""

    def __init__(self, path: Path, *, clock: Callable[[], float]) -> None:
        self._path = path
        self._clock = clock
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute(
                "CREATE TABLE IF NOT EXISTS window_leases "
                "(target TEXT PRIMARY KEY, owner TEXT NOT NULL, fence INTEGER NOT NULL, "
                "expires REAL NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS window_records "
                "(key TEXT PRIMARY KEY, revision INTEGER NOT NULL, payload TEXT NOT NULL)"
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self._path, timeout=30, isolation_level=None)
        try:
            db.execute("PRAGMA synchronous=FULL")
            yield db
        finally:
            db.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def _ttl(ttl: float) -> None:
        if not math.isfinite(ttl) or ttl <= 0:
            raise WindowContractError("Lease TTL must be finite and positive")

    def acquire(self, target_id: str, owner: str, ttl: float) -> WindowLease:
        """Never share a live lease, including a repeated owner string."""
        self._ttl(ttl)
        if not target_id or not owner:
            raise WindowContractError("Lease target and owner must be nonempty")
        with self._transaction() as db:
            now = self._clock()
            row = db.execute("SELECT fence, expires FROM window_leases WHERE target=?", (target_id,)).fetchone()
            if row and row[1] > now:
                raise WindowLeaseLost("Target has an active writer lease")
            fence = row[0] + 1 if row else 1
            db.execute("INSERT OR REPLACE INTO window_leases VALUES (?,?,?,?)", (target_id, owner, fence, now + ttl))
        return WindowLease(target_id, owner, fence)

    def _assert(self, db: sqlite3.Connection, lease: WindowLease) -> None:
        row = db.execute(
            "SELECT owner, fence, expires FROM window_leases WHERE target=?", (lease.target_id,)
        ).fetchone()
        if not row or row[0] != lease.owner or row[1] != lease.fence or row[2] <= self._clock():
            raise WindowLeaseLost("Writer lease expired or fencing token changed")

    def assert_lease(self, lease: WindowLease) -> None:
        """Check ownership; adapters additionally fence mutations at their boundary."""
        with self._connect() as db:
            self._assert(db, lease)

    def renew(self, lease: WindowLease, ttl: float) -> None:
        """Renew only an unexpired epoch, never resurrect a stale writer."""
        self._ttl(ttl)
        with self._transaction() as db:
            self._assert(db, lease)
            db.execute("UPDATE window_leases SET expires=? WHERE target=?", (self._clock() + ttl, lease.target_id))

    def release(self, lease: WindowLease) -> None:
        """Retain the epoch; releasing an old owner cannot affect a newer lease."""
        with self._transaction() as db:
            db.execute(
                "UPDATE window_leases SET expires=0 WHERE target=? AND owner=? AND fence=?",
                (lease.target_id, lease.owner, lease.fence),
            )

    def load(self, key: str) -> WindowRecord | None:
        """Return the durable metadata payload and compare-and-swap revision."""
        with self._connect() as db:
            row = db.execute("SELECT revision,payload FROM window_records WHERE key=?", (key,)).fetchone()
        return WindowRecord(*row) if row else None

    def save(self, key: str, expected: int | None, payload: str, lease: WindowLease) -> WindowRecord:
        """Check fencing and revision inside the same transaction as the write."""
        with self._transaction() as db:
            self._assert(db, lease)
            row = db.execute("SELECT revision FROM window_records WHERE key=?", (key,)).fetchone()
            if (row[0] if row else None) != expected:
                raise WindowContractError("Checkpoint compare-and-swap conflict")
            revision = 1 if expected is None else expected + 1
            db.execute("INSERT OR REPLACE INTO window_records VALUES (?,?,?)", (key, revision, payload))
        return WindowRecord(revision, payload)
