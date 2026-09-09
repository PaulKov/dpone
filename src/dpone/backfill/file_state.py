"""Filesystem implementation of the durable backfill state port."""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dpone.backfill.state import (
    CHUNK_STATUS_FAILED,
    CHUNK_STATUS_PENDING,
    CHUNK_STATUS_RUNNING,
    CHUNK_STATUS_SUCCESS,
    DEFAULT_STATE_DIR,
    STATE_DIR_ENV,
    BackfillChunkRecord,
    BackfillLedger,
)
from dpone.backfill.state_coordination import LocalInitializationCoordinator

_UTC = timezone.utc  # noqa: UP017 - mypy target may be older than datetime.UTC.


class FileBackfillStateStore:
    """Thread-safe filesystem ledger store (one JSON file per run key)."""

    def __init__(self, root_dir: str | Path | None = None) -> None:
        raw_root = root_dir or os.environ.get(STATE_DIR_ENV) or DEFAULT_STATE_DIR
        self._root = Path(raw_root)
        self._lock = threading.RLock()
        self._initialization = LocalInitializationCoordinator()

    @property
    def root_dir(self) -> Path:
        return self._root

    def state_capabilities(self) -> dict[str, Any]:
        return {
            "backend": "local_file",
            "dialect": "file",
            "durable_read": True,
            "durable_write": True,
            "distributed_lock": False,
            "distributed_chunk_lease": False,
            "compare_and_set_completion": False,
            "lock_scope": "process",
        }

    def path_for(self, run_key: str) -> Path:
        return self._root / f"{run_key}.json"

    def load(self, run_key: str) -> BackfillLedger | None:
        with self._lock:
            path = self.path_for(run_key)
            if not path.exists():
                return None
            data = json.loads(path.read_text(encoding="utf-8"))
            return BackfillLedger.from_dict(data)

    def load_verified(self, run_key: str, *, plan_hash: str | None, config_hash: str | None) -> BackfillLedger | None:
        """Load a campaign and fail fast when a pinned id points to a different plan."""

        ledger = self.load(run_key)
        if ledger is None:
            return None
        if config_hash and ledger.config_hash and ledger.config_hash != config_hash:
            raise ValueError("backfill campaign config hash changed; use a new backfill_id to start a new campaign")
        if plan_hash and ledger.plan_hash and ledger.plan_hash != plan_hash:
            raise ValueError("backfill campaign plan hash changed; use a new backfill_id to start a new campaign")
        return ledger

    def save(self, ledger: BackfillLedger) -> Path:
        with self._lock:
            now = datetime.now(timezone.utc).isoformat()  # noqa: UP017
            ledger.created_at = ledger.created_at or now
            ledger.updated_at = now
            return self._write_exact_snapshot(ledger)

    def sync_committed_snapshot(self, ledger: BackfillLedger) -> Path:
        """Atomically cache an already timestamped authoritative snapshot.

        SQL-backed stores call this only after their database transaction is
        known to have committed.  Keeping timestamp ownership in the durable
        backend prevents the local cache and SQL journal from describing two
        subtly different revisions.
        """

        with self._lock:
            return self._write_exact_snapshot(ledger)

    def _write_exact_snapshot(self, ledger: BackfillLedger) -> Path:
        self._root.mkdir(parents=True, exist_ok=True)
        path = self.path_for(ledger.run_key)
        tmp_path = path.with_suffix(".json.tmp")
        tmp_path.write_text(
            json.dumps(ledger.to_jsonable(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        tmp_path.replace(path)
        return path

    def update_chunk(self, ledger: BackfillLedger, record: BackfillChunkRecord) -> None:
        with self._lock:
            record.updated_at = datetime.now(timezone.utc).isoformat()  # noqa: UP017
            current = self.load(ledger.run_key) or ledger
            for position, existing in enumerate(current.chunks):
                if existing.index == record.index:
                    current.chunks[position] = record
                    break
            else:
                current.chunks.append(record)
            self.save(current)

    def acquire_campaign_lock(self, run_key: str, *, owner: str, lease_expires_at: datetime) -> bool:
        with self._lock:
            ledger = self._require(run_key)
            if ledger.status == "cancel_requested" and not _published(ledger):
                return False
            if ledger.status == "cancel_requested":
                ledger.status = "active"
                ledger.cancel_reason = None
                ledger.cancel_requested_by = None
            if ledger.lock_owner and not _expired(ledger.lock_expires_at):
                return False
            ledger.lock_owner = owner
            ledger.lock_expires_at = _dt(lease_expires_at)
            self.save(ledger)
            return True

    def release_campaign_lock(self, run_key: str, *, owner: str) -> None:
        with self._lock:
            ledger = self._require(run_key)
            if ledger.lock_owner == owner:
                ledger.lock_owner = None
                ledger.lock_expires_at = None
                self.save(ledger)

    def renew_campaign_lock(self, run_key: str, *, owner: str, lease_expires_at: datetime) -> bool:
        """Extend only the current, unexpired durable campaign owner."""

        return self._renew_campaign_lock_if_owned(
            run_key,
            owner=owner,
            lease_expires_at=lease_expires_at,
            allow_expired=False,
        )

    def _renew_campaign_lock_if_owned(
        self,
        run_key: str,
        *,
        owner: str,
        lease_expires_at: datetime,
        allow_expired: bool,
    ) -> bool:
        """Refresh an exact owner; only an external liveness fence may allow expiry."""

        with self._lock:
            ledger = self._require(run_key)
            if (
                (ledger.status == "cancel_requested" and not _published(ledger))
                or ledger.lock_owner != owner
                or (not allow_expired and _expired(ledger.lock_expires_at))
                or lease_expires_at <= datetime.now(_UTC)
            ):
                return False
            current_expiry = datetime.fromisoformat(str(ledger.lock_expires_at)) if ledger.lock_expires_at else None
            if current_expiry is None or lease_expires_at > current_expiry:
                ledger.lock_expires_at = _dt(lease_expires_at)
                self.save(ledger)
            return True

    def acquire_initialization_lock(self, run_key: str, *, owner: str) -> bool:
        """Serialize campaign creation without requiring an existing ledger."""

        return self._initialization.acquire(run_key, owner=owner)

    def release_initialization_lock(self, run_key: str, *, owner: str) -> None:
        self._initialization.release(run_key, owner=owner)

    def request_cancel(self, run_key: str, *, reason: str, requested_by: str) -> None:
        with self._lock:
            ledger = self._require(run_key)
            if _published(ledger):
                raise RuntimeError("backfill cancellation is closed after target publication")
            ledger.status = "cancel_requested"
            ledger.cancel_reason = reason
            ledger.cancel_requested_by = requested_by
            self.save(ledger)

    def acquire_chunk_lease(self, run_key: str, index: int, *, owner: str, lease_expires_at: datetime) -> bool:
        with self._lock:
            ledger = self._require(run_key)
            if ledger.status == "cancel_requested":
                return False
            record = ledger.chunk(index)
            if record.status == CHUNK_STATUS_SUCCESS:
                return False
            if record.status == CHUNK_STATUS_RUNNING and not _expired(record.lease_expires_at):
                return False
            record.status = CHUNK_STATUS_RUNNING
            record.attempts += 1
            record.lease_owner = owner
            record.lease_expires_at = _dt(lease_expires_at)
            record.started_at = record.started_at or datetime.now(timezone.utc).isoformat()  # noqa: UP017
            record.error = None
            self.update_chunk(ledger, record)
            return True

    def complete_chunk_if_owned(self, run_key: str, record: BackfillChunkRecord, *, owner: str) -> bool:
        """Persist a terminal chunk transition only for the current lease owner."""

        with self._lock:
            ledger = self._require(run_key)
            current = ledger.chunk(record.index)
            if current.status != CHUNK_STATUS_RUNNING or current.lease_owner != owner:
                return False
            record.lease_owner = None
            record.lease_expires_at = None
            self.update_chunk(ledger, record)
            return True

    def renew_chunk_lease(self, run_key: str, index: int, *, owner: str, lease_expires_at: datetime) -> bool:
        """Extend only the current, still-live owner lease."""

        with self._lock:
            ledger = self._require(run_key)
            record = ledger.chunk(index)
            if (
                record.status != CHUNK_STATUS_RUNNING
                or record.lease_owner != owner
                or _expired(record.lease_expires_at)
                or lease_expires_at <= datetime.now(_UTC)
            ):
                return False
            current_expiry = datetime.fromisoformat(str(record.lease_expires_at))
            if lease_expires_at > current_expiry:
                record.lease_expires_at = _dt(lease_expires_at)
                self.update_chunk(ledger, record)
            return True

    def recover_stale_running(self, run_key: str, *, now: datetime) -> list[int]:
        with self._lock:
            ledger = self._require(run_key)
            recovered: list[int] = []
            for record in ledger.chunks:
                if record.status != CHUNK_STATUS_RUNNING or not _expired(record.lease_expires_at, now=now):
                    continue
                record.status = CHUNK_STATUS_FAILED
                record.error = "lease_expired"
                record.lease_owner = None
                record.lease_expires_at = None
                recovered.append(record.index)
            if recovered:
                self.save(ledger)
            return recovered

    def retryable_chunk_indexes(self, run_key: str) -> list[int]:
        ledger = self._require(run_key)
        return [
            record.index for record in ledger.chunks if record.status in {CHUNK_STATUS_FAILED, CHUNK_STATUS_PENDING}
        ]

    def _require(self, run_key: str) -> BackfillLedger:
        ledger = self.load(run_key)
        if ledger is None:
            raise KeyError(f"backfill campaign {run_key!r} does not exist")
        return ledger


def _dt(value: datetime) -> str:
    return value.astimezone(_UTC).isoformat()


def _expired(value: str | None, *, now: datetime | None = None) -> bool:
    if not value:
        return True
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    current = (now or datetime.now(_UTC)).astimezone(_UTC)
    return parsed <= current


def _published(ledger: BackfillLedger) -> bool:
    publication = ledger.publication
    return publication is not None and publication.phase == "published"


__all__ = ["FileBackfillStateStore"]
