"""Durable backfill chunk ledger.

The ledger is a small JSON document keyed by ``run_key`` and stored on the
local filesystem (default ``.dpone/backfill``). It records per-chunk commit
status so an interrupted backfill resumes from the first non-committed chunk
instead of re-loading history from scratch.

The store is intentionally filesystem-based:

- it works identically for CLI, Airflow pods (mounted work dirs) and CI;
- the ledger is human-readable evidence that can be attached to reviews;
- no extra DDL is required in any of the supported state backends.

Writes are atomic (tmp file + rename) and guarded by a re-entrant lock so the
orchestrator can run chunks in parallel threads.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from dpone.backfill.xmin_handoff_models import BackfillXminHandoffRecord

CHUNK_STATUS_PENDING = "pending"
CHUNK_STATUS_RUNNING = "running"
CHUNK_STATUS_SUCCESS = "success"
CHUNK_STATUS_FAILED = "failed"

DEFAULT_STATE_DIR = ".dpone/backfill"
STATE_DIR_ENV = "DPONE_BACKFILL_STATE_DIR"
BACKFILL_LEDGER_SCHEMA_VERSION = "2"
BACKFILL_LEDGER_MIGRATION_ERROR = "DPONE_BACKFILL_LEDGER_MIGRATION_REQUIRED"


@dataclass
class BackfillChunkRecord:
    """Persistent status of one chunk."""

    index: int
    start: str
    end: str
    idempotency_key: str
    status: str = CHUNK_STATUS_PENDING
    attempts: int = 0
    rows_extracted: int = 0
    rows_loaded: int = 0
    run_id: str | None = None
    load_id: str | None = None
    error: str | None = None
    lease_owner: str | None = None
    lease_expires_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    updated_at: str | None = None
    execution_evidence: dict[str, Any] = field(default_factory=dict)

    def require_same_coordinates(self, incoming: BackfillChunkRecord) -> None:
        """Reject mutation of this chunk's partition or idempotency identity."""

        current = (self.index, self.start, self.end, self.idempotency_key)
        candidate = (incoming.index, incoming.start, incoming.end, incoming.idempotency_key)
        if current != candidate:
            raise ValueError(f"backfill chunk {self.index} immutable coordinates changed")

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "start": self.start,
            "end": self.end,
            "idempotency_key": self.idempotency_key,
            "status": self.status,
            "attempts": self.attempts,
            "rows_extracted": self.rows_extracted,
            "rows_loaded": self.rows_loaded,
            "run_id": self.run_id,
            "load_id": self.load_id,
            "error": self.error,
            "lease_owner": self.lease_owner,
            "lease_expires_at": self.lease_expires_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "updated_at": self.updated_at,
            "execution_evidence": dict(self.execution_evidence),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BackfillChunkRecord:
        return cls(
            index=int(data["index"]),
            start=str(data.get("start") or ""),
            end=str(data.get("end") or ""),
            idempotency_key=str(data.get("idempotency_key") or ""),
            status=str(data.get("status") or CHUNK_STATUS_PENDING),
            attempts=int(data.get("attempts") or 0),
            rows_extracted=int(data.get("rows_extracted") or 0),
            rows_loaded=int(data.get("rows_loaded") or 0),
            run_id=data.get("run_id"),
            load_id=data.get("load_id"),
            error=data.get("error"),
            lease_owner=data.get("lease_owner"),
            lease_expires_at=data.get("lease_expires_at"),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            updated_at=data.get("updated_at"),
            execution_evidence=(
                dict(data["execution_evidence"]) if isinstance(data.get("execution_evidence"), dict) else {}
            ),
        )


@dataclass
class BackfillPublicationRecord:
    """Durable aggregate state of an isolated target publication."""

    mode: str
    target_table: str
    shadow_table: str
    backup_table: str
    phase: str = "planned"
    expected_rows: int | None = None
    actual_rows: int | None = None
    duplicate_keys: int | None = None
    receipt_id: str | None = None
    prepared_at: str | None = None
    published_at: str | None = None
    predecessor_object_id: int | None = None
    predecessor_create_token: str | None = None
    predecessor_receipt_id: str | None = None
    shadow_object_id: int | None = None
    shadow_create_token: str | None = None
    object_contract_sha256: str | None = None

    def __post_init__(self) -> None:
        if any(value is not None and value < 1 for value in (self.predecessor_object_id, self.shadow_object_id)):
            raise ValueError("backfill publication object identity is invalid")
        for value in (self.predecessor_create_token, self.shadow_create_token):
            if value is not None and not str(value).strip():
                raise ValueError("backfill publication object generation token is invalid")
        if self.predecessor_object_id is None and self.predecessor_create_token is not None:
            raise ValueError("backfill publication predecessor generation is incomplete")
        if self.shadow_object_id is None and self.shadow_create_token is not None:
            raise ValueError("backfill publication shadow generation is incomplete")
        if self.object_contract_sha256 is not None and not _is_sha256(self.object_contract_sha256):
            raise ValueError("backfill publication object contract digest is invalid")

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "target_table": self.target_table,
            "shadow_table": self.shadow_table,
            "backup_table": self.backup_table,
            "phase": self.phase,
            "expected_rows": self.expected_rows,
            "actual_rows": self.actual_rows,
            "duplicate_keys": self.duplicate_keys,
            "receipt_id": self.receipt_id,
            "predecessor_object_id": self.predecessor_object_id,
            "predecessor_create_token": self.predecessor_create_token,
            "predecessor_receipt_id": self.predecessor_receipt_id,
            "shadow_object_id": self.shadow_object_id,
            "shadow_create_token": self.shadow_create_token,
            "object_contract_sha256": self.object_contract_sha256,
            "prepared_at": self.prepared_at,
            "published_at": self.published_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BackfillPublicationRecord:
        return cls(
            mode=str(data.get("mode") or ""),
            target_table=str(data.get("target_table") or ""),
            shadow_table=str(data.get("shadow_table") or ""),
            backup_table=str(data.get("backup_table") or ""),
            phase=str(data.get("phase") or "planned"),
            expected_rows=_optional_int(data.get("expected_rows")),
            actual_rows=_optional_int(data.get("actual_rows")),
            duplicate_keys=_optional_int(data.get("duplicate_keys")),
            receipt_id=data.get("receipt_id"),
            predecessor_object_id=_optional_int(data.get("predecessor_object_id")),
            predecessor_create_token=data.get("predecessor_create_token"),
            predecessor_receipt_id=data.get("predecessor_receipt_id"),
            shadow_object_id=_optional_int(data.get("shadow_object_id")),
            shadow_create_token=data.get("shadow_create_token"),
            object_contract_sha256=data.get("object_contract_sha256"),
            prepared_at=data.get("prepared_at"),
            published_at=data.get("published_at"),
        )


@dataclass
class BackfillLedger:
    """Full persisted state of one backfill campaign."""

    run_key: str
    dataset: str
    inner_mode: str
    chunk_config: dict[str, Any]
    plan_hash: str | None = None
    config_hash: str | None = None
    status: str = "active"
    cancel_reason: str | None = None
    cancel_requested_by: str | None = None
    lock_owner: str | None = None
    lock_expires_at: str | None = None
    chunks: list[BackfillChunkRecord] = field(default_factory=list)
    created_at: str | None = None
    updated_at: str | None = None
    xmin_handoff: BackfillXminHandoffRecord | None = None
    publication: BackfillPublicationRecord | None = None
    portable_scope_column_contract: dict[str, Any] | None = None
    kind: str = "dpone.backfill_ledger"
    schema_version: str = BACKFILL_LEDGER_SCHEMA_VERSION

    def chunk(self, index: int) -> BackfillChunkRecord:
        for record in self.chunks:
            if record.index == index:
                return record
        raise KeyError(f"backfill ledger {self.run_key} has no chunk {index}")

    def committed_indexes(self) -> set[int]:
        return {record.index for record in self.chunks if record.status == CHUNK_STATUS_SUCCESS}

    def counts(self) -> dict[str, int]:
        counts = {
            CHUNK_STATUS_PENDING: 0,
            CHUNK_STATUS_RUNNING: 0,
            CHUNK_STATUS_SUCCESS: 0,
            CHUNK_STATUS_FAILED: 0,
        }
        for record in self.chunks:
            counts[record.status] = counts.get(record.status, 0) + 1
        return counts

    def campaign_jsonable(self) -> dict[str, Any]:
        """Return the O(1) campaign projection stored apart from chunk rows."""

        return {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "run_key": self.run_key,
            "dataset": self.dataset,
            "inner_mode": self.inner_mode,
            "chunk_config": dict(self.chunk_config),
            "plan_hash": self.plan_hash,
            "config_hash": self.config_hash,
            "status": self.status,
            "cancel_reason": self.cancel_reason,
            "cancel_requested_by": self.cancel_requested_by,
            "lock_owner": self.lock_owner,
            "lock_expires_at": self.lock_expires_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "xmin_handoff": self.xmin_handoff.to_jsonable() if self.xmin_handoff is not None else None,
            "publication": self.publication.to_jsonable() if self.publication is not None else None,
            "portable_scope_column_contract": (
                dict(self.portable_scope_column_contract) if self.portable_scope_column_contract is not None else None
            ),
        }

    def to_jsonable(self) -> dict[str, Any]:
        """Return the complete portable/file-state representation."""

        return {
            **self.campaign_jsonable(),
            "chunks": [record.to_jsonable() for record in self.chunks],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BackfillLedger:
        version = data.get("schema_version")
        if version != BACKFILL_LEDGER_SCHEMA_VERSION:
            raise ValueError(
                f"{BACKFILL_LEDGER_MIGRATION_ERROR}: expected schema_version="
                f"{BACKFILL_LEDGER_SCHEMA_VERSION}, got {version!r}"
            )
        return cls(
            run_key=str(data.get("run_key") or ""),
            dataset=str(data.get("dataset") or ""),
            inner_mode=str(data.get("inner_mode") or ""),
            chunk_config=dict(data.get("chunk_config") or {}),
            plan_hash=data.get("plan_hash"),
            config_hash=data.get("config_hash"),
            status=str(data.get("status") or "active"),
            cancel_reason=data.get("cancel_reason"),
            cancel_requested_by=data.get("cancel_requested_by"),
            lock_owner=data.get("lock_owner"),
            lock_expires_at=data.get("lock_expires_at"),
            chunks=[BackfillChunkRecord.from_dict(item) for item in data.get("chunks", [])],
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            xmin_handoff=(
                BackfillXminHandoffRecord.from_dict(data["xmin_handoff"])
                if data.get("xmin_handoff") is not None
                else None
            ),
            publication=(
                BackfillPublicationRecord.from_dict(data["publication"])
                if data.get("publication") is not None
                else None
            ),
            portable_scope_column_contract=(_portable_scope_column_contract(data)),
            schema_version=version,
        )


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _is_sha256(value: str) -> bool:
    prefix, separator, digest = value.partition(":")
    return (
        prefix == "sha256" and separator == ":" and len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)
    )


def _portable_scope_column_contract(data: dict[str, Any]) -> dict[str, Any] | None:
    """Distinguish an absent legacy field from corrupt persisted authority."""

    if "portable_scope_column_contract" not in data or data["portable_scope_column_contract"] is None:
        return None
    value = data["portable_scope_column_contract"]
    if not isinstance(value, dict):
        raise ValueError("portable_scope.binding.campaign_contract_invalid")
    return dict(value)


class BackfillStateStore(Protocol):
    """Persistence port for backfill campaign and chunk state."""

    def load(self, run_key: str) -> BackfillLedger | None: ...

    def save(self, ledger: BackfillLedger) -> Path: ...

    def update_chunk(self, ledger: BackfillLedger, record: BackfillChunkRecord) -> None: ...

    def acquire_campaign_lock(self, run_key: str, *, owner: str, lease_expires_at: datetime) -> bool: ...

    def renew_campaign_lock(self, run_key: str, *, owner: str, lease_expires_at: datetime) -> bool: ...

    def release_campaign_lock(self, run_key: str, *, owner: str) -> None: ...

    def acquire_initialization_lock(self, run_key: str, *, owner: str) -> bool: ...

    def release_initialization_lock(self, run_key: str, *, owner: str) -> None: ...

    def acquire_chunk_lease(self, run_key: str, index: int, *, owner: str, lease_expires_at: datetime) -> bool: ...

    def renew_chunk_lease(self, run_key: str, index: int, *, owner: str, lease_expires_at: datetime) -> bool: ...

    def complete_chunk_if_owned(self, run_key: str, record: BackfillChunkRecord, *, owner: str) -> bool: ...

    def recover_stale_running(self, run_key: str, *, now: datetime) -> list[int]: ...

    def request_cancel(self, run_key: str, *, reason: str, requested_by: str) -> None: ...

    def retryable_chunk_indexes(self, run_key: str) -> list[int]: ...


# Compatibility facade: the implementation imports the public ledger models
# above, so the re-export must be resolved only after those models exist.
from dpone.backfill.file_state import FileBackfillStateStore  # noqa: E402

__all__ = [
    "BACKFILL_LEDGER_MIGRATION_ERROR",
    "BACKFILL_LEDGER_SCHEMA_VERSION",
    "CHUNK_STATUS_FAILED",
    "CHUNK_STATUS_PENDING",
    "CHUNK_STATUS_RUNNING",
    "CHUNK_STATUS_SUCCESS",
    "DEFAULT_STATE_DIR",
    "STATE_DIR_ENV",
    "BackfillChunkRecord",
    "BackfillLedger",
    "BackfillPublicationRecord",
    "BackfillStateStore",
    "FileBackfillStateStore",
]
