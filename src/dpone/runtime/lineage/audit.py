"""Load identity lifecycle and audit storage protocol."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol

from dpone.runtime.lineage.identity import LineageIdentityService

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


@dataclass(frozen=True, slots=True)
class LoadAuditRecord:
    """Serializable audit record for an atomic load package."""

    run_id: str
    load_id: str
    status: str
    process_name: str | None
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    strategy: str
    started_at: datetime
    staged_at: datetime | None = None
    committed_at: datetime | None = None
    failed_at: datetime | None = None
    extracted_rows: int | None = None
    staged_rows: int | None = None
    loaded_rows: int | None = None
    inserted_rows: int | None = None
    updated_rows: int | None = None
    deleted_rows: int | None = None
    reactivated_rows: int | None = None
    unchanged_rows: int | None = None
    soft_deleted_rows: int | None = None
    hard_deleted_rows: int | None = None
    active_rows: int | None = None
    total_rows: int | None = None
    commit_receipt_id: str | None = None
    commit_outcome: str | None = None
    error_message: str | None = None
    artifact_uri: str | None = None


class LoadAuditStorage(Protocol):
    """Protocol implemented by BigQuery/Postgres/MSSQL load audit backends."""

    def record_load_started(self, record: LoadAuditRecord) -> None: ...

    def record_load_staged(self, record: LoadAuditRecord) -> None: ...

    def record_load_committed(self, record: LoadAuditRecord) -> None: ...

    def record_load_failed(self, record: LoadAuditRecord) -> None: ...


class LoadIdentityService:
    """Creates run/load IDs and records load lifecycle transitions."""

    def __init__(
        self,
        *,
        identity_service: LineageIdentityService | None = None,
        audit_storage: LoadAuditStorage | None = None,
        on_receipt_backed_commit_audit_failure: Callable[[LoadAuditRecord], None] | None = None,
    ) -> None:
        self.identity_service = identity_service or LineageIdentityService()
        self.audit_storage = audit_storage
        self.on_receipt_backed_commit_audit_failure = on_receipt_backed_commit_audit_failure
        self._terminal_commits: dict[str, LoadAuditRecord] = {}

    def start(self, load_config: LoadConfig, *, process_name: str | None = None) -> LoadAuditRecord:
        now = _utc_now()
        record = LoadAuditRecord(
            run_id=self.identity_service.new_run_id(),
            load_id=self.identity_service.new_load_id(),
            status="started",
            process_name=process_name,
            source_schema=load_config.source_schema,
            source_table=load_config.source_table,
            target_schema=load_config.target_schema,
            target_table=load_config.target_table,
            strategy=load_config.load_strategy.value,
            started_at=now,
        )
        if self.audit_storage:
            self.audit_storage.record_load_started(record)
        return record

    def mark_staged(self, record: LoadAuditRecord, *, extracted_rows: int | None = None) -> LoadAuditRecord:
        updated = replace(
            record,
            status="staged",
            staged_at=_utc_now(),
            extracted_rows=extracted_rows,
        )
        if self.audit_storage:
            self.audit_storage.record_load_staged(updated)
        return updated

    def mark_committed(self, record: LoadAuditRecord, load_result: Any) -> LoadAuditRecord:
        soft_deleted_rows = getattr(load_result, "soft_deleted_rows", None)
        total_rows = getattr(load_result, "total_rows", None)
        updated = replace(
            record,
            status="committed",
            committed_at=_utc_now(),
            staged_rows=getattr(load_result, "staging_rows", None),
            loaded_rows=total_rows,
            inserted_rows=getattr(load_result, "inserted_rows", None),
            updated_rows=getattr(load_result, "updated_rows", None),
            deleted_rows=soft_deleted_rows,
            reactivated_rows=getattr(load_result, "reactivated_rows", None),
            unchanged_rows=getattr(load_result, "unchanged_rows", None),
            soft_deleted_rows=soft_deleted_rows,
            hard_deleted_rows=getattr(load_result, "hard_deleted_rows", None),
            active_rows=getattr(load_result, "active_rows", None),
            total_rows=total_rows,
            commit_receipt_id=getattr(load_result, "commit_receipt_id", None),
            commit_outcome=_enum_value(getattr(load_result, "commit_outcome", None)),
        )
        if self.audit_storage:
            try:
                self.audit_storage.record_load_committed(updated)
            except Exception:
                if not _is_receipt_backed_commit(updated):
                    raise
                self._report_receipt_backed_commit_audit_failure(updated)
        if _is_receipt_backed_commit(updated):
            self._terminal_commits[updated.load_id] = updated
        return updated

    def _report_receipt_backed_commit_audit_failure(self, record: LoadAuditRecord) -> None:
        """Report a repairable audit gap without denying a known commit.

        The target and commit receipt are already durable at this point.  A
        raised exception would make the orchestrator claim failure and invite
        a blind retry, so only this receipt-backed terminal transition is
        non-fatal.  The receipt remains the recovery authority.
        """

        callback = self.on_receipt_backed_commit_audit_failure
        if callback is None:
            return
        try:
            callback(record)
        except Exception:
            return

    def mark_failed(self, record: LoadAuditRecord, exc: Exception) -> LoadAuditRecord:
        committed = self._terminal_commits.get(record.load_id)
        if committed is not None:
            return committed
        updated = replace(
            record,
            status="failed",
            failed_at=_utc_now(),
            error_message=str(exc),
        )
        if self.audit_storage:
            self.audit_storage.record_load_failed(updated)
        return updated


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _enum_value(value: object | None) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "value", value))


def _is_receipt_backed_commit(record: LoadAuditRecord) -> bool:
    return bool(record.commit_receipt_id) and record.commit_outcome in {
        "committed",
        "committed_after_receipt_probe",
        "replay_suppressed",
    }
