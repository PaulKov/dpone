"""In-process load-step audit tap for runtime evidence payloads."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.governance.hooks import LoadStepAuditRecord, LoadStepAuditStorage


from datetime import datetime
from typing import Any

from dpone.runtime.runtime_throughput import enrich_step_details_with_throughput
from dpone.security_redaction import redact_public_text, redact_public_value


class RuntimeLoadStepAuditCollector:
    """Collect load-step audit records while forwarding them to durable storage."""

    def __init__(self, delegate: LoadStepAuditStorage | None = None) -> None:
        self._delegate = delegate
        self.records: list[LoadStepAuditRecord] = []

    def set_default_delegate(self, delegate: LoadStepAuditStorage) -> None:
        """Attach durable storage once runtime policy resolves it."""

        if self._delegate is None and delegate is not self:
            self._delegate = delegate

    def record_step(self, record: LoadStepAuditRecord) -> None:
        self.records.append(record)
        if self._delegate is not None:
            self._delegate.record_step(record)

    def to_jsonable(self) -> list[dict[str, Any]]:
        """Return latest per-step records suitable for compact runtime evidence."""

        return [_record_to_jsonable(record) for record in _latest_records(self.records)]


def _latest_records(records: list[LoadStepAuditRecord]) -> tuple[LoadStepAuditRecord, ...]:
    latest: dict[tuple[str, str, str, str], LoadStepAuditRecord] = {}
    for record in records:
        latest[(record.load_id, record.step_id, record.phase, record.kind)] = record
    return tuple(latest.values())


def _record_to_jsonable(record: LoadStepAuditRecord) -> dict[str, Any]:
    details = enrich_step_details_with_throughput(
        redact_public_value(record.details),
        started_at=record.started_at,
        finished_at=record.finished_at,
        status=record.status,
    )
    return {
        "run_id": record.run_id,
        "load_id": record.load_id,
        "step_id": record.step_id,
        "phase": record.phase,
        "kind": record.kind,
        "status": record.status,
        "started_at": _iso(record.started_at),
        "finished_at": _iso(record.finished_at),
        "duration_seconds": _duration_seconds(record.started_at, record.finished_at),
        "error_message": (
            redact_public_text(record.error_message, fallback="audit error unavailable")
            if record.error_message is not None
            else None
        ),
        "details_json": details,
    }


def _duration_seconds(started_at: datetime, finished_at: datetime | None) -> float | None:
    if finished_at is None:
        return None
    seconds = (finished_at - started_at).total_seconds()
    return round(seconds, 6) if seconds >= 0 else None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


__all__ = ["RuntimeLoadStepAuditCollector"]
