"""Bounded progress and ETA projection for durable backfill ledgers."""

from __future__ import annotations

from datetime import UTC, datetime  # type: ignore[attr-defined]
from typing import Any

from dpone.backfill.state import (
    CHUNK_STATUS_FAILED,
    CHUNK_STATUS_PENDING,
    CHUNK_STATUS_RUNNING,
    CHUNK_STATUS_SUCCESS,
    BackfillLedger,
)


def backfill_progress(ledger: BackfillLedger, *, now: datetime | None = None) -> dict[str, Any]:
    """Project aggregate counters, rate and ETA without per-chunk payloads."""

    counts = ledger.counts()
    committed_records = [record for record in ledger.chunks if record.status == CHUNK_STATUS_SUCCESS]
    rows_extracted = sum(record.rows_extracted for record in committed_records)
    rows_loaded = sum(record.rows_loaded for record in committed_records)
    started = _minimum_time(
        record.started_at for record in ledger.chunks if record.started_at is not None
    ) or _parse_time(ledger.created_at)
    terminal = counts[CHUNK_STATUS_SUCCESS] == len(ledger.chunks)
    finished = _maximum_time(record.finished_at for record in committed_records if record.finished_at is not None)
    end = finished if terminal and finished is not None else (now or datetime.now(UTC))
    elapsed = max(0.0, (end - started).total_seconds()) if started is not None else 0.0
    rate = rows_loaded / elapsed if elapsed > 0 and rows_loaded > 0 else 0.0
    committed = counts[CHUNK_STATUS_SUCCESS]
    remaining = max(0, len(ledger.chunks) - committed)
    estimated_remaining_rows = (rows_loaded / committed * remaining) if committed > 0 else 0.0
    eta = estimated_remaining_rows / rate if rate > 0 and remaining > 0 else 0.0
    return {
        "pending": counts[CHUNK_STATUS_PENDING],
        "running": counts[CHUNK_STATUS_RUNNING],
        "committed": committed,
        "failed": counts[CHUNK_STATUS_FAILED],
        "rows_extracted": rows_extracted,
        "rows_loaded": rows_loaded,
        "rows_per_second": round(rate, 3),
        "eta_seconds": round(eta, 3),
        "elapsed_seconds": round(elapsed, 3),
        "last_transition_at": _latest_transition(ledger),
    }


def _latest_transition(ledger: BackfillLedger) -> str | None:
    values = [ledger.updated_at]
    for record in ledger.chunks:
        values.extend((record.updated_at, record.finished_at, record.started_at))
    latest = _maximum_time(value for value in values if value is not None)
    return latest.isoformat() if latest is not None else None


def _minimum_time(values: Any) -> datetime | None:
    parsed = [value for value in (_parse_time(item) for item in values) if value is not None]
    return min(parsed) if parsed else None


def _maximum_time(values: Any) -> datetime | None:
    parsed = [value for value in (_parse_time(item) for item in values) if value is not None]
    return max(parsed) if parsed else None


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


__all__ = ["backfill_progress"]
