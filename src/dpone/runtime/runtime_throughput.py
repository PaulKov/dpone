"""Runtime throughput evidence for load steps and whole runs."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from datetime import datetime
from typing import Any

RUNTIME_THROUGHPUT_SCHEMA_VERSION = "dpone.runtime.throughput.v1"

_ROW_KEYS = (
    "rows",
    "row_count",
    "loaded_rows",
    "staged_rows",
    "inserted_rows",
    "extracted_rows",
    "written_rows",
    "decoded_rows",
    "encoded_rows",
    "native_rows_decoded",
    "source_rows",
    "target_rows",
    "total_rows",
    "replaced_rows",
)
_BYTE_KEYS = (
    "bytes",
    "byte_count",
    "bytes_processed",
    "bytes_read",
    "bytes_written",
    "source_bytes",
    "sink_bytes",
    "encoded_bytes",
    "object_bytes",
    "size_bytes",
)


def enrich_step_details_with_throughput(
    details: Mapping[str, Any] | None,
    *,
    started_at: datetime | None,
    finished_at: datetime | None,
    status: str | None,
) -> dict[str, Any]:
    """Return audit step details enriched with measured throughput when possible."""

    payload = dict(details or {})
    if "throughput" in payload:
        return payload
    if not _is_finished(status, finished_at):
        return payload

    duration_seconds = _duration_seconds(started_at, finished_at)
    if duration_seconds is None:
        return payload

    row_source, row_count = _first_numeric(payload, _ROW_KEYS)
    byte_source, byte_count = _first_numeric(payload, _BYTE_KEYS)
    if row_count is None and byte_count is None:
        return payload

    throughput: dict[str, Any] = {
        "schema_version": RUNTIME_THROUGHPUT_SCHEMA_VERSION,
        "scope": "load_step",
        "duration_seconds": duration_seconds,
        "rate_type": "counter_over_wall_clock",
    }
    if row_count is not None:
        throughput.update(
            {
                "row_count": int(row_count),
                "row_count_source": row_source,
                "rows_per_second": _rate(row_count, duration_seconds),
            }
        )
    if byte_count is not None:
        bytes_per_second = _rate(byte_count, duration_seconds)
        throughput.update(
            {
                "byte_count": int(byte_count),
                "byte_count_source": byte_source,
                "bytes_per_second": bytes_per_second,
                "mib_per_second": round(bytes_per_second / 1024 / 1024, 6),
            }
        )
    throughput["confidence"] = "measured"
    payload["throughput"] = throughput
    return payload


def enrich_run_result_with_throughput(result: MutableMapping[str, Any]) -> None:
    """Attach whole-run throughput to the mutable ETL result payload."""

    if "run_throughput" in result:
        return
    duration = _positive_float(result.get("duration_seconds"))
    if duration is None:
        return
    row_source, row_count = _first_numeric(
        result,
        ("loaded_rows", "inserted_rows", "staging_rows", "extracted_rows", "final_rows"),
    )
    if row_count is None:
        return
    result["run_throughput"] = {
        "schema_version": RUNTIME_THROUGHPUT_SCHEMA_VERSION,
        "scope": "run",
        "duration_seconds": duration,
        "rate_type": "counter_over_wall_clock",
        "row_count": int(row_count),
        "row_count_source": row_source,
        "rows_per_second": _rate(row_count, duration),
        "confidence": "measured",
    }


def _is_finished(status: str | None, finished_at: datetime | None) -> bool:
    return finished_at is not None and str(status or "").lower() not in {"", "running"}


def _duration_seconds(started_at: datetime | None, finished_at: datetime | None) -> float | None:
    if started_at is None or finished_at is None:
        return None
    seconds = (finished_at - started_at).total_seconds()
    return round(seconds, 6) if seconds > 0 else None


def _first_numeric(payload: Mapping[str, Any], keys: tuple[str, ...]) -> tuple[str | None, float | None]:
    zero_candidate: tuple[str, float] | None = None
    for key in keys:
        value = _non_negative_float(payload.get(key))
        if value is not None:
            if value > 0:
                return key, value
            if zero_candidate is None:
                zero_candidate = (key, value)
    if zero_candidate is not None:
        return zero_candidate
    return None, None


def _positive_float(value: Any) -> float | None:
    parsed = _non_negative_float(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _non_negative_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def _rate(value: float, duration_seconds: float) -> float:
    return round(float(value) / duration_seconds, 6)


__all__ = [
    "RUNTIME_THROUGHPUT_SCHEMA_VERSION",
    "enrich_run_result_with_throughput",
    "enrich_step_details_with_throughput",
]
