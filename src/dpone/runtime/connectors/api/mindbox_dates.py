from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any


def build_mindbox_date_body(
    since: date | datetime | None = None,
    till: date | datetime | None = None,
    window_mode: str = "datetime_utc",
    utc_boundary_time: str = "21:00:00",
) -> dict[str, Any]:
    body: dict[str, Any] = {}
    normalized_mode = str(window_mode).strip().lower() or "datetime_utc"
    legacy_supports_till = normalized_mode != "since_only"
    if normalized_mode in {"since_only", "since_till"}:
        normalized_mode = "datetime_utc"

    if normalized_mode == "none":
        return body
    if normalized_mode == "date_project":
        if since is not None:
            body["sinceDate"] = _format_project_date(since, utc_boundary_time)
        if till is not None:
            body["tillDate"] = _format_project_date(till, utc_boundary_time)
        return body

    if since is not None:
        body["sinceDateTimeUtc"] = _format_datetime_utc(since, utc_boundary_time)
    if till is not None and legacy_supports_till:
        body["tillDateTimeUtc"] = _format_datetime_utc(till, utc_boundary_time)
    return body


def _normalize_utc_naive(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value.replace(tzinfo=None)


def _format_datetime_utc(value: date | datetime, utc_boundary_time: str) -> str:
    if isinstance(value, datetime):
        return _normalize_utc_naive(value).strftime("%Y-%m-%d %H:%M")
    return f"{value.strftime('%Y-%m-%d')} {utc_boundary_time}"


def _project_offset(utc_boundary_time: str) -> timedelta:
    try:
        hours, minutes, seconds = (int(part) for part in str(utc_boundary_time).split(":"))
    except Exception:
        hours, minutes, seconds = 21, 0, 0
    boundary_seconds = hours * 3600 + minutes * 60 + seconds
    return timedelta(seconds=(-boundary_seconds) % 86_400)


def _format_project_date(value: date | datetime, utc_boundary_time: str) -> str:
    if isinstance(value, datetime):
        shifted = _normalize_utc_naive(value) + _project_offset(utc_boundary_time)
        return shifted.date().isoformat()
    return value.isoformat()
