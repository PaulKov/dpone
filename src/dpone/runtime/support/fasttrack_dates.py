from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}


def _parse_boolish(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TRUTHY | {"enabled"}:
            return True
        if normalized in _FALSY | {"disabled"}:
            return False
        if normalized == "":
            return None
    return bool(value)


def fasttrack_parse_temporal_fields_default() -> bool:
    parsed = _parse_boolish(os.getenv("DPONE_PARSE_TEMPORAL_FIELDS_DEFAULT"))
    return True if parsed is None else parsed


def resolve_fasttrack_parse_temporal_fields(options: Mapping[str, Any] | None) -> bool:
    if isinstance(options, Mapping) and "parse_temporal_fields" in options:
        parsed = _parse_boolish(options.get("parse_temporal_fields"))
        if parsed is not None:
            return parsed
    return fasttrack_parse_temporal_fields_default()


def parse_fasttrack_datetime(value: Any, fmt: str | None = None) -> Any:
    """Best-effort parser for Fasttrack datetime strings.

    Landing stays ELT-oriented: we only parse datetime-like fields for better
    target typing/performance and keep the rest of the payload intact.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text == "":
            return None
        dt = None
        if fmt:
            try:
                dt = datetime.strptime(text, fmt)
            except ValueError:
                dt = None
        if dt is None:
            iso_text = text.replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(iso_text)
            except ValueError:
                dt = None
        if dt is None:
            # fallback for common dashboard style without timezone
            for fallback in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
            ):
                try:
                    dt = datetime.strptime(text, fallback)
                    break
                except ValueError:
                    continue
        if dt is None:
            return value

    if dt.tzinfo is not None:
        return dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def parse_fasttrack_record_dates(record: Mapping[str, Any], date_fields: Mapping[str, str | None]) -> dict[str, Any]:
    normalized = dict(record)
    for column, fmt in date_fields.items():
        if column in normalized:
            normalized[column] = parse_fasttrack_datetime(normalized.get(column), fmt=fmt)
    return normalized


__all__ = [
    "fasttrack_parse_temporal_fields_default",
    "parse_fasttrack_datetime",
    "parse_fasttrack_record_dates",
    "resolve_fasttrack_parse_temporal_fields",
]
