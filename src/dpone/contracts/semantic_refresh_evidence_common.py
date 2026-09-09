"""Small validation primitives shared by semantic-refresh evidence contracts."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_closed_mapping,
    require_digest,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
)


def require_nonnegative_int(value: object, field: str) -> int:
    """Return a non-negative integer, rejecting booleans."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SemanticRefreshContractError(f"{field} must be a non-negative integer")
    return value


def require_utc_timestamp(value: object, field: str) -> str:
    """Return one RFC3339 timestamp expressed canonically with a UTC ``Z`` suffix."""

    text = require_text(value, field)
    if not text.endswith("Z"):
        raise SemanticRefreshContractError(f"{field} must be an RFC3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise SemanticRefreshContractError(f"{field} must be an RFC3339 UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise SemanticRefreshContractError(f"{field} must be an RFC3339 UTC timestamp")
    return text


def parse_utc_timestamp(value: object, field: str) -> datetime:
    """Parse a validated UTC timestamp for interval comparison."""

    return datetime.fromisoformat(require_utc_timestamp(value, field)[:-1] + "+00:00").astimezone(
        timezone.utc  # noqa: UP017
    )


def require_utc_interval(start: object, end: object, start_field: str, end_field: str) -> None:
    """Require a non-empty half-open UTC interval."""

    if parse_utc_timestamp(start, start_field) >= parse_utc_timestamp(end, end_field):
        raise SemanticRefreshContractError(f"{start_field} must precede {end_field}")


def require_uuid(value: object, field: str) -> str:
    """Return a canonical lowercase UUID."""

    text = require_text(value, field)
    try:
        canonical = str(uuid.UUID(text))
    except ValueError as exc:
        raise SemanticRefreshContractError(f"{field} must be a canonical UUID") from exc
    if text != canonical:
        raise SemanticRefreshContractError(f"{field} must be a canonical UUID")
    return text


__all__ = [
    "parse_utc_timestamp",
    "SemanticRefreshContractError",
    "require_closed_mapping",
    "require_digest",
    "require_nonnegative_int",
    "require_positive_int",
    "require_text",
    "require_utc_interval",
    "require_utc_timestamp",
    "require_uuid",
    "semantic_refresh_sha256",
]
