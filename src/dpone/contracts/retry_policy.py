"""Validation contract shared by retry-capable public entry points."""

from __future__ import annotations

import math


def validate_retry_attempts(value: object) -> int:
    """Return a non-negative retry count or reject the public input."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("retry attempts must be a non-negative integer")
    return value


def validate_retry_backoff_seconds(value: object) -> float:
    """Return a finite non-negative retry delay or reject the public input."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("retry backoff seconds must be a finite non-negative number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError("retry backoff seconds must be a finite non-negative number")
    return normalized


__all__ = ["validate_retry_attempts", "validate_retry_backoff_seconds"]
