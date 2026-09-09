"""Reusable argparse value adapters for public CLI contracts."""

from __future__ import annotations

import argparse

from dpone.contracts.retry_policy import (
    validate_retry_attempts,
    validate_retry_backoff_seconds,
)


def non_negative_int(raw: str) -> int:
    """Parse a non-negative integer with an actionable argparse diagnostic."""

    try:
        value = int(raw)
        return validate_retry_attempts(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "must be a non-negative integer",
        ) from exc


def non_negative_float(raw: str) -> float:
    """Parse a finite non-negative number with an actionable diagnostic."""

    try:
        value = float(raw)
        return validate_retry_backoff_seconds(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "must be a finite non-negative number",
        ) from exc


__all__ = ["non_negative_float", "non_negative_int"]
