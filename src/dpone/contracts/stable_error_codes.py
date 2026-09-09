"""Stable public error-code recognition shared by runtime adapters."""

from __future__ import annotations

import re

COMMIT_UNKNOWN_CODE = "COMMIT_UNKNOWN"
_STABLE_ERROR_CODE_RE = re.compile(r"\b(?:DPONE_[A-Z0-9_]+|COMMIT_UNKNOWN)\b")


def is_stable_error_code(value: object) -> bool:
    """Return whether ``value`` is a bounded public machine-readable code."""

    return isinstance(value, str) and _STABLE_ERROR_CODE_RE.fullmatch(value) is not None


def stable_error_code_from_text(value: object) -> str | None:
    """Extract the first stable public code from bounded diagnostic text."""

    if not isinstance(value, str):
        return None
    match = _STABLE_ERROR_CODE_RE.search(value)
    return match.group(0) if match is not None else None


__all__ = [
    "COMMIT_UNKNOWN_CODE",
    "is_stable_error_code",
    "stable_error_code_from_text",
]
