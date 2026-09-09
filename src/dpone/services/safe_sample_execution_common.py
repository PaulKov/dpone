"""Shared safe-sample execution value objects and evidence helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.credential_resolution import CredentialResolutionError
from dpone.services.safe_sample_redaction import contains_sensitive_assignment, is_sensitive_key_name

_REDACTED_MESSAGE = "details redacted; check runtime adapter diagnostics and safe sample configuration."


@dataclass(frozen=True, slots=True)
class MssqlSafeSampleBatch:
    """In-memory bounded sample batch passed from source reader to sink writer."""

    rows: tuple[Mapping[str, Any], ...]
    rows_read: int
    bytes_read: int
    diagnostics: Mapping[str, Any] | None = None


def redact_mapping(value: Any) -> dict[str, Any]:
    """Return a copy of mapping-like diagnostics without secret-like keys."""

    if not isinstance(value, Mapping):
        return {}
    redacted = _redact_value(value)
    return redacted if isinstance(redacted, dict) else {}


def _redact_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _redact_mapping(value)
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    if isinstance(value, str) and _contains_sensitive_token(value):
        return _REDACTED_MESSAGE
    return value


def _redact_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for key, raw_value in value.items():
        key_text = str(key)
        if _is_sensitive_key(key_text):
            continue
        redacted[key_text] = _redact_value(raw_value)
    return redacted


def non_negative_int(value: Any) -> int:
    """Coerce a metric value into a non-negative integer."""

    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def credential_resolution_error(value: BaseException) -> tuple[str, str] | None:
    """Return the stable safe projection of one typed credential failure."""

    if not isinstance(value, CredentialResolutionError):
        return None
    return value.code, str(value)


def _is_sensitive_key(key: str) -> bool:
    return is_sensitive_key_name(key)


def _contains_sensitive_token(message: str) -> bool:
    return contains_sensitive_assignment(message)


__all__ = ["MssqlSafeSampleBatch", "credential_resolution_error", "non_negative_int", "redact_mapping"]
