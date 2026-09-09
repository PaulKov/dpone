"""Secret redaction helpers for Airflow self-service diagnostic payloads."""

from __future__ import annotations

from typing import Any

from dpone.readiness.airflow_connection_check_support import FORBIDDEN_SECRET_KEYS


def is_secret_key(key: str) -> bool:
    """Return true when a diagnostic key name should never expose its value."""

    normalized = key.lower()
    return normalized in FORBIDDEN_SECRET_KEYS or any(marker in normalized for marker in FORBIDDEN_SECRET_KEYS)


def redacted_mapping(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a copy with secret-like values replaced by a placeholder."""

    redacted = _redacted_value(payload)
    return redacted if isinstance(redacted, dict) else {}


def without_secret_keys(payload: Any) -> Any:
    """Return a copy with secret-like keys removed recursively."""

    return _without_secret_keys(payload)


def _redacted_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if is_secret_key(str(key)) else _redacted_value(item) for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redacted_value(item) for item in value]
    return value


def _without_secret_keys(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if is_secret_key(key_text):
                continue
            stripped = _without_secret_keys(item)
            if stripped in ({}, []):
                continue
            cleaned[key_text] = stripped
        return cleaned
    if isinstance(value, list):
        return [item for item in (_without_secret_keys(item) for item in value) if item not in ({}, [])]
    return value


__all__ = ["is_secret_key", "redacted_mapping", "without_secret_keys"]
