"""Shared redaction predicates for safe sample diagnostic text."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from dpone.security_redaction import redact_absolute_paths, redact_text

_ASSIGNMENT_KEY_RE = re.compile(
    r"(?:^|[^a-z0-9_])['\"]?([a-z0-9_.-]+)['\"]?\s*[:=]",
    re.IGNORECASE,
)
_DIRECT_SENSITIVE_TOKENS = frozenset({"password", "passwd", "pwd", "secret", "token", "jwt", "vault"})
_EXACT_SENSITIVE_KEYS = frozenset(
    {
        "access_key",
        "api_key",
        "client_secret",
        "connection_string",
        "lease_id",
        "private_key",
        "secret_key",
    }
)
_SENSITIVE_KEY_TOKEN_PAIRS = (
    frozenset({"access", "key"}),
    frozenset({"api", "key"}),
    frozenset({"connection", "string"}),
    frozenset({"private", "key"}),
    frozenset({"secret", "key"}),
)
_REDACTED_DETAILS = "details redacted; check runtime adapter logs and safe sample input configuration."


def contains_sensitive_assignment(message: str) -> bool:
    """Return true when text appears to contain an inline secret assignment.

    Safe diagnostic identifiers such as ``vault_kv`` or ``Vault resolver`` are
    intentionally preserved. Inline values like ``token=...`` or
    ``api_key: ...`` are redacted by callers before they reach logs or evidence.
    """

    return any(is_sensitive_key_name(match.group(1)) for match in _ASSIGNMENT_KEY_RE.finditer(message))


def is_sensitive_key_name(key: str) -> bool:
    """Return true when a structured diagnostic key can hold secret material."""

    normalized = _normalize_key(key)
    if not normalized:
        return False
    if normalized in _EXACT_SENSITIVE_KEYS or normalized.endswith("_lease_id"):
        return True
    tokens = tuple(token for token in normalized.split("_") if token)
    if any(token in _DIRECT_SENSITIVE_TOKENS for token in tokens):
        return True
    token_set = frozenset(tokens)
    return any(pair.issubset(token_set) for pair in _SENSITIVE_KEY_TOKEN_PAIRS)


def redact_safe_sample_text(message: str) -> str:
    """Redact diagnostic text while preserving established assignment behavior."""

    if contains_sensitive_assignment(message):
        return _REDACTED_DETAILS
    return redact_absolute_paths(redact_text(message))


def redact_safe_sample_value(value: Any) -> Any:
    """Recursively redact mappings, lists, and diagnostic text."""

    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for key, raw_value in value.items():
            key_text = str(key)
            safe_presence_value = isinstance(raw_value, bool) or raw_value is None
            if is_sensitive_key_name(key_text) and not safe_presence_value:
                continue
            redacted[redact_text(key_text)] = redact_safe_sample_value(raw_value)
        return redacted
    if isinstance(value, list):
        return [redact_safe_sample_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_safe_sample_value(item) for item in value)
    if isinstance(value, str):
        return redact_safe_sample_text(value)
    return value


def _normalize_key(key: str) -> str:
    text = key.strip().strip("'\"").lower()
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


__all__ = [
    "contains_sensitive_assignment",
    "is_sensitive_key_name",
    "redact_safe_sample_text",
    "redact_safe_sample_value",
]
