"""Canonical identity primitives for semantic-refresh ClickHouse records."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")


def require_digest(value: object, field_name: str) -> None:
    """Require one canonical lowercase SHA-256 digest."""

    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a canonical sha256 digest")


def require_uuid(value: object, field_name: str) -> None:
    """Require one canonical UUID text value."""

    try:
        parsed = uuid.UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        parsed = None
    if parsed is None or str(parsed) != value:
        raise ValueError(f"{field_name} must be a canonical UUID")


def semantic_refresh_fingerprint(payload: Mapping[str, object]) -> str:
    """Hash one closed JSON-like publication payload deterministically."""

    raw = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()
