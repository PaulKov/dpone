"""Canonical identity digests for release-trust v2 receipts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any


def canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    """Return compact UTF-8 JSON with sorted keys (RFC 8785-compatible subset)."""

    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_hex(payload: Mapping[str, Any]) -> str:
    """SHA-256 hex digest of canonical JSON bytes."""

    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def sha256_id(domain: str, payload: Mapping[str, Any]) -> str:
    """Domain-separated content identity as ``sha256:<hex>``."""

    if not isinstance(domain, str) or not domain.strip():
        raise ValueError("domain must be a non-empty string")
    wrapped = {"domain": domain, "payload": dict(payload)}
    return f"sha256:{sha256_hex(wrapped)}"
