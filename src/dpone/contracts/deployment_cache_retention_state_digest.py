"""Primitive canonical identities shared by retention state contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping


class RetentionStateIdentityError(ValueError):
    """A retention state identity is malformed."""


def canonical_digest(value: Mapping[str, object]) -> str:
    body = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(body).hexdigest()


def require_digest(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) != 71 or not value.startswith("sha256:"):
        raise RetentionStateIdentityError(f"{label} must be a canonical sha256 digest")
    if any(character not in "0123456789abcdef" for character in value[7:]):
        raise RetentionStateIdentityError(f"{label} must be a canonical sha256 digest")
    return value


__all__ = ["RetentionStateIdentityError", "canonical_digest", "require_digest"]
