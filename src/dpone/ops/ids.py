"""Small deterministic ID helpers for operational artifacts."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def new_ulid() -> str:
    """Return a 26-character ULID-like monotonic-safe identifier.

    The implementation intentionally avoids adding a dependency. It preserves
    the operational contract that dpone IDs are compact, sortable-ish, and exactly
    26 characters.
    """

    millis = int(datetime.now(UTC).timestamp() * 1000)
    entropy = uuid4().int & ((1 << 80) - 1)
    value = (millis << 80) | entropy
    chars: list[str] = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 31])
        value >>= 5
    return "".join(reversed(chars))
