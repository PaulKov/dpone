"""Canonical PostgreSQL key-snapshot schema derivation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def resolve_snapshot_unique_key(load_config: Any) -> tuple[str, ...]:
    """Return the exact configured reconciliation key or fail closed."""

    raw = getattr(load_config, "unique_key", None)
    keys = (raw,) if isinstance(raw, str) else tuple(raw or ())
    if not keys or any(not str(item).strip() for item in keys):
        raise ValueError("key_snapshot reconciliation requires a non-empty unique_key")
    return tuple(str(item) for item in keys)


def project_snapshot_key_schema(
    source_schema: Sequence[tuple[str, str]],
    key_columns: Sequence[str],
) -> list[tuple[str, str]]:
    """Project exact keys from a complete snapshot-visible source schema."""

    normalized_source = tuple((str(name), str(dtype)) for name, dtype in source_schema)
    names = tuple(name for name, _dtype in normalized_source)
    if len({name.casefold() for name in names}) != len(names):
        raise ValueError("key_snapshot source schema contains ambiguous column identities")
    by_name = dict(normalized_source)
    keys = tuple(str(value) for value in key_columns)
    if len({key.casefold() for key in keys}) != len(keys):
        raise ValueError("key_snapshot unique_key contains duplicate column identities")
    missing = tuple(key for key in keys if key not in by_name)
    if missing:
        raise ValueError("key_snapshot unique_key is absent from source schema")
    return [(key, by_name[key]) for key in keys]


__all__ = ["project_snapshot_key_schema", "resolve_snapshot_unique_key"]
