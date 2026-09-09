"""Bounded decoding of untrusted Kubernetes metadata scalar values."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

_MAX_MAPPING_ITEMS = 64
_MAX_KEY_BYTES = 253
_MAX_VALUE_BYTES = 4 * 1024


def metadata_timestamp(metadata: Mapping[str, object], key: str) -> datetime | None:
    value = metadata.get(key)
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("metadata timestamp must be text")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("metadata timestamp is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("metadata timestamp must include a timezone")
    return parsed


def string_mapping(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    if len(value) > _MAX_MAPPING_ITEMS:
        raise ValueError("metadata mapping exceeds item capacity")
    result: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str):
            continue
        if len(key.encode("utf-8")) > _MAX_KEY_BYTES or len(item.encode("utf-8")) > _MAX_VALUE_BYTES:
            raise ValueError("metadata mapping exceeds byte capacity")
        result[key] = item
    return result


def bounded_text(value: object, *, max_bytes: int) -> str:
    if not isinstance(value, str):
        return ""
    if len(value.encode("utf-8")) > max_bytes:
        raise ValueError("metadata text exceeds byte capacity")
    return value


__all__ = ["bounded_text", "metadata_timestamp", "string_mapping"]
