"""Pure parsing and selection helpers for source materialization policy."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.runtime.storage_policy import parse_byte_size


def materialization_mapping(source_options: Mapping[str, Any] | None) -> Mapping[str, Any]:
    native = mapping_value((source_options or {}).get("native_transfer"))
    snapshot = mapping_value(native.get("snapshot"))
    return mapping_value(snapshot.get("materialization"))


def mapping_value(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def text_value(value: Any, default: str) -> str:
    return str(value if value is not None else default).strip().lower()


def optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def bool_value(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def int_value(value: Any, default: int) -> int:
    return int(value if value is not None else default)


def optional_bytes(value: Any) -> int | None:
    return None if value is None else parse_byte_size(value)


def column_names(value: Any) -> tuple[str, ...]:
    if value in (None, "auto"):
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence):
        return tuple(str(item) for item in value)
    return ()


def speedup_pct(current: float | None, materialized: float | None) -> float | None:
    if current is None or materialized is None or current <= 0:
        return None
    return round(((materialized - current) / current) * 100, 2)


def source_shape_benefits(source_shape: Any) -> bool:
    table_kind = str(getattr(source_shape, "table_kind", "unknown")).lower()
    low_confidence = bool(getattr(source_shape, "low_confidence", False))
    return table_kind in {"heap", "view", "unknown"} or low_confidence
