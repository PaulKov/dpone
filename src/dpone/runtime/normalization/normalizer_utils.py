"""Small helpers for nested normalization orchestration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

_TECHNICAL_PREFIX = "__dpone__"


def ensure_no_framework_columns(value: Mapping[str, object]) -> None:
    for key, nested in value.items():
        if str(key).startswith(_TECHNICAL_PREFIX):
            raise ValueError(f"Source column `{key}` uses reserved dpone technical namespace `{_TECHNICAL_PREFIX}`")
        if isinstance(nested, Mapping):
            ensure_no_framework_columns(nested)
        elif isinstance(nested, list):
            for item in nested:
                if isinstance(item, Mapping):
                    ensure_no_framework_columns(item)


def is_nested(value: object) -> bool:
    return isinstance(value, (Mapping, list))


def join_path(prefix: str, field: str) -> str:
    return f"{prefix}.{field}" if prefix else field


def join_table(parent_table: str, field: str, separator: str) -> str:
    safe_field = "".join(char if char.isalnum() or char == "_" else "_" for char in field).strip("_") or "nested"
    return f"{parent_table}{separator}{safe_field}"


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def identity_row(
    row: Mapping[str, object], root_source_row: Mapping[str, object], unique_key: Sequence[str]
) -> Mapping[str, object]:
    if not unique_key:
        return row
    return {key: row.get(key, root_source_row.get(key)) for key in unique_key}


def with_identity_columns(
    row: Mapping[str, object], root_source_row: Mapping[str, object], unique_key: Sequence[str]
) -> dict[str, object]:
    output = dict(row)
    for key in unique_key:
        if key not in output and key in root_source_row:
            output[key] = root_source_row[key]
    return output
