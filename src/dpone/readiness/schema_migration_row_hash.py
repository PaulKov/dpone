"""Provider-neutral typed row hashing for schema migration evidence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any


def typed_rows_hash(rows: Sequence[Mapping[str, Any]], key_columns: Sequence[str] = ()) -> str:
    """Return a stable type-aware hash for JSON-like rows."""

    normalized = [_typed_row(_row(row)) for row in rows]
    sort_key = _sort_key_factory(tuple(key_columns))
    return _stable_fingerprint(sorted(normalized, key=sort_key))


def _row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _jsonable(value) for key, value in row.items()}


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _typed_row(row: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {key: {"type": type(value).__name__, "value": value} for key, value in sorted(row.items())}


def _sort_key_factory(key_columns: Sequence[str]):
    def sort_key(row: Mapping[str, Any]) -> str:
        if key_columns:
            key_payload = {column: row.get(column) for column in key_columns}
            return _json_key(key_payload)
        return _json_key(row)

    return sort_key


def _json_key(value: Any) -> str:
    return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _stable_fingerprint(payload: Any) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()


__all__ = ["typed_rows_hash"]
