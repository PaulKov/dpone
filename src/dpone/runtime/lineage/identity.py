"""ID generation for dpone load and row lineage."""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from collections.abc import Mapping, Sequence
from typing import Any

_CROCKFORD_BASE32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


class LineageIdentityService:
    """Generate stable load identifiers and deterministic row lineage hashes."""

    def new_run_id(self) -> str:
        return self._new_ulid()

    def new_load_id(self) -> str:
        return self._new_ulid()

    def row_id(
        self,
        *,
        source_type: str,
        source_schema: str,
        source_table: str,
        row: Mapping[str, object],
        unique_key: str | Sequence[str] | None = None,
        row_path: str | None = None,
    ) -> str:
        identity = self._row_identity_payload(
            source_type=source_type,
            source_schema=source_schema,
            source_table=source_table,
            row=row,
            unique_key=unique_key,
            row_path=row_path,
        )
        return hashlib.sha256(_stable_json(identity).encode("utf-8")).hexdigest()

    def row_hash(
        self,
        row: Mapping[str, object],
        *,
        exclude_columns: Sequence[str] = (),
    ) -> str:
        excluded = {column.lower() for column in exclude_columns}
        payload = {str(key): value for key, value in row.items() if str(key).lower() not in excluded}
        return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()

    def _new_ulid(self) -> str:
        timestamp_ms = int(time.time() * 1000)
        random_value = secrets.randbits(80)
        value = (timestamp_ms << 80) | random_value
        return _encode_base32(value, length=26)

    def _row_identity_payload(
        self,
        *,
        source_type: str,
        source_schema: str,
        source_table: str,
        row: Mapping[str, object],
        unique_key: str | Sequence[str] | None,
        row_path: str | None,
    ) -> dict[str, object]:
        key_columns = _normalize_unique_key(unique_key)
        if key_columns:
            key_values = {column: row.get(column) for column in key_columns}
            return {
                "source_type": source_type,
                "source_schema": source_schema,
                "source_table": source_table,
                "key": key_values,
            }
        return {
            "source_type": source_type,
            "source_schema": source_schema,
            "source_table": source_table,
            "row_path": row_path,
            "row": dict(row),
        }


def _encode_base32(value: int, *, length: int) -> str:
    chars: list[str] = []
    for _ in range(length):
        chars.append(_CROCKFORD_BASE32[value & 31])
        value >>= 5
    return "".join(reversed(chars))


def _normalize_unique_key(unique_key: str | Sequence[str] | None) -> tuple[str, ...]:
    if unique_key is None:
        return ()
    if isinstance(unique_key, str):
        return (unique_key,)
    return tuple(str(column) for column in unique_key)


def _stable_json(value: Any) -> str:
    rendered = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False)
    type_markers = _non_json_type_markers(value)
    if not type_markers:
        return rendered
    type_markers.sort(key=lambda marker: json.dumps(marker, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    markers = json.dumps(type_markers, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return f"{rendered}\x1e{markers}"


def _non_json_type_markers(value: Any, *, path: tuple[str | int, ...] = ()) -> list[dict[str, object]]:
    """Describe values whose Python type is erased by ``json.dumps(default=str)``."""

    if type(value) in (type(None), bool, int, float, str):
        return []
    if isinstance(value, Mapping):
        markers: list[dict[str, object]] = []
        for key, item in value.items():
            markers.extend(_non_json_type_markers(item, path=(*path, str(key))))
        return markers
    if isinstance(value, list):
        markers = []
        for index, item in enumerate(value):
            markers.extend(_non_json_type_markers(item, path=(*path, index)))
        return markers
    if isinstance(value, tuple):
        markers = [_type_marker(value, path)]
        for index, item in enumerate(value):
            markers.extend(_non_json_type_markers(item, path=(*path, index)))
        return markers
    return [_type_marker(value, path)]


def _type_marker(value: Any, path: tuple[str | int, ...]) -> dict[str, object]:
    value_type = type(value)
    return {
        "path": list(path),
        "python_type": f"{value_type.__module__}.{value_type.__qualname__}",
    }
