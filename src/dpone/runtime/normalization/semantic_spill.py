"""Lossless internal row codec for nested spill semantics."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import TextIO

_ROW_SCHEMA = "dpone.nested-semantic-row.v1"


def write_semantic_rows(handle: TextIO, rows: Sequence[Mapping[str, object]]) -> None:
    """Write rows with explicit scalar tags so replay preserves Python types."""

    for row in rows:
        encoded_columns = []
        for name, value in row.items():
            if not isinstance(name, str):
                raise TypeError(f"Nested semantic spill column names must be strings, got {type(name).__name__}")
            encoded_columns.append([name, _encode_value(value)])
        handle.write(
            json.dumps(
                {"schema": _ROW_SCHEMA, "columns": encoded_columns},
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
        )


def read_semantic_rows(path: Path) -> Iterator[Mapping[str, object]]:
    """Read one fail-closed typed semantic sidecar."""

    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
                yield _decode_row(payload)
            except (TypeError, ValueError, KeyError) as exc:
                raise ValueError(f"Invalid nested semantic spill row {line_number} in {path}: {exc}") from exc


def _encode_value(value: object) -> list[object]:
    if value is None:
        return ["null", None]
    if isinstance(value, bool):
        return ["boolean", value]
    if isinstance(value, int):
        return ["integer", str(value)]
    if isinstance(value, float):
        return ["number", value.hex()]
    if isinstance(value, datetime):
        return ["timestamp", value.isoformat()]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, str):
        return ["string", value]
    if isinstance(value, bytes):
        return ["bytes", value.hex()]
    if isinstance(value, list):
        return ["list", [_encode_value(item) for item in value]]
    if isinstance(value, Mapping):
        encoded_items: list[list[object]] = []
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"Nested semantic JSON object keys must be strings, got {type(key).__name__}")
            encoded_items.append([key, _encode_value(item)])
        return ["mapping", encoded_items]
    raise TypeError(f"Unsupported nested semantic spill value type {type(value).__name__}")


def _decode_row(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict) or payload.get("schema") != _ROW_SCHEMA:
        raise ValueError(f"expected schema {_ROW_SCHEMA}")
    columns = payload.get("columns")
    if not isinstance(columns, list):
        raise ValueError("columns must be a list")
    row: dict[str, object] = {}
    for item in columns:
        if not isinstance(item, list) or len(item) != 2 or not isinstance(item[0], str):
            raise ValueError("each column must be a [name, encoded_value] pair")
        name = item[0]
        if name in row:
            raise ValueError(f"duplicate column {name!r}")
        row[name] = _decode_value(item[1])
    return row


def _decode_value(encoded: object) -> object:
    if not isinstance(encoded, list) or len(encoded) != 2 or not isinstance(encoded[0], str):
        raise ValueError("encoded value must be a [type, value] pair")
    kind, value = encoded
    if kind == "null" and value is None:
        return None
    if kind == "boolean" and isinstance(value, bool):
        return value
    if kind == "integer" and isinstance(value, str):
        return int(value)
    if kind == "number" and isinstance(value, str):
        return float.fromhex(value)
    if kind == "timestamp" and isinstance(value, str):
        return datetime.fromisoformat(value)
    if kind == "date" and isinstance(value, str):
        return date.fromisoformat(value)
    if kind == "string" and isinstance(value, str):
        return value
    if kind == "bytes" and isinstance(value, str):
        return bytes.fromhex(value)
    if kind == "list" and isinstance(value, list):
        return [_decode_value(item) for item in value]
    if kind == "mapping" and isinstance(value, list):
        result: dict[str, object] = {}
        for item in value:
            if not isinstance(item, list) or len(item) != 2 or not isinstance(item[0], str):
                raise ValueError("mapping entries must be [name, encoded_value] pairs")
            if item[0] in result:
                raise ValueError(f"duplicate mapping key {item[0]!r}")
            result[item[0]] = _decode_value(item[1])
        return result
    raise ValueError(f"invalid encoded {kind!r} value")


__all__ = ["read_semantic_rows", "write_semantic_rows"]
