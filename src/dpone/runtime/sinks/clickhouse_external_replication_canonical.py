"""Canonical schema and row encoding for external ClickHouse generations."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

from dpone.ports.clickhouse_external_replication import ExternalContractError, canonical_json

_CONTENT_DIGEST_VERSION = "dpone.clickhouse.canonical-rows.v1"
_SCHEMA_DIGEST_VERSION = "dpone.clickhouse.canonical-schema.v1"


def canonical_schema_digest(columns: Sequence[Sequence[Any]]) -> str:
    """Digest ordered ClickHouse catalog columns without exposing their values."""

    normalized = []
    for column in columns:
        if len(column) < 5:
            raise ExternalContractError("GENERATION_UNKNOWN", "schema catalog row is incomplete")
        normalized.append(
            {
                "name": str(column[0]),
                "type": str(column[1]),
                "default_kind": str(column[2] or ""),
                "default_expression": str(column[3] or ""),
                "position": int(column[4]),
            }
        )
    payload = {"version": _SCHEMA_DIGEST_VERSION, "columns": normalized}
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def canonical_rows_digest(rows: Iterable[Sequence[Any]]) -> str:
    """Digest a typed row multiset in deterministic canonical order."""

    digest = hashlib.sha256()
    digest.update((_CONTENT_DIGEST_VERSION + "\n").encode("utf-8"))
    encoded_rows = sorted(canonical_json([_canonical_value(value) for value in row]) for row in rows)
    for encoded in encoded_rows:
        digest.update(str(len(encoded)).encode("ascii"))
        digest.update(b":")
        digest.update(encoded.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def canonical_rows_json(rows: Iterable[Sequence[Any]]) -> str:
    """Serialize typed rows canonically while preserving artifact row order."""

    return canonical_json([[_canonical_value(value) for value in row] for row in rows])


def _canonical_value(value: Any) -> Any:
    if value is None:
        return ["null", None]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", str(value)]
    if isinstance(value, Decimal):
        return ["decimal", format(value, "f")]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ExternalContractError("GENERATION_UNKNOWN", "non-finite values are unsupported")
        return ["float", value.hex()]
    if isinstance(value, str):
        return ["string", value]
    if isinstance(value, bytes | bytearray | memoryview):
        return ["bytes", bytes(value).hex()]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat(timespec="microseconds")]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, time):
        return ["time", value.isoformat(timespec="microseconds")]
    if isinstance(value, UUID):
        return ["uuid", str(value)]
    if isinstance(value, tuple | list):
        return ["sequence", [_canonical_value(item) for item in value]]
    if isinstance(value, Mapping):
        items = [(_canonical_value(key), _canonical_value(item)) for key, item in value.items()]
        items.sort(key=lambda pair: canonical_json(pair[0]))
        return ["mapping", [[key, item] for key, item in items]]
    raise ExternalContractError("GENERATION_UNKNOWN", "canonical value type is unsupported")


__all__ = ["canonical_rows_digest", "canonical_rows_json", "canonical_schema_digest"]
