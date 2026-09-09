"""ClickHouse RowBinary encoding for typed native transfer streams."""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import date
from datetime import time as dt_time
from decimal import Decimal
from typing import Any

from dpone.runtime.clickhouse_binary_encoding import (
    encode_clickhouse_value,
    resolve_columns,
    root_type,
    unwrap_nullable,
    validate_clickhouse_value_fidelity,
)
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy


class ClickHouseRowBinaryEncoder:
    """Encode typed rows without materializing the dataset.

    ``chunk_rows`` retains its legacy default. Optional positive byte limits cap
    each yielded batch and each complete encoded row; the smaller limit applies
    to a row. An oversized row fails before emitting any portion of that row.
    Variable payloads are preflighted before encoding allocations. Bounds cover
    encoded payload storage (including temporary copies), not source objects,
    driver buffers, retained consumer chunks, or process RSS.

    Mapping keys must exactly match the schema; positional rows must have the
    exact arity. Missing values must be supplied explicitly as ``None``.
    """

    def __init__(
        self,
        schema: Sequence[tuple[str, str]],
        *,
        chunk_rows: int = 8192,
        schema_kind: str = "mssql",
        type_policy: MssqlClickHouseTypePolicy | None = None,
        target_schema: Sequence[tuple[str, str]] | None = None,
        max_batch_bytes: int | None = None,
        max_row_bytes: int | None = None,
    ) -> None:
        self._type_policy = type_policy or MssqlClickHouseTypePolicy()
        self._columns = resolve_columns(
            schema,
            schema_kind=schema_kind,
            type_policy=type_policy,
            target_schema=target_schema,
        )
        self._chunk_rows = max(1, int(chunk_rows))
        for limit in (max_batch_bytes, max_row_bytes):
            if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0):
                raise ValueError("typed_stream_byte_limit_must_be_positive_integer")
        self._max_batch_bytes = max_batch_bytes
        limits = [limit for limit in (max_batch_bytes, max_row_bytes) if limit is not None]
        self._max_row_bytes = min(limits) if limits else None

    def iter_batches(self, rows: Iterable[Mapping[str, Any] | Sequence[Any]]) -> Iterator[bytes]:
        """Yield RowBinary chunks without materializing the full dataset."""

        buffer = bytearray()
        count = 0
        for row in rows:
            values = self._row_values(row)
            encoded_row = bytearray()
            for value, column in zip(values, self._columns, strict=True):
                if self._max_row_bytes is not None:
                    preflight_value(
                        value,
                        column.clickhouse_type,
                        column.source_type,
                        self._type_policy,
                        self._max_row_bytes - len(encoded_row),
                    )
                encoded = encode_clickhouse_value(
                    value,
                    column.clickhouse_type,
                    column.source_type,
                    self._type_policy,
                )
                if self._max_row_bytes is not None and len(encoded_row) + len(encoded) > self._max_row_bytes:
                    raise ValueError("typed_stream_row_bytes_exceeded")
                encoded_row.extend(encoded)
            if buffer and self._max_batch_bytes is not None and len(buffer) + len(encoded_row) > self._max_batch_bytes:
                yield bytes(buffer)
                buffer.clear()
                count = 0
            buffer.extend(encoded_row)
            count += 1
            if count >= self._chunk_rows:
                yield bytes(buffer)
                buffer.clear()
                count = 0
        if buffer:
            yield bytes(buffer)

    def _row_values(self, row: Mapping[str, Any] | Sequence[Any]) -> tuple[Any, ...]:
        if isinstance(row, Mapping):
            if set(row) != {column.name for column in self._columns}:
                raise ValueError("typed_stream_row_shape_mismatch")
            return tuple(row[column.name] for column in self._columns)
        if isinstance(row, str | bytes | bytearray) or len(row) != len(self._columns):
            raise ValueError("typed_stream_row_shape_mismatch")
        return tuple(row)


__all__ = ["ClickHouseRowBinaryEncoder"]


def preflight_value(
    value: Any, dtype: str, source_type: str, policy: MssqlClickHouseTypePolicy, remaining: int
) -> None:
    """Reject oversized variable payloads before allocating encoded copies.

    Custom string conversion is deliberately unavailable with byte limits because
    its allocation size cannot be inspected safely before calling user code.
    """
    nullable, inner = unwrap_nullable(dtype)
    remaining -= int(nullable)
    if value is None:
        if remaining < 0:
            raise ValueError("typed_stream_row_bytes_exceeded")
        return
    root = root_type(inner)
    if root not in {"string", "fixedstring"}:
        if not isinstance(value, int | float | Decimal | date | dt_time | uuid.UUID | str | bytes | bytearray):
            raise ValueError("typed_stream_bounded_scalar_requires_builtin_value")
        if isinstance(value, str | bytes | bytearray) and len(value) > 512:
            raise ValueError("typed_stream_scalar_representation_too_large")
        validate_clickhouse_value_fidelity(value, dtype)
        return
    if isinstance(value, str):
        if len(value) > remaining:
            raise ValueError("typed_stream_row_bytes_exceeded")
        size = 0
        for char in value:
            size += len(char.encode("utf-8"))
            if size > remaining:
                raise ValueError("typed_stream_row_bytes_exceeded")
    elif isinstance(value, bytes | bytearray):
        size = len(value)
        if any(token in source_type.lower() for token in ("binary", "rowversion", "timestamp", "image")):
            if policy.binary_encoding == "base64":
                size = 4 * ((size + 2) // 3)
            elif policy.binary_encoding == "hex":
                size *= 2
    elif isinstance(value, date | dt_time):
        # MSSQL temporal text policies produce at most 40 ASCII bytes.
        size = 0
    else:
        raise ValueError("typed_stream_bounded_string_requires_text_or_bytes")
    if root == "fixedstring":
        match = re.fullmatch(r"FixedString\((\d+)\)", inner, re.IGNORECASE)
        if match is None:
            raise ValueError("typed_stream_invalid_fixedstring")
        size = max(size, int(match[1]))
    else:
        size += max(1, (size.bit_length() + 6) // 7)
    if size > remaining:
        raise ValueError("typed_stream_row_bytes_exceeded")
