"""Opt-in encoded payload limits, excluding source/driver allocations and RSS.

A row must fit in one batch. Encoder-owned payload storage is bounded by a
constant multiple of batch bytes (batch, immutable yield copy, and current row).
Consumers retaining chunks and ODBC fetch buffers require separate budgets.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.runtime.storage_policy import parse_byte_size


@dataclass(frozen=True)
class TypedStreamLimits:
    """Validated limits using existing native-transfer wire configuration."""

    chunk_rows: int
    max_batch_bytes: int | None = None

    @classmethod
    def from_options(cls, options: Mapping[str, Any], batch_size: int) -> TypedStreamLimits:
        native = options.get("native_transfer", {})
        wire = native.get("wire", {}) if isinstance(native, Mapping) else {}
        if not isinstance(wire, Mapping):
            raise ValueError("typed_stream_wire_must_be_mapping")
        rows = wire.get("block_rows", batch_size)
        if isinstance(rows, bool) or not isinstance(rows, int) or rows <= 0:
            raise ValueError("typed_stream_block_rows_must_be_positive_integer")
        raw = wire.get("block_bytes")
        limit = None
        if raw is not None:
            if isinstance(raw, bool) or not re.fullmatch(r"\d+(?:\.\d+)?\s*[A-Za-z]*", str(raw).strip()):
                raise ValueError("typed_stream_block_bytes_must_be_positive_size")
            try:
                limit = parse_byte_size(raw)
            except OverflowError as error:
                raise ValueError("typed_stream_block_bytes_must_be_finite") from error
            if limit <= 0:
                raise ValueError("typed_stream_block_bytes_must_be_positive_size")
        return cls(min(max(1, batch_size), rows), limit)
