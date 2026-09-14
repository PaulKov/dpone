"""Canonical character-wire decoding shared by validation and bounded consumers.

Raw empty cells mean NULL; quotes and backslashes have no framing role. Optional
record limits count every wire byte, including LF. Existing producer callers omit
the limit to preserve their historical admission behavior.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import BinaryIO

from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec, is_bulk_text_type


class BulkTextFileReadError(ValueError):
    """Wire failure translated by each caller into its own public error boundary."""

    def __init__(self, blocker: str) -> None:
        self.blocker = blocker
        super().__init__(blocker)


def iter_wire_rows(
    stream: BinaryIO,
    width: int,
    codec: BulkTextCodec,
    *,
    max_record_bytes: int | None = None,
) -> Iterator[tuple[bytes, ...]]:
    """Read exact LF records, retaining empty cells and validating row arity."""
    if max_record_bytes is not None and (type(max_record_bytes) is not int or max_record_bytes <= 0):
        raise ValueError("max_record_bytes must be a positive integer or None")
    terminator = codec.row_terminator.encode("utf-8")
    separator = codec.field_terminator.encode("utf-8")
    while raw := stream.readline(-1 if max_record_bytes is None else max_record_bytes + 1):
        if max_record_bytes is not None and len(raw) > max_record_bytes:
            raise BulkTextFileReadError("record_limit")
        if not raw.endswith(terminator):
            raise BulkTextFileReadError("row_terminator_mismatch")
        values = tuple(raw.removesuffix(terminator).split(separator))
        if len(values) != width:
            raise BulkTextFileReadError("row_width_mismatch")
        yield values


def decode_wire_value(raw: bytes, *, dtype: str, codec: BulkTextCodec) -> object:
    """Decode one cell using the producer's exact text and binary grammar."""
    if raw == b"":
        return None
    try:
        value = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BulkTextFileReadError("utf8_invalid") from exc
    normalized = str(dtype).strip().lower().split("(", 1)[0]
    if normalized in {"binary", "varbinary", "image"}:
        try:
            return bytes.fromhex(codec.decode(value))
        except ValueError as exc:
            raise BulkTextFileReadError("binary_hex_invalid") from exc
    return codec.decode(value) if is_bulk_text_type(dtype) else value


def iter_rows(
    stream: BinaryIO,
    schema: tuple[tuple[str, str], ...],
    codec: BulkTextCodec,
    *,
    max_record_bytes: int | None = None,
) -> Iterator[tuple[object, ...]]:
    """Yield ordered logical values without CSV, trimming or normalization."""
    for cells in iter_wire_rows(stream, len(schema), codec, max_record_bytes=max_record_bytes):
        yield tuple(
            decode_wire_value(cell, dtype=dtype, codec=codec)
            for cell, (_name, dtype) in zip(cells, schema, strict=True)
        )
