"""Lossless ClickHouse driver values adapted to the admitted native target types."""

from __future__ import annotations

from collections.abc import Generator, Iterator, Mapping
from contextlib import closing
from typing import Any

from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.mssql_native_row_reservation import _SizedNativeRow


def native_source_rows(rows: Iterator[Any], contract: Any, max_row_bytes: int) -> Iterator[Any]:
    """Decode declared text only; binary columns retain their exact bytes.

    ClickHouse String is binary-safe. Invalid UTF-8 is an error for an authored
    Unicode target, never replacement text. No arbitrary ``str`` coercion occurs.
    """

    with closing(_sized_native_source_rows(rows, contract, max_row_bytes)) as adapted:
        for row in adapted:
            yield row.values


def _sized_native_source_rows(
    rows: Iterator[Any], contract: Any, max_row_bytes: int
) -> Generator[_SizedNativeRow, None, None]:
    """Retain the existing early size check for the internal framing consumer.

    The compatibility iterator unwraps values; production framing also consumes
    the reservation. Closing either iterator deterministically closes the source.
    """

    encoder = MssqlNativeEncoder(contract, max_row_bytes=max_row_bytes)
    try:
        for row in rows:
            if isinstance(row, Mapping):
                if set(row) != {column.name for column in contract.columns}:
                    raise ValueError("mssql_native.source_columns_mismatch")
                values = tuple(row[column.name] for column in contract.columns)
            else:
                values = tuple(row)
            if len(values) != len(contract.columns):
                raise ValueError("mssql_native.source_columns_mismatch")
            adapted = []
            decode_budget = max_row_bytes * 3 // 2
            for value, column in zip(values, contract.columns, strict=True):
                if isinstance(value, bytes) and column.storage_type in {"nvarchar", "nchar", "varchar", "char"}:
                    # A BMP character occupies at most three UTF-8 bytes for
                    # two native UTF-16 bytes. This predecode allocation bound
                    # must not be mistaken for the exact native row-size limit.
                    source_bound = (
                        max_row_bytes * 3 // 2 if column.storage_type in {"nvarchar", "nchar"} else max_row_bytes
                    )
                    source_bound = min(source_bound, decode_budget)
                    if len(value) > source_bound:
                        raise ValueError("mssql_native_row_bytes_exceeded")
                    decode_budget -= len(value)
                    try:
                        value = value.decode("utf-8", errors="strict")
                    except UnicodeError:
                        raise ValueError("mssql_native.invalid_utf8_source_text") from None
                adapted.append(value)
            normalized = tuple(adapted)
            size = encoder.encoded_row_size(normalized)
            yield _SizedNativeRow(normalized, contract, max_row_bytes, size)
    finally:
        close = getattr(rows, "close", None)
        if close is not None:
            close()
