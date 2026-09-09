"""ClickHouse Native format encoder for typed native transfers."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

from dpone.runtime.clickhouse_binary_encoding import (
    ClickHouseColumnSpec,
    default_non_null_value,
    encode_clickhouse_non_null,
    encode_clickhouse_string,
    resolve_columns,
    unwrap_nullable,
    var_uint,
)
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy


class ClickHouseNativeEncoder:
    """Encode typed rows into ClickHouse Native columnar blocks."""

    def __init__(
        self,
        schema: Sequence[tuple[str, str]],
        *,
        block_rows: int = 65_536,
        block_bytes: int | None = None,
        schema_kind: str = "mssql",
        type_policy: MssqlClickHouseTypePolicy | None = None,
        target_schema: Sequence[tuple[str, str]] | None = None,
    ) -> None:
        self._type_policy = type_policy or MssqlClickHouseTypePolicy()
        self._columns = resolve_columns(
            schema,
            schema_kind=schema_kind,
            type_policy=type_policy,
            target_schema=target_schema,
        )
        self._block_rows = max(1, int(block_rows))
        self._block_bytes = int(block_bytes) if block_bytes is not None else None

    def iter_batches(self, rows: Iterable[Mapping[str, Any] | Sequence[Any]]) -> Iterator[bytes]:
        """Yield ClickHouse Native blocks without materializing all rows."""

        block: list[tuple[Any, ...]] = []
        estimated_bytes = 0
        for row in rows:
            values = self._row_values(row)
            block.append(values)
            estimated_bytes += _estimate_row_bytes(values)
            if len(block) >= self._block_rows or self._should_flush_by_bytes(estimated_bytes):
                yield self._encode_block(block)
                block.clear()
                estimated_bytes = 0
        if block:
            yield self._encode_block(block)

    def _row_values(self, row: Mapping[str, Any] | Sequence[Any]) -> tuple[Any, ...]:
        if isinstance(row, Mapping):
            return tuple(row.get(column.name) for column in self._columns)
        return tuple(row)

    def _should_flush_by_bytes(self, estimated_bytes: int) -> bool:
        return self._block_bytes is not None and estimated_bytes >= self._block_bytes

    def _encode_block(self, rows: Sequence[tuple[Any, ...]]) -> bytes:
        payload = bytearray()
        payload.extend(var_uint(len(self._columns)))
        payload.extend(var_uint(len(rows)))
        for index, column in enumerate(self._columns):
            values = [row[index] for row in rows]
            payload.extend(encode_clickhouse_string(column.name))
            payload.extend(encode_clickhouse_string(column.clickhouse_type))
            payload.extend(_encode_column(column, values, self._type_policy))
        return bytes(payload)


def _encode_column(
    column: ClickHouseColumnSpec,
    values: Sequence[Any],
    policy: MssqlClickHouseTypePolicy,
) -> bytes:
    nullable, inner_type = unwrap_nullable(column.clickhouse_type)
    if not nullable:
        if any(value is None for value in values):
            raise ValueError(f"NULL cannot be encoded for non-nullable ClickHouse type {column.clickhouse_type!r}.")
        return b"".join(encode_clickhouse_non_null(value, inner_type, column.source_type, policy) for value in values)

    null_map = bytes(1 if value is None else 0 for value in values)
    nested_values = (default_non_null_value(inner_type) if value is None else value for value in values)
    nested = b"".join(
        encode_clickhouse_non_null(value, inner_type, column.source_type, policy) for value in nested_values
    )
    return null_map + nested


def _estimate_row_bytes(values: Sequence[Any]) -> int:
    total = 0
    for value in values:
        if value is None:
            total += 1
        elif isinstance(value, bytes | bytearray):
            total += len(value)
        else:
            total += len(str(value))
    return total


__all__ = ["ClickHouseNativeEncoder"]
