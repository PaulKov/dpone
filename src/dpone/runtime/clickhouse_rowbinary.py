"""ClickHouse RowBinary encoding for typed native transfer streams."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

from dpone.runtime.clickhouse_binary_encoding import encode_clickhouse_value, resolve_columns
from dpone.runtime.support.type_mapping.mssql_clickhouse import MssqlClickHouseTypePolicy


class ClickHouseRowBinaryEncoder:
    """Encode typed Python rows into ClickHouse RowBinary chunks."""

    def __init__(
        self,
        schema: Sequence[tuple[str, str]],
        *,
        chunk_rows: int = 8192,
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
        self._chunk_rows = max(1, int(chunk_rows))

    def iter_batches(self, rows: Iterable[Mapping[str, Any] | Sequence[Any]]) -> Iterator[bytes]:
        """Yield RowBinary chunks without materializing the full dataset."""

        buffer = bytearray()
        count = 0
        for row in rows:
            values = self._row_values(row)
            for value, column in zip(values, self._columns, strict=True):
                buffer.extend(
                    encode_clickhouse_value(
                        value,
                        column.clickhouse_type,
                        column.source_type,
                        self._type_policy,
                    )
                )
            count += 1
            if count >= self._chunk_rows:
                yield bytes(buffer)
                buffer.clear()
                count = 0
        if buffer:
            yield bytes(buffer)

    def _row_values(self, row: Mapping[str, Any] | Sequence[Any]) -> tuple[Any, ...]:
        if isinstance(row, Mapping):
            return tuple(row.get(column.name) for column in self._columns)
        return tuple(row)


__all__ = ["ClickHouseRowBinaryEncoder"]
