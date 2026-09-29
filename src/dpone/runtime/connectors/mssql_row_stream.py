"""Bounded, unambiguous ODBC row extraction for MSSQL."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any


def stream_mssql_rows(
    connection: Any,
    query: Any,
    *,
    params: Iterable[Any] | None,
    batch_size: int,
    as_dict: bool,
) -> Iterator[list[Any]]:
    """Yield bounded batches and close the cursor for every terminal path."""

    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("mssql_row_stream.batch_size_positive")
    cursor = connection.cursor()
    try:
        cursor.execute(str(query), tuple(params or ()))
        columns = [column[0] for column in cursor.description or []]
        if as_dict and len({str(column).lower() for column in columns}) != len(columns):
            raise RuntimeError("mssql_row_stream.duplicate_column")
        while rows := cursor.fetchmany(batch_size):
            if not as_dict:
                yield [tuple(row) for row in rows]
                continue
            batch = []
            for row in rows:
                if len(row) != len(columns):
                    raise RuntimeError("mssql_row_stream.column_count_mismatch")
                batch.append(dict(zip(columns, row)))
            yield batch
    finally:
        cursor.close()


__all__ = ["stream_mssql_rows"]
