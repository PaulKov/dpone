"""Fail-closed boundaries for row-addressable MSSQL extraction."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.runtime.connectors.mssql import MSSQLConnector


@pytest.mark.parametrize(
    ("columns", "row", "error"),
    (
        (("id", "id"), (1, 2), "duplicate_column"),
        (("id", "amount"), (1,), "column_count_mismatch"),
    ),
)
def test_mssql_row_stream_rejects_ambiguous_or_truncated_rows(
    columns: tuple[str, ...], row: tuple[object, ...], error: str
) -> None:
    class Cursor:
        description = tuple((name,) for name in columns)

        def __init__(self) -> None:
            self.closed = False
            self.read = False

        def execute(self, _query: str, _params: tuple[object, ...]) -> None:
            return None

        def fetchmany(self, _size: int) -> list[tuple[object, ...]]:
            if self.read:
                return []
            self.read = True
            return [row]

        def close(self) -> None:
            self.closed = True

    cursor = Cursor()
    connector = SimpleNamespace(connection=SimpleNamespace(cursor=lambda: cursor))

    with pytest.raises(RuntimeError, match=f"mssql_row_stream.{error}"):
        list(MSSQLConnector.get_records_streaming(connector, "SELECT id, amount", as_dict=True))

    assert cursor.closed
