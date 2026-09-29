"""Fail-closed boundaries for row-addressable MSSQL extraction."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory


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


def test_mssql_streaming_artifact_passes_configured_fetch_bound() -> None:
    class Connector:
        def __init__(self) -> None:
            self.batch_size: int | None = None

        def get_records_iterator(
            self, _query: str, params: tuple[object, ...] | None = None, *, batch_size: int = 10000
        ):
            assert params is None
            self.batch_size = batch_size
            return iter(({"id": 1},))

    connector = Connector()
    artifact = MSSQLQueryoutArtifactFactory(connector, logger=None).streaming_artifact("SELECT id", batch_size=2)

    assert connector.batch_size == 2
    assert list(artifact._iterator) == [{"id": 1}]


def test_mssql_row_iterator_passes_fetch_bound_to_cursor() -> None:
    class Connector:
        def __init__(self) -> None:
            self.batch_size: int | None = None

        def get_records_streaming(self, _query: str, *, params, batch_size: int, as_dict: bool):
            assert params is None
            assert as_dict
            self.batch_size = batch_size
            yield [{"id": 1}]

    connector = Connector()

    assert list(MSSQLConnector.get_records_iterator(connector, "SELECT id", batch_size=2)) == [{"id": 1}]
    assert connector.batch_size == 2


def test_mssql_row_stream_rejects_invalid_fetch_bound_before_cursor_open() -> None:
    connector = SimpleNamespace(connection=SimpleNamespace(cursor=lambda: pytest.fail("opened cursor")))

    with pytest.raises(ValueError, match="mssql_row_stream.batch_size_positive"):
        list(MSSQLConnector.get_records_streaming(connector, "SELECT id", batch_size=0))
