"""Fail-closed boundaries for row-addressable MSSQL extraction."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
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

        def get_records_streaming(
            self, _query: str, *, params: tuple[object, ...] | None, batch_size: int, as_dict: bool
        ):
            assert params is None
            assert as_dict
            self.batch_size = batch_size
            return iter(([{"id": 1}],))

    connector = Connector()
    artifact = MSSQLQueryoutArtifactFactory(connector, logger=None).streaming_artifact("SELECT id", batch_size=2)

    assert connector.batch_size == 2
    assert list(artifact._iterator) == [{"id": 1}]


def test_streaming_artifact_abort_closes_bounded_cursor() -> None:
    closed = False

    class Connector:
        def get_records_streaming(self, _query: str, *, params, batch_size: int, as_dict: bool):
            nonlocal closed
            assert params is None and batch_size == 2 and as_dict
            try:
                yield [{"id": 1}, {"id": 2}]
                yield [{"id": 3}]
            finally:
                closed = True

    artifact = MSSQLQueryoutArtifactFactory(Connector(), logger=None).streaming_artifact(
        "SELECT id", batch_size=2, require_bounded=True
    )
    assert next(artifact._iterator) == {"id": 1}

    artifact.terminate(ArtifactTerminalOutcome.ABORT)

    assert closed


def test_streaming_artifact_preserves_legacy_connector_port() -> None:
    class LegacyConnector:
        def get_records_iterator(self, _query: str, params: tuple[object, ...] | None = None):
            assert params == (7,)
            return iter(({"id": 7},))

    artifact = MSSQLQueryoutArtifactFactory(LegacyConnector(), logger=None).streaming_artifact(
        "SELECT id WHERE id = ?", params=(7,), batch_size=2
    )

    assert list(artifact._iterator) == [{"id": 7}]

    with pytest.raises(RuntimeError, match="mssql_row_stream.bounded_connector_required"):
        MSSQLQueryoutArtifactFactory(LegacyConnector(), logger=None).streaming_artifact(
            "SELECT id", batch_size=2, require_bounded=True
        )


def test_clickhouse_target_type_requires_bounded_connector() -> None:
    class LegacyConnector:
        def get_records_iterator(self, _query: str):
            return iter(())

    config = SimpleNamespace(
        source_schema="dbo",
        source_table="orders",
        batch_size=2,
        options={"source_type": "mssql", "target_type": "clickhouse", "mssql_export_mode": "streaming"},
    )
    factory = MSSQLQueryoutArtifactFactory(LegacyConnector(), logger=None)

    with pytest.raises(RuntimeError, match="mssql_row_stream.bounded_connector_required"):
        factory.artifact_for_query(config, "SELECT id FROM dbo.orders", [("id", "int")])


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
