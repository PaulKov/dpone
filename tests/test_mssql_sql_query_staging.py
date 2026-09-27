"""SQL Server stages a foreign SQL query through one bulk import."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.runtime.sinks.strategies.mssql.mssql_sql_query_staging import stage_sql_query_artifact
from dpone.runtime.sql_query_artifact import SqlQueryArtifact


def test_clickhouse_query_rows_are_streamed_into_mssql_staging() -> None:
    class Source:
        dialect = "clickhouse"

        def get_records_streaming(self, query, batch_size=10_000, as_dict=False):
            assert query == "SELECT 1 AS id"
            assert as_dict is True
            assert batch_size == 100
            yield [{"id": 1}, {"id": 2}]
            yield [{"id": 3}]

    class Staging:
        def __init__(self) -> None:
            self.rows = []
            self.created = False
            self.dropped = False

        def create(self, load_config, schema):
            self.created = True
            return SimpleNamespace(row_count=None, cleanup=self.drop)

        def drop(self) -> None:
            self.dropped = True

        def insert_streaming_rows(self, handle, rows):
            self.rows.extend(rows)
            return len(self.rows)

    artifact = SqlQueryArtifact(sql="SELECT 1 AS id", dialect="clickhouse", sql_hash="abc")
    staging = Staging()
    handle = stage_sql_query_artifact(
        staging,
        SimpleNamespace(batch_size=100, options={"_source_connector": Source()}),
        artifact,
        (("id", "int"),),
    )

    assert staging.created is True
    assert staging.dropped is False
    assert [row["id"] for row in staging.rows] == [1, 2, 3]
    assert handle.row_count == 3


def test_missing_source_stream_fails_closed() -> None:
    artifact = SqlQueryArtifact(sql="SELECT 1", dialect="clickhouse", sql_hash="abc")
    with pytest.raises(RuntimeError, match="mssql_sql_query_source_stream_required"):
        stage_sql_query_artifact(object(), SimpleNamespace(options={}), artifact, ())
