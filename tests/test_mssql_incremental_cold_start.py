"""Hermetic: MSSQL incremental cold-start when sink watermark table is missing."""

from __future__ import annotations

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.sources.strategies.mssql.mssql_incremental import MSSQLIncrementalExtractStrategy


class _ExplodingSink:
    dialect = "clickhouse"

    def get_max_column_value(self, *args, **kwargs):
        del args, kwargs
        raise RuntimeError("ClickHouse get_records error: Code: 60. Unknown table")


def test_mssql_incremental_get_state_returns_none_when_sink_table_missing() -> None:
    strategy = MSSQLIncrementalExtractStrategy(connector=object(), logger=object(), sink_connector=_ExplodingSink())
    config = LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="dpone_it",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={"incremental_column": "updated_at"},
    )
    assert strategy.get_state(config) is None
