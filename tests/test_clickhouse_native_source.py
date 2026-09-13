"""One data SELECT and fail-closed snapshot/type admission with synthetic metadata."""

from contextlib import nullcontext

import pytest

from dpone.runtime.sources.clickhouse_native_source import ClickHouseNativeSource
from tests.test_mssql_native_policy import config


class Connector:
    driver = "native"

    def __init__(self, engine="MergeTree", dtype="UInt64"):
        self.engine, self.dtype = engine, dtype
        self.selects = []
        self.connection = self

    def get_records(self, query, params=None, as_dict=False):
        if "system.tables" in query:
            return [
                {"engine": self.engine, "database_engine": "Atomic", "uuid": "11111111-1111-4111-8111-111111111111"}
            ]
        return [{"name": "value", "type": self.dtype, "default_kind": ""}]

    def execute_iter(self, query, params, **kwargs):
        self.selects.append((query, params, kwargs))
        return iter([[("value", self.dtype)], (2**64 - 1,), (2**64 - 1,)])

    def disconnect(self):
        pass


def test_single_query_preserves_duplicates_without_count_or_offset():
    connector = Connector()
    value = config()
    value.source_schema = "synthetic"
    value.source_table = "events"
    result = ClickHouseNativeSource(connector, schema_guard_factory=lambda cfg: nullcontext()).extract(value)
    assert list(result.artifact.iter_native_rows()) == [{"value": 2**64 - 1}, {"value": 2**64 - 1}]
    assert len(connector.selects) == 1
    assert "OFFSET" not in connector.selects[0][0]
    assert result.artifact.rows_exported == 2


@pytest.mark.parametrize(
    "engine,dtype",
    [
        ("ReplacingMergeTree", "UInt64"),
        ("Distributed", "UInt64"),
        ("MergeTree", "DateTime64(9)"),
        ("MergeTree", "Array(UInt64)"),
    ],
)
def test_unsupported_metadata_fails_before_data(engine, dtype):
    connector = Connector(engine, dtype)
    value = config()
    value.source_schema = "synthetic"
    value.source_table = "events"
    with pytest.raises(ValueError):
        ClickHouseNativeSource(connector, schema_guard_factory=lambda cfg: nullcontext()).extract(value)
    assert not connector.selects


def test_temporal_projection_preserves_far_future_microseconds():
    from datetime import datetime

    from dpone.runtime.sources.clickhouse_native_values import projection, restore

    assert restore(8615030400000001, "DateTime64(6)") == datetime(2243, 1, 1, 0, 0, 0, 1)
    sql, dtype = projection("observed_at", "Nullable(DateTime64(6, 'UTC'))")
    assert "toUnixTimestamp64Micro" in sql and dtype == "Nullable(Int64)"


def test_missing_ddl_guard_fails_before_metadata():
    connector = Connector()
    value = config()
    value.source_schema = "synthetic"
    value.source_table = "events"
    with pytest.raises(ValueError, match="ddl_guard"):
        ClickHouseNativeSource(connector).extract(value)
    assert not connector.selects


def test_iterator_cancellation_closes_backing_stream():
    from dpone.runtime.sources.clickhouse_native_source import NativeQueryArtifact

    events = []

    def rows():
        try:
            yield {"value": 1}
            yield {"value": 2}
        finally:
            events.append("closed")

    artifact = NativeQueryArtifact(rows(), query_id="synthetic", cleanup=lambda: None)
    iterator = artifact.iter_native_rows()
    next(iterator)
    iterator.close()
    assert events == ["closed"] and getattr(artifact, "rows_exported", None) is None


def test_metadata_failure_preserves_primary_when_guard_cleanup_fails():
    class Guard:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            raise RuntimeError("synthetic cleanup")

    value = config()
    value.source_schema, value.source_table = "synthetic", "events"
    with pytest.raises(ValueError, match="plain_mergetree") as failure:
        ClickHouseNativeSource(Connector("Distributed"), schema_guard_factory=lambda cfg: Guard()).extract(value)
    assert any("guard cleanup failed" in note for note in failure.value.__notes__)


def test_schema_failure_preserves_primary_when_stream_close_fails():
    class Stream:
        def __iter__(self):
            return self

        def __next__(self):
            return [("wrong", "UInt64")]

        def close(self):
            raise RuntimeError("synthetic cleanup")

    class FailingConnector(Connector):
        def execute_iter(self, *args, **kwargs):
            return Stream()

    value = config()
    value.source_schema, value.source_table = "synthetic", "events"
    result = ClickHouseNativeSource(FailingConnector(), schema_guard_factory=lambda cfg: nullcontext()).extract(value)
    assert result.artifact.source_relation_uuid == "11111111-1111-4111-8111-111111111111"
    with pytest.raises(ValueError, match="schema_changed") as failure:
        list(result.artifact.iter_native_rows())
    assert any("iterator cleanup failed" in note for note in failure.value.__notes__)


def test_window_parameters_keep_microseconds_and_force_complete_query():
    class TemporalConnector(Connector):
        def get_records(self, query, params=None, as_dict=False):
            rows = super().get_records(query, params, as_dict)
            if "system.columns" in query:
                return [{"name": "observed_at", "type": "DateTime64(6)", "default_kind": ""}]
            return rows

        def execute_iter(self, query, params, **kwargs):
            self.selects.append((query, params, kwargs))
            return iter([[("observed_at", "Int64")]])

    value = config("partition_replace")
    value.source_schema, value.source_table = "synthetic", "events"
    value.options.update(
        mssql_native_window={"column": "observed_at", "anchor": "data_interval_end", "lookback": "P1D"},
        interval={"interval_end": "2026-09-10T00:00:00.000001Z"},
    )
    connector = TemporalConnector()
    result = ClickHouseNativeSource(connector, schema_guard_factory=lambda cfg: nullcontext()).extract(value)
    assert list(result.artifact.iter_native_rows()) == []
    query, params, kwargs = connector.selects[0]
    assert params["end"].endswith(".000001") and params["start"].endswith(".000001")
    assert "toUnixTimestamp64Micro" in query and " < toDateTime64" in query
    # The SELECT alias contains integer microseconds; the window must bind the
    # original DateTime column, not that alias under ClickHouse alias substitution.
    assert "AS `__dpone_native_source` WHERE `__dpone_native_source`.`observed_at` >=" in query
    assert "AND `__dpone_native_source`.`observed_at` <" in query
    assert kwargs["settings"]["result_overflow_mode"] == "throw"
    assert kwargs["settings"]["use_query_cache"] == 0
