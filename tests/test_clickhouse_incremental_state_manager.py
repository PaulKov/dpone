from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from dpone.config import LoadConfig
from dpone.contracts.clickhouse_incremental_cursor import CLICKHOUSE_MSSQL_CURSOR_ERROR_CODE
from dpone.runtime.sources.strategies.clickhouse.incremental_state_manager import IncrementalStateManager


class _Logger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, message: str) -> None:
        self.messages.append(message)

    def warning(self, message: str) -> None:
        self.messages.append(message)


@dataclass
class _MssqlLikeSink:
    value: str | None = "2026-07-07 09:00:00"
    calls: list[tuple[str, str, str, str | None]] = field(default_factory=list)

    def get_max_column_value(self, schema: str, table: str, column: str, *, database: str | None = None):
        self.calls.append((schema, table, column, database))
        return self.value


@dataclass
class _SqlRendererSink:
    calls: list[str] = field(default_factory=list)

    def build_max_query(self, schema: str, table: str, column: str, *, database: str | None = None) -> str:
        return f"SELECT MAX([{column}]) FROM [{database}].[{schema}].[{table}]"

    def get_records(self, query: str, params=None):
        del params
        self.calls.append(query)
        return [("2026-07-07 10:00:00",)]


@dataclass
class _BigQueryLikeSink:
    calls: list[str] = field(default_factory=list)

    def build_max_query(self, schema: str, table: str, column: str) -> str:
        return f"SELECT MAX(`{column}`) as max_val FROM `project.{schema}.{table}`"

    def get_records(self, query: str, params=None):
        del params
        self.calls.append(query)
        return [{"max_val": "2026-07-07 11:00:00"}]


def _config(*, sink_type: str = "mssql") -> LoadConfig:
    return LoadConfig(
        source_conn_id="clickhouse",
        target_conn_id="mssql",
        source_schema="marketing_datamarts",
        source_table="sample_web_sync",
        target_database="DWH_Stage",
        target_schema="marketing",
        target_table="sample_web_sync",
        options={"incremental_column": "event_at", "sink_type": sink_type},
    )


def test_incremental_state_manager_rejects_mssql_before_target_max_io() -> None:
    sink = _MssqlLikeSink()

    with pytest.raises(ValueError, match=CLICKHOUSE_MSSQL_CURSOR_ERROR_CODE):
        IncrementalStateManager(sink_connector=sink, logger=_Logger()).get_state(_config())

    assert sink.calls == []


def test_incremental_state_manager_keeps_native_max_accessor_for_non_mssql_sink() -> None:
    sink = _MssqlLikeSink()
    config = _config(sink_type="postgres")
    config.options["incremental_state_timezone"] = "passthrough"

    state = IncrementalStateManager(sink_connector=sink, logger=_Logger()).get_state(config)

    assert state == {"last_value": "2026-07-07 09:00:00", "column": "event_at"}
    assert sink.calls == [("marketing", "sample_web_sync", "event_at", "DWH_Stage")]


def test_incremental_state_manager_uses_sink_dialect_max_query_when_available() -> None:
    sink = _SqlRendererSink()
    config = _config(sink_type="postgres")
    config.options["incremental_state_timezone"] = "passthrough"

    state = IncrementalStateManager(sink_connector=sink, logger=_Logger()).get_state(config)

    assert state == {"last_value": "2026-07-07 10:00:00", "column": "event_at"}
    assert sink.calls == ["SELECT MAX([event_at]) FROM [DWH_Stage].[marketing].[sample_web_sync]"]


def test_incremental_state_manager_keeps_legacy_bigquery_style_max_query_contract() -> None:
    sink = _BigQueryLikeSink()
    config = _config(sink_type="bigquery")
    config.options["incremental_state_timezone"] = "passthrough"

    state = IncrementalStateManager(sink_connector=sink, logger=_Logger()).get_state(config)

    assert state == {"last_value": "2026-07-07 11:00:00", "column": "event_at"}
    assert sink.calls == ["SELECT MAX(`event_at`) as max_val FROM `project.marketing.sample_web_sync`"]
