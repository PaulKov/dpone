"""Runtime sinks.

IMPORTANT
---------
Some sinks (e.g. BigQuery) require optional heavyweight dependencies.
To keep lightweight tooling usable in minimal environments, this package exports
symbols **lazily**.

Prefer explicit submodules for strict imports:
- :mod:`dpone.runtime.sinks.postgres`
- :mod:`dpone.runtime.sinks.bigquery`
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "AbstractSink",
    "LoadPayload",
    "LoadResult",
    "PostgresSink",
    "BigQuerySink",
    "MSSQLSink",
    "ClickHouseSink",
    "KafkaSink",
    "SinkStrategy",
    # PostgreSQL strategies
    "PostgresFullRefreshStrategy",
    "PostgresIncrementMergeStrategy",
    "PostgresIncrementAppendStrategy",
    "PostgresReplaceStrategy",
    # BigQuery strategies
    "BigQueryFullRefreshStrategy",
    "BigQueryIncrementMergeStrategy",
    "BigQueryIncrementAppendStrategy",
    "BigQueryReplaceStrategy",
]

_EXPORTS: dict[str, str] = {
    # Base
    "AbstractSink": "dpone.runtime.sinks.sink_protocol:AbstractSink",
    "LoadPayload": "dpone.runtime.sinks.load_payload:LoadPayload",
    "LoadResult": "dpone.runtime.sinks.load_result:LoadResult",
    # Sinks
    "PostgresSink": "dpone.runtime.sinks.postgres:PostgresSink",
    "BigQuerySink": "dpone.runtime.sinks.bigquery:BigQuerySink",
    "MSSQLSink": "dpone.runtime.sinks.mssql:MSSQLSink",
    "ClickHouseSink": "dpone.runtime.sinks.clickhouse:ClickHouseSink",
    "KafkaSink": "dpone.runtime.sinks.kafka:KafkaSink",
    # Strategy base
    "SinkStrategy": "dpone.runtime.sinks.strategies.base:SinkStrategy",
    # Postgres strategies
    "PostgresFullRefreshStrategy": "dpone.runtime.sinks.strategies.postgres.postgres_full_refresh:PostgresFullRefreshStrategy",
    "PostgresIncrementMergeStrategy": "dpone.runtime.sinks.strategies.postgres.postgres_increment_merge:PostgresIncrementMergeStrategy",
    "PostgresIncrementAppendStrategy": "dpone.runtime.sinks.strategies.postgres.postgres_increment_append:PostgresIncrementAppendStrategy",
    "PostgresReplaceStrategy": "dpone.runtime.sinks.strategies.postgres.postgres_replace_strategy:PostgresReplaceStrategy",
    # BigQuery strategies
    "BigQueryFullRefreshStrategy": "dpone.runtime.sinks.strategies.bigquery.bigquery_full_refresh:BigQueryFullRefreshStrategy",
    "BigQueryIncrementMergeStrategy": "dpone.runtime.sinks.strategies.bigquery.bigquery_increment_merge:BigQueryIncrementMergeStrategy",
    "BigQueryIncrementAppendStrategy": "dpone.runtime.sinks.strategies.bigquery.bigquery_increment_append:BigQueryIncrementAppendStrategy",
    "BigQueryReplaceStrategy": "dpone.runtime.sinks.strategies.bigquery.bigquery_replace_strategy:BigQueryReplaceStrategy",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    mod = import_module(module_name)
    value = getattr(mod, attr)
    globals()[name] = value  # cache
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(__all__)))
