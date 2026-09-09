"""Стратегии загрузки данных для sinks.

Ленивый export защищает lightweight tooling и тесты от optional runtime dependencies
(например psycopg / cloud SDK), пока конкретная стратегия не используется.
"""

from __future__ import annotations

from importlib import import_module

_EXPORTS: dict[str, tuple[str, str]] = {
    "SinkStrategy": ("dpone.runtime.sinks.strategies.base", "SinkStrategy"),
    # PostgreSQL стратегии
    "PostgresFullRefreshStrategy": (
        "dpone.runtime.sinks.strategies.postgres.postgres_full_refresh",
        "PostgresFullRefreshStrategy",
    ),
    "PostgresIncrementMergeStrategy": (
        "dpone.runtime.sinks.strategies.postgres.postgres_increment_merge",
        "PostgresIncrementMergeStrategy",
    ),
    "PostgresIncrementAppendStrategy": (
        "dpone.runtime.sinks.strategies.postgres.postgres_increment_append",
        "PostgresIncrementAppendStrategy",
    ),
    "PostgresReplaceStrategy": (
        "dpone.runtime.sinks.strategies.postgres.postgres_replace_strategy",
        "PostgresReplaceStrategy",
    ),
    # BigQuery стратегии
    "BigQueryStrategyBase": ("dpone.runtime.sinks.strategies.bigquery.bigquery_base", "BigQueryStrategyBase"),
    "BigQueryFullRefreshStrategy": (
        "dpone.runtime.sinks.strategies.bigquery.bigquery_full_refresh",
        "BigQueryFullRefreshStrategy",
    ),
    "BigQueryIncrementMergeStrategy": (
        "dpone.runtime.sinks.strategies.bigquery.bigquery_increment_merge",
        "BigQueryIncrementMergeStrategy",
    ),
    "BigQueryIncrementAppendStrategy": (
        "dpone.runtime.sinks.strategies.bigquery.bigquery_increment_append",
        "BigQueryIncrementAppendStrategy",
    ),
    "BigQueryReplaceStrategy": (
        "dpone.runtime.sinks.strategies.bigquery.bigquery_replace_strategy",
        "BigQueryReplaceStrategy",
    ),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(name)
    module_name, attr_name = _EXPORTS[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
