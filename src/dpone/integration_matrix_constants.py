"""Constants and row aliases for integration matrix certification."""

from __future__ import annotations

from typing import Any

SOURCE_FAMILIES: tuple[str, ...] = ("postgres", "mssql", "mysql", "clickhouse", "api", "kafka")
SINK_FAMILIES: tuple[str, ...] = ("mssql", "postgres", "clickhouse", "bigquery", "kafka")
BASE_LOAD_STRATEGIES: tuple[str, ...] = ("full_refresh", "incremental_append", "incremental_merge", "replace")
COMMON_PRODUCTION_STRATEGIES: tuple[str, ...] = ("snapshot_diff",)
DB_TARGET_PRODUCTION_STRATEGIES: tuple[str, ...] = ("scd2", "backfill")
PARTITION_REPLACE_SINKS: frozenset[str] = frozenset({"mssql", "postgres", "clickhouse", "bigquery"})
SOURCE_SPECIFIC_STRATEGIES: dict[str, tuple[str, ...]] = {
    "postgres": ("xmin", "cdc"),
    "mssql": ("cdc",),
}
LOAD_STRATEGIES: tuple[str, ...] = (*BASE_LOAD_STRATEGIES, *COMMON_PRODUCTION_STRATEGIES)

EXTRAS_BY_FAMILY: dict[str, tuple[str, ...]] = {
    "api": (),
    "bigquery": ("gcp",),
    "clickhouse": ("clickhouse",),
    "kafka": ("kafka",),
    "mssql": ("mssql",),
    "mysql": ("mysql",),
    "postgres": ("postgres",),
}

PROFILE_BY_FAMILY: dict[str, str] = {
    "api": "rest_mock",
    "bigquery": "bigquery_documented_contract",
    "clickhouse": "clickhouse_local",
    "kafka": "kafka_local",
    "mssql": "mssql_local",
    "mysql": "mysql_local",
    "postgres": "postgres_local",
}

LIVE_PROFILE_BY_FAMILY: dict[str, str] = {
    "api": "rest_vendor_live",
    "bigquery": "bigquery_vendor_live",
    "clickhouse": "clickhouse_live",
    "kafka": "kafka_live",
    "mssql": "mssql_live",
    "mysql": "mysql_live",
    "postgres": "postgres_live",
}

MatrixRow = dict[str, Any]
DEFAULT_MOCK_ROW_COUNT = 10_000
MAX_MOCK_ROW_COUNT = 100_000
INCREMENTAL_CHANGE_RATIO = 0.20
PHYSICAL_DELETE_RATIO = 0.05

__all__ = [
    "BASE_LOAD_STRATEGIES",
    "COMMON_PRODUCTION_STRATEGIES",
    "DB_TARGET_PRODUCTION_STRATEGIES",
    "DEFAULT_MOCK_ROW_COUNT",
    "EXTRAS_BY_FAMILY",
    "INCREMENTAL_CHANGE_RATIO",
    "LIVE_PROFILE_BY_FAMILY",
    "LOAD_STRATEGIES",
    "MAX_MOCK_ROW_COUNT",
    "MatrixRow",
    "PARTITION_REPLACE_SINKS",
    "PHYSICAL_DELETE_RATIO",
    "PROFILE_BY_FAMILY",
    "SINK_FAMILIES",
    "SOURCE_FAMILIES",
    "SOURCE_SPECIFIC_STRATEGIES",
]
