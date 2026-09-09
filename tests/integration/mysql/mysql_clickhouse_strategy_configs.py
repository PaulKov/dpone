"""LoadConfig builders for mysql→ClickHouse strategy live certification."""

from __future__ import annotations

from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from tests.integration.mysql.mysql_clickhouse_wide_fixtures import WIDE_TABLE

TARGET_DATABASE = "dpone_it"


def _load_options(tmp_path: Path, **extra) -> dict:
    return {
        "sink_type": "clickhouse",
        "partition_tmp_dir": str(tmp_path),
        "technical_columns": "forbidden",
        "physical_design": {
            "storage": {
                "clickhouse": {
                    "engine": "MergeTree",
                    "partition_by": "business_date",
                    "order_by": ["id"],
                }
            }
        },
        **extra,
    }


def _base(
    *,
    mysql_database: str,
    table: str,
    tmp_path: Path,
    strategy: LoadStrategy,
    **kwargs,
) -> LoadConfig:
    options = _load_options(tmp_path, **kwargs.pop("options", {}))
    return LoadConfig(
        source_conn_id="mysql_source",
        target_conn_id="clickhouse_sink",
        source_schema=mysql_database,
        source_table=table,
        source_database=mysql_database,
        target_schema=TARGET_DATABASE,
        target_table=table,
        load_strategy=strategy,
        export_format="clickhouse-tsv",
        compress_export=False,
        options=options,
        **kwargs,
    )


def full_refresh(*, mysql_database: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.FULL_REFRESH,
    )


def incremental_append(*, mysql_database: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={"incremental_column": "updated_at"},
    )


def incremental_merge(*, mysql_database: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["id"],
        options={"incremental_column": "updated_at"},
    )


def replace(*, mysql_database: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.REPLACE,
        custom_predicate="business_date = CAST('2026-07-21' AS date)",
        options={"incremental_column": "updated_at"},
    )


def partition_replace(*, mysql_database: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.PARTITION_REPLACE,
        partition={"column": "business_date", "values_from_staging": True},
        options={"incremental_column": "updated_at"},
    )


def backfill_replace(*, mysql_database: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.BACKFILL,
        custom_predicate="business_date = CAST('2026-07-21' AS date)",
        options={"backfill": {"inner_mode": "replace"}},
    )


def snapshot_diff(*, mysql_database: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.SNAPSHOT_DIFF,
        unique_key=["id"],
        options={"diff": {"delete_policy": "hard_delete"}, "mutations_sync": 1},
    )


def scd2(*, mysql_database: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.SCD2,
        unique_key=["id"],
        options={
            "incremental_column": "updated_at",
            "technical_columns": "required",
            "mutations_sync": 1,
            "scd2": {
                "valid_to_column": "__dpone__valid_to_at",
                "current_flag_column": "__dpone__is_current",
                "row_hash_column": "__dpone__row_hash",
                "delete_policy": "expire",
            },
        },
    )


__all__ = [
    "TARGET_DATABASE",
    "backfill_replace",
    "full_refresh",
    "incremental_append",
    "incremental_merge",
    "partition_replace",
    "replace",
    "scd2",
    "snapshot_diff",
]
