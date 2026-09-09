"""LoadConfig builders for mssql→ClickHouse strategy live certification."""

from __future__ import annotations

from pathlib import Path

from tests.integration.mssql.mssql_clickhouse_wide_fixtures import SOURCE_SCHEMA, WIDE_TABLE

from dpone.config import LoadConfig, LoadStrategy

TARGET_DATABASE = "dpone_it"


def _load_options(tmp_path: Path, **extra) -> dict:
    return {
        "sink_type": "clickhouse",
        "batch_commit_mode": "whole",
        "partition_tmp_dir": str(tmp_path),
        "technical_columns": "forbidden",
        "type_fidelity": {"binary_encoding": "hex"},
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
    source_schema: str,
    table: str,
    tmp_path: Path,
    strategy: LoadStrategy,
    **kwargs,
) -> LoadConfig:
    options = _load_options(tmp_path, **kwargs.pop("options", {}))
    return LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema=source_schema,
        source_table=table,
        target_schema=TARGET_DATABASE,
        target_table=table,
        load_strategy=strategy,
        export_format="mssql-delimited",
        compress_export=False,
        options=options,
        **kwargs,
    )


def full_refresh(*, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.FULL_REFRESH,
    )


def incremental_append(*, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={"incremental_column": "updated_at", "delta_size_threshold": 0},
    )


def incremental_merge(*, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["id"],
        options={"incremental_column": "updated_at", "delta_size_threshold": 0},
    )


def replace(*, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.REPLACE,
        custom_predicate="business_date = CAST('2026-07-21' AS date)",
        options={"incremental_column": "updated_at"},
    )


def partition_replace(*, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.PARTITION_REPLACE,
        partition={"column": "business_date", "values_from_staging": True},
        options={"incremental_column": "updated_at"},
    )


def backfill_replace(*, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.BACKFILL,
        custom_predicate="business_date = CAST('2026-07-21' AS date)",
        options={"backfill": {"inner_mode": "replace"}},
    )


def snapshot_diff(*, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.SNAPSHOT_DIFF,
        unique_key=["id"],
        options={"diff": {"delete_policy": "hard_delete"}, "mutations_sync": 1},
    )


def scd2(*, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    return _base(
        source_schema=source_schema,
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
