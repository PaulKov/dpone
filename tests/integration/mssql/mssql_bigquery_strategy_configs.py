"""LoadConfig builders for mssql→BigQuery strategy live certification."""

from __future__ import annotations

from pathlib import Path

from tests.integration.mssql.mssql_bigquery_wide_fixtures import SOURCE_SCHEMA, WIDE_TABLE

from dpone.config import LoadConfig, LoadStrategy


def _base(
    *,
    source_schema: str,
    dataset: str,
    table: str,
    tmp_path: Path,
    strategy: LoadStrategy,
    **kwargs,
) -> LoadConfig:
    options = {
        "sink_type": "bigquery",
        # whole → FileExportArtifact → BQ direct load (no GCS).
        "batch_commit_mode": "whole",
        "partition_tmp_dir": str(tmp_path),
        "technical_columns": "forbidden",
        **kwargs.pop("options", {}),
    }
    return LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="bigquery_dwh",
        source_schema=source_schema,
        source_table=table,
        target_schema=dataset,
        target_table=table,
        staging_schema=dataset,
        load_strategy=strategy,
        export_format="csv",
        compress_export=False,
        options=options,
        **kwargs,
    )


def full_refresh(
    *, dataset: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA
) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        dataset=dataset,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.FULL_REFRESH,
    )


def incremental_append(
    *, dataset: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA
) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        dataset=dataset,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={"incremental_column": "updated_at", "delta_size_threshold": 0},
    )


def incremental_merge(
    *, dataset: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA
) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        dataset=dataset,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["id"],
        options={"incremental_column": "updated_at", "delta_size_threshold": 0},
    )


def replace(*, dataset: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        dataset=dataset,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.REPLACE,
        custom_predicate="business_date = CAST('2026-07-21' AS date)",
        options={"incremental_column": "updated_at"},
    )


def partition_replace(
    *, dataset: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA
) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        dataset=dataset,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.PARTITION_REPLACE,
        partition={"column": "business_date", "values_from_staging": True},
        options={"incremental_column": "updated_at"},
    )


def snapshot_diff(
    *, dataset: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA
) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        dataset=dataset,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.SNAPSHOT_DIFF,
        unique_key=["id"],
        options={"diff": {"delete_policy": "hard_delete"}},
    )


def scd2(*, dataset: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        dataset=dataset,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.SCD2,
        unique_key=["id"],
        options={
            "incremental_column": "updated_at",
            "technical_columns": "required",
            "scd2": {
                "valid_to_column": "__dpone__valid_to_at",
                "current_flag_column": "__dpone__is_current",
                "row_hash_column": "__dpone__row_hash",
                "delete_policy": "expire",
            },
        },
    )


def backfill_replace(
    *, dataset: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA
) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        dataset=dataset,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.BACKFILL,
        custom_predicate="business_date = CAST('2026-07-21' AS date)",
        options={"backfill": {"inner_mode": "replace"}},
    )


__all__ = [
    "backfill_replace",
    "full_refresh",
    "incremental_append",
    "incremental_merge",
    "partition_replace",
    "replace",
    "scd2",
    "snapshot_diff",
]
