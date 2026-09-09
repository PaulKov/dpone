"""LoadConfig builders for postgres→MSSQL strategy live certification."""

from __future__ import annotations

import os
from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper
from tests.integration.postgres.postgres_mssql_wide_fixtures import (
    SOURCE_SCHEMA,
    WIDE_TABLE,
    wide_columns,
)

TARGET_SCHEMA = "dpone_it"
STAGING_SCHEMA = "staging"


def _bulk_options(tmp_path: Path, **extra) -> dict:
    options = {
        "source_type": "postgres",
        "sink_type": "mssql",
        "batch_commit_mode": "whole",
        "work_dir": str(tmp_path),
        "partition_tmp_dir": str(tmp_path),
        "technical_columns": "forbidden",
        "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000, "packet_size": 16384}},
        "schema_contract": {
            "columns": {
                column.name: {"type": "string"}
                for column in wide_columns()
                if PostgresMssqlTypeMapper().resolve(column.pg_type).requires_explicit_contract
            }
        },
    }
    options.update(extra)
    return options


def _base(
    *,
    source_schema: str,
    table: str,
    tmp_path: Path,
    strategy: LoadStrategy,
    **kwargs,
) -> LoadConfig:
    options = _bulk_options(tmp_path, **kwargs.pop("options", {}))
    return LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_schema=source_schema,
        source_table=table,
        target_schema=TARGET_SCHEMA,
        target_table=table,
        target_database=os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it"),
        staging_schema=STAGING_SCHEMA,
        staging_database=os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it"),
        load_strategy=strategy,
        export_format="csv",
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


def snapshot_diff(*, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.SNAPSHOT_DIFF,
        unique_key=["id"],
        options={"diff": {"delete_policy": "hard_delete"}},
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
            "scd2": {
                "valid_to_column": "__dpone__valid_to_at",
                "current_flag_column": "__dpone__is_current",
                "row_hash_column": "__dpone__row_hash",
                "delete_policy": "expire",
            },
        },
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


def cdc_apply(*, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    """Build the declared-but-unsupported MSSQL batch-sink case.

    PostgreSQL CDC has a separate runtime and MSSQL currently has no live CDC
    apply adapter.  Keeping this builder beside the supported cases makes the
    negative capability leg explicit and lets vendor-live prove that it fails
    before target mutation.
    """

    return _base(
        source_schema=source_schema,
        table=table,
        tmp_path=tmp_path,
        strategy=LoadStrategy.CDC_APPLY,
        unique_key=["id"],
    )


__all__ = [
    "STAGING_SCHEMA",
    "TARGET_SCHEMA",
    "backfill_replace",
    "cdc_apply",
    "full_refresh",
    "incremental_append",
    "incremental_merge",
    "partition_replace",
    "replace",
    "scd2",
    "snapshot_diff",
]
