"""LoadConfig builders for mssql→Kafka strategy live certification.

``partition_replace`` / ``scd2`` and non-merge backfill inner modes are N/A for KafkaSink.
"""

from __future__ import annotations

from pathlib import Path

from tests.integration.mssql.mssql_kafka_wide_fixtures import SOURCE_SCHEMA, WIDE_TABLE

from dpone.config import LoadConfig, LoadStrategy


def _kafka_options(tmp_path: Path, topic: str, **extra) -> dict:
    return {
        "sink_type": "kafka",
        "topic": topic,
        "message_format": "json",
        "envelope": "dpone",
        "batch_commit_mode": "whole",
        "partition_tmp_dir": str(tmp_path),
        "key": {"mode": "unique_key"},
        "delivery": {"mode": "at_least_once"},
        **extra,
    }


def _base(
    *,
    source_schema: str,
    table: str,
    topic: str,
    tmp_path: Path,
    strategy: LoadStrategy,
    **kwargs,
) -> LoadConfig:
    options = _kafka_options(tmp_path, topic, **kwargs.pop("options", {}))
    return LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="kafka_sink",
        source_schema=source_schema,
        source_table=table,
        target_schema="kafka",
        target_table=topic,
        load_strategy=strategy,
        export_format="csv",
        compress_export=False,
        options=options,
        **kwargs,
    )


def full_refresh(
    *, topic: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA
) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        topic=topic,
        tmp_path=tmp_path,
        strategy=LoadStrategy.FULL_REFRESH,
    )


def incremental_append(
    *, topic: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA
) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        topic=topic,
        tmp_path=tmp_path,
        strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={"incremental_column": "updated_at", "delta_size_threshold": 0},
    )


def incremental_merge(
    *, topic: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA
) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        topic=topic,
        tmp_path=tmp_path,
        strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["id"],
        options={"incremental_column": "updated_at", "delta_size_threshold": 0, "merge_policy": "event_upsert"},
    )


def replace(*, topic: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        topic=topic,
        tmp_path=tmp_path,
        strategy=LoadStrategy.REPLACE,
        custom_predicate="business_date = CAST('2026-07-21' AS date)",
        options={"incremental_column": "updated_at"},
    )


def snapshot_diff(
    *, topic: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA
) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        topic=topic,
        tmp_path=tmp_path,
        strategy=LoadStrategy.SNAPSHOT_DIFF,
        unique_key=["id"],
        options={"diff": {"delete_policy": "hard_delete"}},
    )


def backfill_merge(
    *, topic: str, tmp_path: Path, table: str = WIDE_TABLE, source_schema: str = SOURCE_SCHEMA
) -> LoadConfig:
    return _base(
        source_schema=source_schema,
        table=table,
        topic=topic,
        tmp_path=tmp_path,
        strategy=LoadStrategy.BACKFILL,
        custom_predicate="business_date = CAST('2026-07-21' AS date)",
        unique_key=["id"],
        options={"backfill": {"inner_mode": "incremental_merge"}},
    )


__all__ = [
    "backfill_merge",
    "full_refresh",
    "incremental_append",
    "incremental_merge",
    "replace",
    "snapshot_diff",
]
