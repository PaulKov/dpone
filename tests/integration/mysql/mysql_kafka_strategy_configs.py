"""LoadConfig builders for mysql→Kafka strategy live certification."""

from __future__ import annotations

from pathlib import Path

from dpone.config import LoadConfig, LoadStrategy
from tests.integration.mysql.mysql_kafka_wide_fixtures import WIDE_TABLE


def _kafka_options(tmp_path: Path, topic: str, **extra) -> dict:
    return {
        "sink_type": "kafka",
        "topic": topic,
        "message_format": "json",
        "envelope": "dpone",
        "partition_tmp_dir": str(tmp_path),
        "key": {"mode": "unique_key"},
        "delivery": {"mode": "at_least_once"},
        **extra,
    }


def _base(
    *,
    mysql_database: str,
    table: str,
    topic: str,
    tmp_path: Path,
    strategy: LoadStrategy,
    **kwargs,
) -> LoadConfig:
    options = _kafka_options(tmp_path, topic, **kwargs.pop("options", {}))
    return LoadConfig(
        source_conn_id="mysql_source",
        target_conn_id="kafka_sink",
        source_schema=mysql_database,
        source_table=table,
        source_database=mysql_database,
        target_schema="kafka",
        target_table=topic,
        load_strategy=strategy,
        export_format="csv",
        compress_export=False,
        options=options,
        **kwargs,
    )


def full_refresh(*, mysql_database: str, topic: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        topic=topic,
        tmp_path=tmp_path,
        strategy=LoadStrategy.FULL_REFRESH,
    )


def incremental_append(*, mysql_database: str, topic: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        topic=topic,
        tmp_path=tmp_path,
        strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={"incremental_column": "updated_at"},
    )


def incremental_merge(*, mysql_database: str, topic: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        topic=topic,
        tmp_path=tmp_path,
        strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["id"],
        options={"incremental_column": "updated_at", "merge_policy": "event_upsert"},
    )


def replace(*, mysql_database: str, topic: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        topic=topic,
        tmp_path=tmp_path,
        strategy=LoadStrategy.REPLACE,
        custom_predicate="business_date = CAST('2026-07-21' AS date)",
        options={"incremental_column": "updated_at"},
    )


def snapshot_diff(*, mysql_database: str, topic: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
        table=table,
        topic=topic,
        tmp_path=tmp_path,
        strategy=LoadStrategy.SNAPSHOT_DIFF,
        unique_key=["id"],
        options={"diff": {"delete_policy": "hard_delete"}},
    )


def backfill_merge(*, mysql_database: str, topic: str, tmp_path: Path, table: str = WIDE_TABLE) -> LoadConfig:
    return _base(
        mysql_database=mysql_database,
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
