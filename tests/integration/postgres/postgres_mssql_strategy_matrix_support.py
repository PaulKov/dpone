"""Reusable vendor helpers for the PostgreSQL→MSSQL strategy submode matrix."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.config import LoadConfig, LoadStrategy
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedStandardEtlRunner,
)

SOURCE_SCHEMA = "dpone_src"
SOURCE_TABLE = "strategy_capability_source"
TARGET_SCHEMA = "dpone_it"
STAGING_SCHEMA = "staging"

BASE_ROWS = (
    (1, 1, "old-one"),
    (2, 2, "old-two"),
)
CHANGED_ROWS = (
    (1, 1, "new-one"),
    (3, 1, "new-three"),
)
SCHEMA = (("id", "integer"), ("partition_id", "integer"), ("value", "text"))


def ensure_source(postgres: Any) -> None:
    postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{SOURCE_SCHEMA}"')
    postgres.execute_query(
        f'CREATE TABLE IF NOT EXISTS "{SOURCE_SCHEMA}"."{SOURCE_TABLE}" ('
        "id integer NOT NULL, partition_id integer NULL, value text NULL)"
    )


def set_source_rows(postgres: Any, rows: tuple[tuple[object, object, object], ...]) -> None:
    postgres.execute_query(f'TRUNCATE TABLE "{SOURCE_SCHEMA}"."{SOURCE_TABLE}"')
    for row in rows:
        postgres.execute_query(
            f'INSERT INTO "{SOURCE_SCHEMA}"."{SOURCE_TABLE}" (id, partition_id, value) VALUES (%s, %s, %s)',
            row,
        )


def target_table(case_id: str) -> str:
    token = re.sub(r"[^a-z0-9]+", "_", case_id.lower()).strip("_")
    return f"strategy_{token}"[:120]


def config(
    case_id: str,
    strategy: LoadStrategy,
    tmp_path: Path,
    **overrides: Any,
) -> LoadConfig:
    options = {
        "sink_type": "mssql",
        "batch_commit_mode": "whole",
        "partition_tmp_dir": str(tmp_path),
        "technical_columns": "required",
        "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000}},
    }
    options.update(overrides.pop("options", {}))
    values: dict[str, Any] = {
        "source_conn_id": "postgres_source",
        "target_conn_id": "mssql_sink",
        "source_schema": SOURCE_SCHEMA,
        "source_table": SOURCE_TABLE,
        "target_schema": TARGET_SCHEMA,
        "target_table": target_table(case_id),
        "staging_schema": STAGING_SCHEMA,
        "load_strategy": strategy,
        "export_format": "csv",
        "compress_export": False,
        "options": options,
    }
    values.update(overrides)
    return LoadConfig(**values)


def load_source(runner: GovernedStandardEtlRunner, load_config: LoadConfig):
    """Execute one fully admitted standard-ETL invocation."""

    return runner.run(
        load_config,
        label=f"strategy_{load_config.target_table}",
    )


def drop_target(mssql: Any, load_config: LoadConfig) -> None:
    mssql.execute_query(f"DROP TABLE IF EXISTS [{TARGET_SCHEMA}].[{load_config.target_table}]")


def target_rows(mssql: Any, load_config: LoadConfig) -> list[dict[str, Any]]:
    exists = mssql.get_records(
        "SELECT CASE WHEN OBJECT_ID(?, 'U') IS NULL THEN 0 ELSE 1 END",
        (f"{TARGET_SCHEMA}.{load_config.target_table}",),
    )
    if not exists or not int(exists[0][0]):
        return []
    columns = mssql.get_records(
        "SELECT c.name FROM sys.columns AS c WHERE c.object_id = OBJECT_ID(?) ORDER BY c.column_id",
        (f"{TARGET_SCHEMA}.{load_config.target_table}",),
    )
    names = [str(row[0]) for row in columns]
    order = "[id]"
    if "__dpone__valid_from_at" in names:
        order += ", [__dpone__valid_from_at]"
    selected = ", ".join(f"[{name}]" for name in names)
    return mssql.get_records(
        f"SELECT {selected} FROM [{TARGET_SCHEMA}].[{load_config.target_table}] ORDER BY {order}",
        as_dict=True,
    )


def rows_sha256(rows: list[dict[str, Any]]) -> str:
    encoded = json.dumps(rows, default=str, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


def staging_count(mssql: Any, load_config: LoadConfig) -> int:
    rows = mssql.get_records(
        "SELECT COUNT_BIG(*) FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name LIKE ?",
        (STAGING_SCHEMA, f"stg_{load_config.target_table}_%"),
    )
    return int(rows[0][0]) if rows else 0


def config_evidence(load_config: LoadConfig) -> tuple[str, dict[str, Any]]:
    payload = {
        "mode": load_config.load_strategy.value,
        "only_new_rows": load_config.only_new_rows,
        "overwrite_type": load_config.overwrite_type,
        "merge_policy": load_config.merge_policy,
        "duplicate_policy": load_config.duplicate_policy,
        "unique_key": load_config.unique_key,
        "custom_predicate": load_config.custom_predicate,
        "partition": load_config.partition,
        "diff": load_config.options.get("diff"),
        "scd2": load_config.options.get("scd2"),
        "backfill": load_config.options.get("backfill"),
    }
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode()).hexdigest(), payload


def result_evidence(result: Any) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        raise TypeError("governed strategy result must be a mapping")
    values = dict(result)
    return {key: value for key, value in values.items() if value is not None}


__all__ = [
    "BASE_ROWS",
    "CHANGED_ROWS",
    "SCHEMA",
    "config",
    "config_evidence",
    "drop_target",
    "ensure_source",
    "load_source",
    "result_evidence",
    "rows_sha256",
    "set_source_rows",
    "staging_count",
    "target_rows",
]
