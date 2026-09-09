"""Real-vendor helpers for the finite 10k×128 governed soak."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from dpone.config import LoadConfig, LoadStrategy
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper
from tests.integration.postgres.postgres_mssql_assertions import assert_wide_catalog, canonical_wide_value
from tests.integration.postgres.postgres_mssql_wide_fixtures import (
    SOURCE_SCHEMA,
    WIDE_COLUMN_TARGET,
    WideColumn,
    create_wide_postgres_table,
    wide_columns,
)

TARGET_SCHEMA = "dpone_it"
STAGING_SCHEMA = "staging"
SOAK_SOURCE_TABLE = "postgres_mssql_wide_soak_source"
SOAK_ROW_COUNT = 10_000

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")
_STRATEGIES = {
    "full_refresh": LoadStrategy.FULL_REFRESH,
    "incremental_append": LoadStrategy.INCREMENTAL_APPEND,
    "incremental_merge": LoadStrategy.INCREMENTAL_MERGE,
    "replace": LoadStrategy.REPLACE,
    "partition_replace": LoadStrategy.PARTITION_REPLACE,
    "snapshot_diff": LoadStrategy.SNAPSHOT_DIFF,
    "scd2": LoadStrategy.SCD2,
    "backfill": LoadStrategy.BACKFILL,
}


def create_soak_source(postgres: Any) -> tuple[WideColumn, ...]:
    """Create the canonical 128-column relation and populate exactly 10k rows."""

    columns = tuple(create_wide_postgres_table(postgres, table=SOAK_SOURCE_TABLE, schema=SOURCE_SCHEMA))
    names = ", ".join(_pg_identifier(column.name) for column in columns)
    full_projected: list[str] = []
    sparse_projected: list[str] = []
    for column in columns:
        quoted = _pg_identifier(column.name)
        if column.name == "id":
            full_projected.append("series_id")
            sparse_projected.append("series_id")
        elif column.name == "c_name":
            full_projected.append("'row-' || series_id::text")
            sparse_projected.append("'row-' || series_id::text")
        else:
            full_projected.append(quoted)
            sparse_projected.append(quoted if "NOT NULL" in column.pg_ddl.upper() else "NULL")
    postgres.execute_query(
        f'INSERT INTO "{SOURCE_SCHEMA}"."{SOAK_SOURCE_TABLE}" ({names}) '
        f"SELECT {', '.join(sparse_projected)} "
        f'FROM "{SOURCE_SCHEMA}"."{SOAK_SOURCE_TABLE}" AS seed '
        "CROSS JOIN generate_series(3, 9999) AS series_id WHERE seed.id = 1"
    )
    postgres.execute_query(
        f'INSERT INTO "{SOURCE_SCHEMA}"."{SOAK_SOURCE_TABLE}" ({names}) '
        f"SELECT {', '.join(full_projected)} "
        f'FROM "{SOURCE_SCHEMA}"."{SOAK_SOURCE_TABLE}" AS seed '
        "CROSS JOIN generate_series(10000, 10000) AS series_id WHERE seed.id = 1"
    )
    observed = int(postgres.get_records(f'SELECT COUNT(*) FROM "{SOURCE_SCHEMA}"."{SOAK_SOURCE_TABLE}"')[0][0])
    assert observed == SOAK_ROW_COUNT
    assert len(columns) == WIDE_COLUMN_TARGET
    return columns


def mutate_soak_source(postgres: Any, run_number: int) -> str:
    """Change one full-width row so every measured run performs real mutation."""

    if not 0 <= run_number <= 7:
        raise ValueError("wide_soak.run_number_out_of_range")
    label = "row-one-warmup" if run_number == 0 else f"row-one-run-{run_number:02d}"
    postgres.execute_query(
        f'UPDATE "{SOURCE_SCHEMA}"."{SOAK_SOURCE_TABLE}" SET "c_name" = %s, "updated_at" = %s WHERE "id" = 1',
        (label, f"2026-07-21 12:00:{run_number:02d}"),
    )
    return label


def soak_load_config(
    strategy_name: str,
    *,
    target_database: str,
    work_dir: Path,
) -> LoadConfig:
    """Build one honest complete-snapshot config for a soak strategy."""

    strategy = _STRATEGIES[strategy_name]
    target_table = _safe_identifier(f"wide_soak_{strategy_name}")
    work_dir.mkdir(parents=True, exist_ok=True)
    options: dict[str, Any] = {
        "source_type": "postgres",
        "sink_type": "mssql",
        "batch_commit_mode": "whole",
        "work_dir": str(work_dir),
        "partition_tmp_dir": str(work_dir),
        "technical_columns": "required",
        "bulk": {"mode": "bcp", "bcp": {"batch_size": 10_000, "packet_size": 16384}},
        "schema_contract": {
            "columns": {
                column.name: {"type": "string"}
                for column in wide_columns()
                if PostgresMssqlTypeMapper().resolve(column.pg_type).requires_explicit_contract
            }
        },
    }
    values: dict[str, Any] = {
        "source_conn_id": "postgres_source",
        "target_conn_id": "mssql_sink",
        "source_schema": SOURCE_SCHEMA,
        "source_table": SOAK_SOURCE_TABLE,
        "target_database": target_database,
        "target_schema": TARGET_SCHEMA,
        "target_table": target_table,
        "staging_database": target_database,
        "staging_schema": STAGING_SCHEMA,
        "load_strategy": strategy,
        "export_format": "csv",
        "compress_export": False,
        "options": options,
    }
    if strategy is not LoadStrategy.INCREMENTAL_APPEND and strategy is not LoadStrategy.SCD2:
        options["physical_design"] = {"indexes": {"primary_key": ["id"]}}
    if strategy is LoadStrategy.INCREMENTAL_MERGE:
        values.update(unique_key=["id"], merge_policy="update_insert")
    elif strategy is LoadStrategy.REPLACE:
        values["portable_scope"] = {
            "version": 1,
            "kind": "equality",
            "column": "business_date",
            "value": {"type": "date", "value": "2026-07-21"},
        }
    elif strategy is LoadStrategy.PARTITION_REPLACE:
        values["partition"] = {
            "column": "business_date",
            "values_from_staging": True,
            "max_partitions_per_run": 1,
            "native": False,
            "native_mode": "fallback",
        }
    elif strategy is LoadStrategy.SNAPSHOT_DIFF:
        values["unique_key"] = ["id"]
        options["diff"] = {"compare": "row_hash", "delete_policy": "hard_delete"}
    elif strategy is LoadStrategy.SCD2:
        values["unique_key"] = ["id"]
        options["scd2"] = {"delete_policy": "expire"}
    elif strategy is LoadStrategy.BACKFILL:
        values.update(unique_key=["id"], merge_policy="update_insert")
        options["backfill"] = {"inner_mode": "incremental_merge", "parallel_workers": 1}
        options["physical_design"] = {"indexes": {"primary_key": ["id"]}}
    return LoadConfig(**values)


def target_image(
    mssql: Any,
    config: LoadConfig,
    strategy_name: str,
    expected_name: str | None,
    columns: tuple[WideColumn, ...],
) -> dict[str, Any]:
    """Return a compact exact keyset/catalog image without timing fields."""

    if not mssql.table_exists(config.target_schema, config.target_table, database=config.target_database):
        return {"exists": False}
    current = " WHERE [__dpone__is_current] = 1" if strategy_name == "scd2" else ""
    totals = mssql.get_records(
        f"SELECT COUNT_BIG(*), COUNT_BIG(DISTINCT [id]), MIN([id]), MAX([id]), "
        f"SUM(CONVERT(bigint, [id])) FROM [{config.target_schema}].[{config.target_table}]{current}"
    )[0]
    all_rows = int(
        mssql.get_records(f"SELECT COUNT_BIG(*) FROM [{config.target_schema}].[{config.target_table}]")[0][0]
    )
    business_columns = int(
        mssql.get_records(
            "SELECT COUNT_BIG(*) FROM sys.columns WHERE object_id = OBJECT_ID(?) AND name NOT LIKE '__dpone__%'",
            (f"{config.target_schema}.{config.target_table}",),
        )[0][0]
    )
    named_rows = 0
    if expected_name is not None:
        predicate = " AND [__dpone__is_current] = 1" if strategy_name == "scd2" else ""
        named_rows = int(
            mssql.get_records(
                f"SELECT COUNT_BIG(*) FROM [{config.target_schema}].[{config.target_table}] "
                f"WHERE [id] = 1 AND [c_name] = ?{predicate}",
                (expected_name,),
            )[0][0]
        )
    return {
        "exists": True,
        "active_rows": int(totals[0]),
        "active_distinct_ids": int(totals[1]),
        "minimum_id": int(totals[2]),
        "maximum_id": int(totals[3]),
        "active_id_sum": int(totals[4]),
        "all_rows": all_rows,
        "expected_name": expected_name,
        "expected_name_rows": named_rows,
        "business_column_count": business_columns,
        "catalog_fingerprint": assert_wide_catalog(
            mssql,
            table=config.target_table,
            columns=columns,
            target_schema=config.target_schema,
        ),
    }


def assert_soak_correctness(
    postgres: Any,
    mssql: Any,
    config: LoadConfig,
    strategy_name: str,
    columns: tuple[WideColumn, ...],
    *,
    run_number: int,
    expected_name: str,
) -> dict[str, Any]:
    """Prove exact keyset plus all 128 values on both boundary rows."""

    image = target_image(mssql, config, strategy_name, expected_name, columns)
    expected_all = (
        (run_number + 1) * SOAK_ROW_COUNT
        if strategy_name == "incremental_append"
        else SOAK_ROW_COUNT + run_number
        if strategy_name == "scd2"
        else SOAK_ROW_COUNT
    )
    append_multiplier = run_number + 1 if strategy_name == "incremental_append" else 1
    expected_active = SOAK_ROW_COUNT * append_multiplier
    assert image == {
        "exists": True,
        "active_rows": expected_active,
        "active_distinct_ids": SOAK_ROW_COUNT,
        "minimum_id": 1,
        "maximum_id": SOAK_ROW_COUNT,
        "active_id_sum": SOAK_ROW_COUNT * (SOAK_ROW_COUNT + 1) // 2 * append_multiplier,
        "all_rows": expected_all,
        "expected_name": expected_name,
        "expected_name_rows": 1,
        "business_column_count": WIDE_COLUMN_TARGET,
        "catalog_fingerprint": image["catalog_fingerprint"],
    }
    _assert_boundary_rows(postgres, mssql, config, strategy_name, columns, expected_name=expected_name)
    return image


def staging_count(mssql: Any, config: LoadConfig) -> int:
    """Return the exact number of route-owned staging tables."""

    return len(staging_objects(mssql, config))


def staging_objects(mssql: Any, config: LoadConfig) -> tuple[str, ...]:
    """Return exact staging names from the authored staging database."""

    database_name = config.staging_database or config.target_database
    if database_name is None:
        raise ValueError("wide_soak.staging_database_required")
    database = _safe_identifier(database_name)
    rows = mssql.get_records(
        f"SELECT t.name FROM [{database}].sys.tables AS t "
        f"JOIN [{database}].sys.schemas AS s ON s.schema_id=t.schema_id "
        "WHERE s.name=? AND t.name LIKE ? ORDER BY t.name",
        (config.staging_schema, f"stg_{config.target_table}_%"),
    )
    return tuple(str(row[0]) for row in rows)


def _assert_boundary_rows(
    postgres: Any,
    mssql: Any,
    config: LoadConfig,
    strategy_name: str,
    columns: tuple[WideColumn, ...],
    *,
    expected_name: str,
) -> None:
    names = ", ".join(_pg_identifier(column.name) for column in columns)
    text_names = ", ".join(
        f"{_pg_identifier(column.name)}::text AS {_pg_identifier(column.name)}" for column in columns
    )
    source_rows = postgres.get_records(
        f'SELECT {names} FROM "{SOURCE_SCHEMA}"."{SOAK_SOURCE_TABLE}" WHERE id IN (1, 10000) ORDER BY id',
        as_dict=True,
    )
    source_text = postgres.get_records(
        f'SELECT {text_names} FROM "{SOURCE_SCHEMA}"."{SOAK_SOURCE_TABLE}" WHERE id IN (1, 10000) ORDER BY id',
        as_dict=True,
    )
    assert len(source_rows) == len(source_text) == 2
    target_names = ", ".join(f"[{column.name}]" for column in columns)
    for source_row, text_row in zip(source_rows, source_text, strict=True):
        row_id = int(source_row["id"])
        predicates = ["[id] = ?"]
        params: list[Any] = [row_id]
        if row_id == 1:
            predicates.append("[c_name] = ?")
            params.append(expected_name)
        if strategy_name == "scd2":
            predicates.append("[__dpone__is_current] = 1")
        target = mssql.get_records(
            f"SELECT TOP (1) {target_names} FROM [{config.target_schema}].[{config.target_table}] "
            f"WHERE {' AND '.join(predicates)} ORDER BY [__dpone__loaded_at] DESC",
            tuple(params),
            as_dict=True,
        )
        assert len(target) == 1
        for column in columns:
            expected = canonical_wide_value(column, source_row[column.name], serialized=text_row[column.name])
            actual = canonical_wide_value(column, target[0][column.name], serialized=target[0][column.name])
            assert actual == expected, f"{strategy_name}.run boundary id={row_id}.{column.name}"


def _safe_identifier(value: str) -> str:
    if _SAFE_IDENTIFIER.fullmatch(value) is None:
        raise ValueError("wide_soak.identifier_invalid")
    return value


def _pg_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


__all__ = [
    "SOAK_ROW_COUNT",
    "SOAK_SOURCE_TABLE",
    "assert_soak_correctness",
    "create_soak_source",
    "mutate_soak_source",
    "soak_load_config",
    "staging_count",
    "staging_objects",
    "target_image",
]
