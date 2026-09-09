"""Typed pre-staging and vendor data-guard cases for the strategy matrix."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.config import LoadStrategy
from tests.integration.postgres.postgres_mssql_strategy_matrix_support import config


def pre_staging_rejections(tmp_path: Path) -> list[tuple[str, Any, str]]:
    cases: list[tuple[str, Any, str]] = [
        (
            "reject__full__exchange",
            config("reject__full__exchange", LoadStrategy.FULL_REFRESH, tmp_path, overwrite_type="exchange"),
            "mssql.strategy.full_refresh.exchange_physical_preservation",
        ),
        (
            "reject__full__unknown",
            config("reject__full__unknown", LoadStrategy.FULL_REFRESH, tmp_path, overwrite_type="unknown"),
            "mssql.strategy.full_refresh.overwrite_type",
        ),
        (
            "reject__append__missing_key",
            config(
                "reject__append__missing_key",
                LoadStrategy.INCREMENTAL_APPEND,
                tmp_path,
                only_new_rows=True,
            ),
            "mssql.strategy.incremental_append.unique_key",
        ),
        (
            "reject__merge__missing_key",
            config("reject__merge__missing_key", LoadStrategy.INCREMENTAL_MERGE, tmp_path),
            "mssql.strategy.incremental_merge.unique_key",
        ),
        (
            "reject__merge__duplicate_policy",
            config(
                "reject__merge__duplicate_policy",
                LoadStrategy.INCREMENTAL_MERGE,
                tmp_path,
                unique_key=["id"],
                duplicate_policy="last_write_wins",
            ),
            "mssql.strategy.incremental_merge.duplicate_policy",
        ),
        (
            "reject__merge__duplicate_key_declaration",
            config(
                "reject__merge__duplicate_key_declaration",
                LoadStrategy.INCREMENTAL_MERGE,
                tmp_path,
                unique_key=["id", "id"],
            ),
            "mssql.strategy.unique_key",
        ),
        (
            "reject__merge__clickhouse_override",
            config(
                "reject__merge__clickhouse_override",
                LoadStrategy.INCREMENTAL_MERGE,
                tmp_path,
                unique_key=["id"],
                allow_non_recommended_policy=True,
            ),
            "mssql.strategy.incremental_merge.allow_non_recommended_policy",
        ),
        (
            "reject__merge__mutations_sync",
            config(
                "reject__merge__mutations_sync",
                LoadStrategy.INCREMENTAL_MERGE,
                tmp_path,
                unique_key=["id"],
                mutations_sync=1,
            ),
            "mssql.strategy.incremental_merge.mutations_sync",
        ),
        (
            "merge__shadow_swap",
            config(
                "merge__shadow_swap",
                LoadStrategy.INCREMENTAL_MERGE,
                tmp_path,
                unique_key=["id"],
                merge_policy="shadow_swap",
            ),
            "mssql.strategy.incremental_merge.shadow_swap_physical_preservation",
        ),
        (
            "data_reject__merge_duplicate__shadow_swap",
            config(
                "data_reject__merge_duplicate__shadow_swap",
                LoadStrategy.INCREMENTAL_MERGE,
                tmp_path,
                unique_key=["id"],
                merge_policy="shadow_swap",
            ),
            "mssql.strategy.incremental_merge.shadow_swap_physical_preservation",
        ),
        (
            "reject__replace__missing_predicate",
            config("reject__replace__missing_predicate", LoadStrategy.REPLACE, tmp_path),
            "mssql.strategy.replace.custom_predicate",
        ),
        (
            "reject__partition__values_external",
            config(
                "reject__partition__values_external",
                LoadStrategy.PARTITION_REPLACE,
                tmp_path,
                partition={"column": "partition_id", "values_from_staging": False},
            ),
            "mssql.strategy.partition_replace.values_from_staging",
        ),
        (
            "reject__partition__expression",
            config(
                "reject__partition__expression",
                LoadStrategy.PARTITION_REPLACE,
                tmp_path,
                partition={"column": "partition_id", "value_expression": "partition_id + 1"},
            ),
            "mssql.strategy.partition_replace.value_expression",
        ),
        (
            "reject__partition__required",
            config(
                "reject__partition__required",
                LoadStrategy.PARTITION_REPLACE,
                tmp_path,
                partition={"column": "partition_id", "native_mode": "required"},
            ),
            "mssql.strategy.partition_replace.native_switch",
        ),
        (
            "reject__partition__limit_zero",
            config(
                "reject__partition__limit_zero",
                LoadStrategy.PARTITION_REPLACE,
                tmp_path,
                partition={"column": "partition_id", "max_partitions_per_run": 0},
            ),
            "mssql.strategy.partition_replace.max_partitions_per_run",
        ),
        (
            "reject__partition__missing_column",
            config(
                "reject__partition__missing_column",
                LoadStrategy.PARTITION_REPLACE,
                tmp_path,
                partition={"native_mode": "fallback"},
            ),
            "mssql.strategy.partition_replace.column",
        ),
        (
            "reject__partition__unknown_native_mode",
            config(
                "reject__partition__unknown_native_mode",
                LoadStrategy.PARTITION_REPLACE,
                tmp_path,
                partition={"column": "partition_id", "native_mode": "unknown"},
            ),
            "mssql.strategy.partition_replace.native_mode",
        ),
        (
            "reject__snapshot__missing_key",
            config("reject__snapshot__missing_key", LoadStrategy.SNAPSHOT_DIFF, tmp_path),
            "mssql.strategy.snapshot_diff.unique_key",
        ),
        (
            "reject__snapshot__compare",
            config(
                "reject__snapshot__compare",
                LoadStrategy.SNAPSHOT_DIFF,
                tmp_path,
                unique_key=["id"],
                options={"diff": {"compare": "checksum"}},
            ),
            "mssql.strategy.snapshot_diff.compare",
        ),
        (
            "reject__snapshot__delete",
            config(
                "reject__snapshot__delete",
                LoadStrategy.SNAPSHOT_DIFF,
                tmp_path,
                unique_key=["id"],
                options={"diff": {"delete_policy": "truncate"}},
            ),
            "mssql.strategy.snapshot_diff.delete_policy",
        ),
        (
            "reject__scd2__missing_key",
            config("reject__scd2__missing_key", LoadStrategy.SCD2, tmp_path),
            "mssql.strategy.scd2.unique_key",
        ),
        (
            "reject__scd2__hard_marker",
            config(
                "reject__scd2__hard_marker",
                LoadStrategy.SCD2,
                tmp_path,
                unique_key=["id"],
                options={"scd2": {"delete_policy": "hard_delete_not_supported"}},
            ),
            "mssql.strategy.scd2.delete_policy",
        ),
        (
            "reject__scd2__custom_hash",
            config(
                "reject__scd2__custom_hash",
                LoadStrategy.SCD2,
                tmp_path,
                unique_key=["id"],
                options={"scd2": {"row_hash_column": "custom_hash"}},
            ),
            "mssql.strategy.scd2.row_hash_column",
        ),
        (
            "reject__backfill__full_refresh",
            config(
                "reject__backfill__full_refresh",
                LoadStrategy.BACKFILL,
                tmp_path,
                options={"backfill": {"inner_mode": "full_refresh"}},
            ),
            "mssql.strategy.backfill.inner_mode",
        ),
        (
            "reject__backfill__workers_zero",
            config(
                "reject__backfill__workers_zero",
                LoadStrategy.BACKFILL,
                tmp_path,
                partition={"column": "partition_id"},
                options={"backfill": {"inner_mode": "partition_replace", "parallel_workers": 0}},
            ),
            "mssql_transaction.backfill_execution_policy_invalid",
        ),
        (
            "reject__backfill__partition_missing_column",
            config(
                "reject__backfill__partition_missing_column",
                LoadStrategy.BACKFILL,
                tmp_path,
                options={"backfill": {"inner_mode": "partition_replace"}},
            ),
            "mssql.strategy.partition_replace.column",
        ),
        (
            "reject__backfill__replace_missing_predicate",
            config(
                "reject__backfill__replace_missing_predicate",
                LoadStrategy.BACKFILL,
                tmp_path,
                options={"backfill": {"inner_mode": "replace"}},
            ),
            "mssql.strategy.backfill.scope_required",
        ),
        (
            "reject__backfill__merge_missing_key",
            config(
                "reject__backfill__merge_missing_key",
                LoadStrategy.BACKFILL,
                tmp_path,
                options={"backfill": {"inner_mode": "incremental_merge"}},
            ),
            "mssql.strategy.incremental_merge.unique_key",
        ),
        (
            "reject__backfill__unknown_inner",
            config(
                "reject__backfill__unknown_inner",
                LoadStrategy.BACKFILL,
                tmp_path,
                options={"backfill": {"inner_mode": "unknown"}},
            ),
            "mssql_transaction.backfill_execution_policy_invalid",
        ),
        (
            "reject__cdc",
            config("reject__cdc", LoadStrategy.CDC_APPLY, tmp_path, unique_key=["id"]),
            "mssql.strategy.mode",
        ),
        (
            "reject__irrelevant_diff",
            config(
                "reject__irrelevant_diff",
                LoadStrategy.FULL_REFRESH,
                tmp_path,
                options={"diff": {"compare": "row_hash"}},
            ),
            "mssql.strategy.full_refresh.irrelevant_diff",
        ),
    ]
    for policy in (
        "lightweight_delete_insert",
        "mutation_delete_insert",
        "event_upsert",
        "unknown",
    ):
        case_id = f"reject__merge__{policy}"
        cases.append(
            (
                case_id,
                config(
                    case_id,
                    LoadStrategy.INCREMENTAL_MERGE,
                    tmp_path,
                    unique_key=["id"],
                    merge_policy=policy,
                ),
                "mssql.strategy.incremental_merge.merge_policy",
            )
        )
    for native in (False, True):
        for native_mode in ("auto", "fallback", "required"):
            for require_native in (False, True):
                if native_mode != "required" and not require_native:
                    continue
                case_id = (
                    f"reject__partition_product__native_{str(native).lower()}__"
                    f"{native_mode}__require_{str(require_native).lower()}"
                )
                cases.append(
                    (
                        case_id,
                        config(
                            case_id,
                            LoadStrategy.PARTITION_REPLACE,
                            tmp_path,
                            partition={
                                "column": "partition_id",
                                "native": native,
                                "native_mode": native_mode,
                                "require_native": require_native,
                            },
                        ),
                        "mssql.strategy.partition_replace.native_switch",
                    )
                )
    for hint in ("partition_function", "switch_out_schema", "switch_out_table_template"):
        case_id = f"reject__partition_hint__{hint}"
        cases.append(
            (
                case_id,
                config(
                    case_id,
                    LoadStrategy.PARTITION_REPLACE,
                    tmp_path,
                    partition={"column": "partition_id", hint: "vendor_hint"},
                ),
                "mssql.strategy.partition_replace.native_switch",
            )
        )
    for field in (
        "valid_from_column",
        "valid_to_column",
        "current_flag_column",
    ):
        case_id = f"reject__scd2__custom_{field}"
        cases.append(
            (
                case_id,
                config(
                    case_id,
                    LoadStrategy.SCD2,
                    tmp_path,
                    unique_key=["id"],
                    options={"scd2": {field: f"custom_{field}"}},
                ),
                f"mssql.strategy.scd2.{field}",
            )
        )
    return cases


def staged_data_rejections(tmp_path: Path) -> list[tuple[str, Any, str, tuple[tuple[Any, Any, Any], ...]]]:
    cases = []
    duplicate_rows = ((1, 1, "duplicate-a"), (1, 2, "duplicate-b"))
    for policy in ("auto", "update_insert", "delete_insert"):
        case_id = f"data_reject__merge_duplicate__{policy}"
        cases.append(
            (
                case_id,
                config(
                    case_id,
                    LoadStrategy.INCREMENTAL_MERGE,
                    tmp_path,
                    unique_key=["id"],
                    merge_policy=policy,
                ),
                "duplicate_unique_key",
                duplicate_rows,
            )
        )
    cases.extend(
        [
            (
                "data_reject__append_duplicate",
                config(
                    "data_reject__append_duplicate",
                    LoadStrategy.INCREMENTAL_APPEND,
                    tmp_path,
                    only_new_rows=True,
                    unique_key=["id"],
                ),
                "duplicate_unique_key",
                duplicate_rows,
            ),
            (
                "data_reject__partition_limit",
                config(
                    "data_reject__partition_limit",
                    LoadStrategy.PARTITION_REPLACE,
                    tmp_path,
                    partition={
                        "column": "partition_id",
                        "native_mode": "fallback",
                        "max_partitions_per_run": 1,
                    },
                ),
                "partition_limit",
                ((1, 1, "one"), (2, 2, "two")),
            ),
            (
                "data_reject__partition_null",
                config(
                    "data_reject__partition_null",
                    LoadStrategy.PARTITION_REPLACE,
                    tmp_path,
                    partition={"column": "partition_id", "native_mode": "fallback"},
                ),
                "partition_null",
                ((1, None, "null-partition"),),
            ),
        ]
    )
    return cases


__all__ = ["pre_staging_rejections", "staged_data_rejections"]
