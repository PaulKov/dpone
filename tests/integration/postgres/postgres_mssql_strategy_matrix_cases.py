"""Finite authored configurations for the MSSQL vendor capability matrix."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.config import LoadStrategy
from tests.integration.postgres.postgres_mssql_strategy_matrix_support import config


def supported_cases(tmp_path: Path) -> list[tuple[str, Any, str]]:
    cases: list[tuple[str, Any, str]] = [
        ("full__truncate_insert", config("full__truncate_insert", LoadStrategy.FULL_REFRESH, tmp_path), "full"),
        ("append__all", config("append__all", LoadStrategy.INCREMENTAL_APPEND, tmp_path), "append_all"),
        (
            "append__only_new",
            config(
                "append__only_new",
                LoadStrategy.INCREMENTAL_APPEND,
                tmp_path,
                only_new_rows=True,
                unique_key=["id"],
            ),
            "append_only_new",
        ),
        (
            "replace__predicate",
            config(
                "replace__predicate",
                LoadStrategy.REPLACE,
                tmp_path,
                custom_predicate="partition_id = 1",
            ),
            "replace",
        ),
    ]
    for policy in ("auto", "update_insert", "delete_insert"):
        case_id = f"merge__{policy}"
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
                "merge",
            )
        )
    for native in (False, True):
        for native_mode in ("auto", "fallback"):
            case_id = f"partition__native_{str(native).lower()}__{native_mode}"
            cases.append(
                (
                    case_id,
                    config(
                        case_id,
                        LoadStrategy.PARTITION_REPLACE,
                        tmp_path,
                        partition={
                            "column": "partition_id",
                            "values_from_staging": True,
                            "max_partitions_per_run": 1,
                            "native": native,
                            "native_mode": native_mode,
                            "require_native": False,
                        },
                    ),
                    "partition",
                )
            )
    cases.append(
        (
            "partition__limit_equality",
            config(
                "partition__limit_equality",
                LoadStrategy.PARTITION_REPLACE,
                tmp_path,
                partition={
                    "column": "partition_id",
                    "max_partitions_per_run": 2,
                    "native_mode": "fallback",
                },
            ),
            "partition_limit_equality",
        )
    )
    cases.append(
        (
            "partition__limit_large_4096",
            config(
                "partition__limit_large_4096",
                LoadStrategy.PARTITION_REPLACE,
                tmp_path,
                partition={
                    "column": "partition_id",
                    "max_partitions_per_run": 4096,
                    "native_mode": "fallback",
                },
            ),
            "partition",
        )
    )
    for compare in ("row_hash", "all_columns"):
        for delete_policy in ("ignore", "hard_delete", "soft_delete"):
            case_id = f"snapshot__{compare}__{delete_policy}"
            cases.append(
                (
                    case_id,
                    config(
                        case_id,
                        LoadStrategy.SNAPSHOT_DIFF,
                        tmp_path,
                        unique_key=["id"],
                        options={"diff": {"compare": compare, "delete_policy": delete_policy}},
                    ),
                    "snapshot",
                )
            )
    for delete_policy in ("expire", "ignore"):
        case_id = f"scd2__{delete_policy}"
        cases.append(
            (
                case_id,
                config(
                    case_id,
                    LoadStrategy.SCD2,
                    tmp_path,
                    unique_key=["id"],
                    options={"scd2": {"delete_policy": delete_policy}},
                ),
                "scd2",
            )
        )
    for inner_mode in ("partition_replace", "replace", "incremental_merge"):
        for workers in (1, 2):
            case_id = f"backfill__{inner_mode}__authored_workers_{workers}__single_invocation"
            kwargs: dict[str, Any] = {"options": {"backfill": {"inner_mode": inner_mode, "parallel_workers": workers}}}
            if inner_mode == "partition_replace":
                kwargs["partition"] = {"column": "partition_id", "native_mode": "fallback"}
            elif inner_mode == "replace":
                kwargs["custom_predicate"] = "partition_id = 1"
            else:
                kwargs["unique_key"] = ["id"]
            cases.append(
                (
                    case_id,
                    config(case_id, LoadStrategy.BACKFILL, tmp_path, **kwargs),
                    f"backfill_{inner_mode}",
                )
            )
    return cases


def empty_cases(tmp_path: Path) -> list[tuple[str, Any, str]]:
    return [
        ("empty__full", config("empty__full", LoadStrategy.FULL_REFRESH, tmp_path), "full"),
        ("empty__append", config("empty__append", LoadStrategy.INCREMENTAL_APPEND, tmp_path), "append"),
        (
            "empty__merge",
            config("empty__merge", LoadStrategy.INCREMENTAL_MERGE, tmp_path, unique_key=["id"]),
            "merge",
        ),
        (
            "empty__replace",
            config(
                "empty__replace",
                LoadStrategy.REPLACE,
                tmp_path,
                custom_predicate="partition_id = 1",
            ),
            "replace",
        ),
        (
            "empty__partition",
            config(
                "empty__partition",
                LoadStrategy.PARTITION_REPLACE,
                tmp_path,
                partition={"column": "partition_id", "native_mode": "fallback"},
            ),
            "partition",
        ),
        (
            "empty__snapshot",
            config(
                "empty__snapshot",
                LoadStrategy.SNAPSHOT_DIFF,
                tmp_path,
                unique_key=["id"],
                options={"diff": {"delete_policy": "hard_delete"}},
            ),
            "snapshot_reject",
        ),
        (
            "empty__scd2",
            config(
                "empty__scd2",
                LoadStrategy.SCD2,
                tmp_path,
                unique_key=["id"],
                options={"scd2": {"delete_policy": "expire"}},
            ),
            "scd2_reject",
        ),
        (
            "empty__backfill",
            config(
                "empty__backfill",
                LoadStrategy.BACKFILL,
                tmp_path,
                custom_predicate="partition_id = 1",
                options={"backfill": {"inner_mode": "replace"}},
            ),
            "backfill_replace",
        ),
    ]


def production_route_rejections(tmp_path: Path) -> list[tuple[str, Any]]:
    """Standard PostgreSQL source routes blocked by generic atomic admission."""

    return [
        (
            "route_reject__append__xmin_generic_atomicity",
            config(
                "route_reject__append__xmin_generic_atomicity",
                LoadStrategy.INCREMENTAL_APPEND,
                tmp_path,
            ),
        ),
        (
            "route_reject__merge__xmin_generic_atomicity",
            config(
                "route_reject__merge__xmin_generic_atomicity",
                LoadStrategy.INCREMENTAL_MERGE,
                tmp_path,
                unique_key=["id"],
            ),
        ),
    ]


__all__ = ["empty_cases", "production_route_rejections", "supported_cases"]
