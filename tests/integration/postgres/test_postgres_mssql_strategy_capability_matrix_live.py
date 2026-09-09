"""Real PostgreSQL→SQL Server capability matrix for every MSSQL submode.

The 128-column route test certifies source export and type transport. This
matrix holds that transport invariant and exhaustively exercises the finite
typed strategy policies against the same PostgreSQL 16 / SQL Server 2022
containers. Unbounded numeric knobs use boundary equivalence partitions.
"""

from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import ObservedImageDigest, RouteLiveObservationRecorder

from dpone.config import LoadStrategy
from dpone.config.mssql_strategy_contract import MSSQLStrategyContractError
from dpone.contracts.mssql_transaction_governance import MssqlTransactionContractError
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from tests.integration.postgres.postgres_live_support import (
    ensure_mssql_database_and_schemas,
    mssql_connector,
    postgres_connector,
    postgres_mssql_enabled,
    wait_until_ready,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedPostgresSnapshotSource,
    GovernedStandardEtlRunner,
    governed_mssql_campaign,
)
from tests.integration.postgres.postgres_mssql_strategy_matrix_cases import (
    empty_cases,
    production_route_rejections,
    supported_cases,
)
from tests.integration.postgres.postgres_mssql_strategy_matrix_rejections import (
    pre_staging_rejections,
    staged_data_rejections,
)
from tests.integration.postgres.postgres_mssql_strategy_matrix_support import (
    BASE_ROWS,
    CHANGED_ROWS,
    config_evidence,
    drop_target,
    ensure_source,
    load_source,
    result_evidence,
    rows_sha256,
    set_source_rows,
    staging_count,
    target_rows,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    QuietIntegrationLogger,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
]

_EVIDENCE = Path("test_artifacts/live_certification/postgres_mssql_strategy_capability_matrix.json")
_INVOCATION_LINEAGE_COLUMNS = frozenset(
    {
        "__dpone__run_id",
        "__dpone__load_id",
        "__dpone__loaded_at",
        "__dpone__extracted_at",
    }
)


def _baseline(postgres: Any, runner: GovernedStandardEtlRunner, config: Any) -> None:
    options = dict(config.options)
    for section in ("diff", "scd2", "backfill", "cdc"):
        options.pop(section, None)
    baseline = replace(
        config,
        load_strategy=LoadStrategy.FULL_REFRESH,
        unique_key=None,
        only_new_rows=False,
        overwrite_type=None,
        merge_policy="auto",
        duplicate_policy="fail",
        partition={},
        custom_predicate=None,
        options=options,
    )
    set_source_rows(postgres, BASE_ROWS)
    load_source(runner, baseline)


def _business(rows: list[dict[str, Any]]) -> dict[int, tuple[int | None, str | None]]:
    return {
        int(row["id"]): (
            int(row["partition_id"]) if row["partition_id"] is not None else None,
            row["value"],
        )
        for row in rows
        if row.get("__dpone__is_current", True)
    }


def _semantic_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Exclude invocation identity while retaining strategy-owned state."""

    return [{key: value for key, value in row.items() if key not in _INVOCATION_LINEAGE_COLUMNS} for row in rows]


def _record(
    config: Any,
    case_id: str,
    category: str,
    *,
    before: list[dict[str, Any]],
    after: list[dict[str, Any]],
    actions: list[dict[str, Any]],
    staging_after: int,
    before_image_required: bool = False,
) -> dict[str, Any]:
    config_sha, authored = config_evidence(config)
    before_sha = rows_sha256(_semantic_rows(before))
    after_sha = rows_sha256(_semantic_rows(after))
    return {
        "case_id": case_id,
        "category": category,
        "config_sha256": config_sha,
        "authored_config": authored,
        "actions": actions,
        "before_sha256": before_sha,
        "after_sha256": after_sha,
        "raw_before_sha256": rows_sha256(before),
        "raw_after_sha256": rows_sha256(after),
        "before_image_preserved": before_sha == after_sha,
        "before_image_required": before_image_required,
        "staging_objects_after": staging_after,
        "passed": True,
    }


def _run_supported(
    postgres: Any,
    mssql: Any,
    runner: GovernedStandardEtlRunner,
    case_id: str,
    config: Any,
    scenario: str,
):
    drop_target(mssql, config)
    actions: list[dict[str, Any]] = []
    if scenario in {"snapshot", "scd2"}:
        set_source_rows(postgres, BASE_ROWS)
        initial = load_source(runner, config)
        actions.append({"name": "baseline", "result": result_evidence(initial)})
    elif config.unique_key:
        # A keyed target is born through the keyed strategy so its exact type,
        # nullability, BIN2 collation and UNIQUE authority are present from the
        # first row.  A keyless full-refresh baseline would be a different
        # physical contract and must now fail closed.
        set_source_rows(postgres, BASE_ROWS)
        initial = load_source(runner, config)
        actions.append({"name": "keyed_baseline", "result": result_evidence(initial)})
    else:
        _baseline(postgres, runner, config)
    before = target_rows(mssql, config)

    if scenario == "full":
        mssql.execute_query(
            f"CREATE UNIQUE INDEX [ux_{config.target_table}_id] ON [dpone_it].[{config.target_table}] ([id])"
        )
        set_source_rows(postgres, CHANGED_ROWS)
        result = load_source(runner, config)
        actions.append({"name": "truncate_insert", "result": result_evidence(result)})
        index_count = mssql.get_records(
            "SELECT COUNT_BIG(*) FROM sys.indexes WHERE object_id = OBJECT_ID(?) AND name = ?",
            (f"dpone_it.{config.target_table}", f"ux_{config.target_table}_id"),
        )[0][0]
        assert int(index_count) == 1
        assert _business(target_rows(mssql, config)) == {1: (1, "new-one"), 3: (1, "new-three")}
    elif scenario.startswith("append"):
        set_source_rows(postgres, CHANGED_ROWS)
        first = load_source(runner, config)
        actions.append({"name": "first", "result": result_evidence(first)})
        if scenario == "append_only_new":
            second = load_source(runner, config)
            actions.append({"name": "identical_rerun", "result": result_evidence(second)})
            assert second.inserted_rows == 0
            assert len(target_rows(mssql, config)) == 3
        else:
            assert len(target_rows(mssql, config)) == 4
    elif scenario in {"merge", "replace", "partition", "partition_limit_equality"}:
        rows = CHANGED_ROWS if scenario != "partition_limit_equality" else BASE_ROWS
        set_source_rows(postgres, rows)
        first = load_source(runner, config)
        stable_sha = rows_sha256(_semantic_rows(target_rows(mssql, config)))
        second = load_source(runner, config)
        actions.extend(
            [
                {"name": "first", "result": result_evidence(first)},
                {"name": "identical_rerun", "result": result_evidence(second)},
            ]
        )
        assert rows_sha256(_semantic_rows(target_rows(mssql, config))) == stable_sha
        expected = (
            {1: (1, rows[0][2]), 2: (2, rows[1][2])}
            if scenario == "partition_limit_equality"
            else {
                1: (1, "new-one"),
                2: (2, "old-two"),
                3: (1, "new-three"),
            }
        )
        assert _business(target_rows(mssql, config)) == expected
    elif scenario == "snapshot":
        compare = config.options["diff"]["compare"]
        delete_policy = config.options["diff"]["delete_policy"]
        mssql.execute_query(
            f"UPDATE [dpone_it].[{config.target_table}] SET [__dpone__row_hash] = REPLICATE('0', 64) WHERE id = 1"
        )
        set_source_rows(postgres, BASE_ROWS)
        compare_probe = load_source(runner, config)
        actions.append({"name": "compare_probe", "result": result_evidence(compare_probe)})
        assert compare_probe.updated_rows == (1 if compare == "row_hash" else 0)
        set_source_rows(postgres, CHANGED_ROWS)
        changed = load_source(runner, config)
        actions.append({"name": "delete_change", "result": result_evidence(changed)})
        if delete_policy == "ignore":
            assert 2 in _business(target_rows(mssql, config))
        elif delete_policy == "hard_delete":
            assert 2 not in _business(target_rows(mssql, config))
        else:
            deleted = next(row for row in target_rows(mssql, config) if int(row["id"]) == 2)
            deleted_at = deleted["__dpone__deleted_at"]
            repeated = load_source(runner, config)
            repeated_row = next(row for row in target_rows(mssql, config) if int(row["id"]) == 2)
            assert repeated_row["__dpone__deleted_at"] == deleted_at
            actions.append({"name": "repeat_absence", "result": result_evidence(repeated)})
            set_source_rows(postgres, (*CHANGED_ROWS, (2, 2, "old-two")))
            reactivated = load_source(runner, config)
            active = next(row for row in target_rows(mssql, config) if int(row["id"]) == 2)
            assert active["__dpone__deleted_at"] is None
            assert reactivated.reactivated_rows == 1
            actions.append({"name": "reactivate", "result": result_evidence(reactivated)})
    elif scenario == "scd2":
        identical = load_source(runner, config)
        assert len(target_rows(mssql, config)) == 2
        set_source_rows(postgres, CHANGED_ROWS)
        changed = load_source(runner, config)
        actions.extend(
            [
                {"name": "identical_rerun", "result": result_evidence(identical)},
                {"name": "change_delete", "result": result_evidence(changed)},
            ]
        )
        current_ids = {int(row["id"]) for row in target_rows(mssql, config) if bool(row["__dpone__is_current"])}
        assert (2 in current_ids) is (config.options["scd2"]["delete_policy"] == "ignore")
    elif scenario.startswith("backfill_"):
        set_source_rows(postgres, CHANGED_ROWS)
        first = load_source(runner, config)
        stable_sha = rows_sha256(_semantic_rows(target_rows(mssql, config)))
        second = load_source(runner, config)
        assert rows_sha256(_semantic_rows(target_rows(mssql, config))) == stable_sha
        actions.extend(
            [
                {"name": "first", "result": result_evidence(first)},
                {"name": "identical_rerun", "result": result_evidence(second)},
            ]
        )
        assert _business(target_rows(mssql, config)) == {
            1: (1, "new-one"),
            2: (2, "old-two"),
            3: (1, "new-three"),
        }
    else:  # pragma: no cover - completeness guard below owns the enum
        raise AssertionError(f"unknown supported scenario {scenario}")
    after = target_rows(mssql, config)
    assert staging_count(mssql, config) == 0
    return _record(config, case_id, "supported", before=before, after=after, actions=actions, staging_after=0)


def _run_empty(
    postgres: Any,
    mssql: Any,
    runner: GovernedStandardEtlRunner,
    case_id: str,
    config: Any,
    scenario: str,
):
    drop_target(mssql, config)
    base_scenario = scenario.removesuffix("_reject")
    if base_scenario in {"snapshot", "scd2"} or config.unique_key:
        set_source_rows(postgres, BASE_ROWS)
        load_source(runner, config)
    else:
        _baseline(postgres, runner, config)
    before = target_rows(mssql, config)
    set_source_rows(postgres, ())
    if scenario.endswith("_reject"):
        strategy_name = {"snapshot": "snapshot_diff", "scd2": "scd2"}[base_scenario]
        expected_error = f"mssql.strategy.{strategy_name}.empty_destructive_snapshot"
        with pytest.raises(SnapshotReconciliationError) as raised:
            load_source(runner, config)
        assert str(raised.value) == expected_error
        after = target_rows(mssql, config)
        assert rows_sha256(_semantic_rows(after)) == rows_sha256(_semantic_rows(before))
        assert staging_count(mssql, config) == 0
        return _record(
            config,
            case_id,
            "empty_destructive_snapshot_guard",
            before=before,
            after=after,
            actions=[{"name": "vendor_guard", "error": expected_error}],
            staging_after=0,
            before_image_required=True,
        )
    result = load_source(runner, config)
    after = target_rows(mssql, config)
    expected_counts = {
        "full": 0,
        "append": 2,
        "merge": 2,
        "replace": 1,
        "partition": 2,
        "snapshot": 0,
        "scd2": 2,
        "backfill_replace": 1,
    }
    assert len(after) == expected_counts[base_scenario]
    assert staging_count(mssql, config) == 0
    return _record(
        config,
        case_id,
        "empty_input",
        before=before,
        after=after,
        actions=[{"name": "empty", "result": result_evidence(result)}],
        staging_after=0,
    )


def _run_pre_staging_reject(
    mssql: Any,
    runner: GovernedStandardEtlRunner,
    case_id: str,
    config: Any,
    blocker: str,
):
    drop_target(mssql, config)
    mssql.execute_query(
        f"CREATE TABLE [dpone_it].[{config.target_table}] "
        "([id] int NOT NULL, [partition_id] int NULL, [value] nvarchar(max) NULL)"
    )
    mssql.execute_query(f"INSERT INTO [dpone_it].[{config.target_table}] VALUES (1, 1, N'before-image')")
    before = target_rows(mssql, config)
    with pytest.raises((MSSQLStrategyContractError, MssqlTransactionContractError)) as raised:
        runner.run(config, label=f"strategy_reject_{config.target_table}")
    error = raised.value
    actual_blocker = error.blocker if isinstance(error, MSSQLStrategyContractError) else str(error)
    assert actual_blocker == blocker
    after = target_rows(mssql, config)
    assert rows_sha256(after) == rows_sha256(before)
    assert staging_count(mssql, config) == 0
    return _record(
        config,
        case_id,
        "unsupported_pre_staging",
        before=before,
        after=after,
        actions=[
            {
                "name": "typed_reject",
                "error_code": getattr(error, "code", actual_blocker),
                "blocker": blocker,
            }
        ],
        staging_after=0,
        before_image_required=True,
    )


def _run_staged_reject(
    postgres: Any,
    mssql: Any,
    runner: GovernedStandardEtlRunner,
    case_id: str,
    config: Any,
    reason: str,
    rows: tuple[tuple[Any, Any, Any], ...],
):
    drop_target(mssql, config)
    _baseline(postgres, runner, config)
    before = target_rows(mssql, config)
    set_source_rows(postgres, rows)
    error_type, expected_error = {
        "duplicate_unique_key": (
            SnapshotReconciliationError,
            "mssql_native_projection.key_sql_equivalence_collision",
        ),
        "partition_limit": (
            ValueError,
            "MSSQL partition_replace would replace 2 partitions, above max_partitions_per_run=1.",
        ),
        "partition_null": (
            ValueError,
            "MSSQL partition_replace rejects NULL partition identities before target mutation.",
        ),
    }[reason]
    with pytest.raises(error_type) as raised:
        load_source(runner, config)
    assert str(raised.value) == expected_error
    after = target_rows(mssql, config)
    assert rows_sha256(after) == rows_sha256(before)
    assert staging_count(mssql, config) == 0
    return _record(
        config,
        case_id,
        "staged_data_guard",
        before=before,
        after=after,
        actions=[{"name": "vendor_guard", "reason": reason, "error": expected_error}],
        staging_after=0,
        before_image_required=True,
    )


def _run_production_route_reject(
    postgres: Any,
    mssql: Any,
    runner: GovernedStandardEtlRunner,
    case_id: str,
    config: Any,
):
    """Prove the real XMin route fails before target or staging mutation."""

    drop_target(mssql, config)
    set_source_rows(postgres, BASE_ROWS)
    before = target_rows(mssql, config)
    with pytest.raises(RuntimeError) as raised:
        load_source(runner, config)
    assert str(raised.value) == "mssql_transaction.source_checkpoint_not_atomic:external_nonatomic"
    after = target_rows(mssql, config)
    assert after == before == []
    assert staging_count(mssql, config) == 0
    return _record(
        config,
        case_id,
        "production_route_atomicity_guard",
        before=before,
        after=after,
        actions=[{"name": "admission", "error": str(raised.value)}],
        staging_after=0,
        before_image_required=True,
    )


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_strategy_capability_matrix_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    postgres = postgres_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_source(postgres)
    ensure_mssql_database_and_schemas()
    mssql = mssql_connector()
    wait_until_ready("mssql", lambda: mssql.get_records("SELECT 1"))
    supported = supported_cases(tmp_path)
    empties = empty_cases(tmp_path)
    pre_staging = pre_staging_rejections(tmp_path)
    staged = staged_data_rejections(tmp_path)
    production_rejections = production_route_rejections(tmp_path)
    expected_ids = {
        case_id
        for case_id, *_rest in (
            *supported,
            *empties,
            *pre_staging,
            *staged,
            *production_rejections,
        )
    }
    assert len(expected_ids) == (
        len(supported) + len(empties) + len(pre_staging) + len(staged) + len(production_rejections)
    )

    target_database = os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it")
    with governed_mssql_campaign(mssql, target_database=target_database) as campaign:

        def runner(load_config: Any) -> GovernedStandardEtlRunner:
            load_config.target_database = target_database
            route = campaign.route(
                target_schema=load_config.target_schema,
                target_table=load_config.target_table,
            )
            return GovernedStandardEtlRunner(
                route,
                postgres,
                logger=QuietIntegrationLogger(),
                source_type=GovernedPostgresSnapshotSource,
            )

        def production_runner(load_config: Any) -> GovernedStandardEtlRunner:
            load_config.target_database = target_database
            route = campaign.route(
                target_schema=load_config.target_schema,
                target_table=load_config.target_table,
            )
            return GovernedStandardEtlRunner(
                route,
                postgres,
                logger=QuietIntegrationLogger(),
            )

        records = [
            *(
                _run_supported(
                    postgres,
                    mssql,
                    runner(load_config),
                    case_id,
                    load_config,
                    scenario,
                )
                for case_id, load_config, scenario in supported
            ),
            *(
                _run_empty(
                    postgres,
                    mssql,
                    runner(load_config),
                    case_id,
                    load_config,
                    scenario,
                )
                for case_id, load_config, scenario in empties
            ),
            *(
                _run_pre_staging_reject(
                    mssql,
                    runner(load_config),
                    case_id,
                    load_config,
                    blocker,
                )
                for case_id, load_config, blocker in pre_staging
            ),
            *(
                _run_staged_reject(
                    postgres,
                    mssql,
                    runner(load_config),
                    case_id,
                    load_config,
                    reason,
                    rows,
                )
                for case_id, load_config, reason, rows in staged
            ),
            *(
                _run_production_route_reject(
                    postgres,
                    mssql,
                    production_runner(load_config),
                    case_id,
                    load_config,
                )
                for case_id, load_config in production_rejections
            ),
        ]
    assert {record["case_id"] for record in records} == expected_ids
    assert all(record["staging_objects_after"] == 0 for record in records)
    assert all(record["before_image_preserved"] for record in records if record["before_image_required"])
    for record in records:
        case_id = str(record["case_id"])
        route_live_recorder.observe_case_digests(
            "strategy_capability",
            case_id,
            before_image=ObservedImageDigest(str(record["before_sha256"])),
            after_image=ObservedImageDigest(str(record["after_sha256"])),
            observations={
                "category": record["category"],
                "actions": record["actions"],
                "staging_objects_after": record["staging_objects_after"],
                "before_image_required": record["before_image_required"],
                "raw_before_sha256": record["raw_before_sha256"],
                "raw_after_sha256": record["raw_after_sha256"],
            },
        )

    postgres_version = str(postgres.get_records("SHOW server_version")[0][0])
    sql_version = mssql.get_records(
        "SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)), "
        "CAST(SERVERPROPERTY('Edition') AS nvarchar(128))"
    )[0]
    evidence = {
        "schema_version": "dpone.postgres_mssql.strategy_capability_matrix.v1",
        "status": "local_certification_passed",
        "release_ready": False,
        "source": {"vendor": "PostgreSQL", "version": postgres_version},
        "sink": {"vendor": "Microsoft SQL Server", "version": str(sql_version[0]), "edition": str(sql_version[1])},
        "transport_equivalence": "128-column full-orchestration route suite",
        "numeric_equivalence_partitions": {
            "authored_parallel_workers_single_invocation_only": [1, 2],
            "max_partitions_per_run": ["zero_reject", "equality_pass", "exceeded_reject"],
        },
        "backfill_orchestration_certified_here": False,
        "expected_case_ids": sorted(expected_ids),
        "case_count": len(records),
        "cases": records,
        "remaining_release_gates": ["published_release_pin", "production_compression_benchmark", "production_soak"],
    }
    _EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    _EVIDENCE.write_text(json.dumps(evidence, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
