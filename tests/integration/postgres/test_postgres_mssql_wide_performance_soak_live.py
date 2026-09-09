"""Seven measured 10k×128 governed runs for every MSSQL strategy."""

from __future__ import annotations

import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder
from tools.route_live_certification.reviewed_cases_strategy import wide_performance_soak_suite

from tests.integration.postgres.postgres_live_support import (
    postgres_connector,
    postgres_mssql_enabled,
    wait_until_ready,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedMssqlCampaign,
    GovernedPostgresSnapshotSource,
    GovernedStandardEtlRunner,
)
from tests.integration.postgres.postgres_mssql_wide_soak_live_support import (
    SOAK_ROW_COUNT,
    assert_soak_correctness,
    create_soak_source,
    mutate_soak_source,
    soak_load_config,
    staging_count,
    staging_objects,
    target_image,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import QuietIntegrationLogger

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
    pytest.mark.route_live_wide,
]


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="PostgreSQL/MSSQL Docker IT not configured")
def test_postgres_mssql_wide_performance_soak_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Execute one warmup plus seven separately observed runs per strategy."""

    postgres = postgres_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    columns = create_soak_source(postgres)
    grouped: dict[str, list[Any]] = defaultdict(list)
    for reviewed in wide_performance_soak_suite().cases:
        parameters = json.loads(reviewed.config_json)["parameters"]
        grouped[str(parameters["strategy"])].append(reviewed)
    assert set(grouped) == {
        "full_refresh",
        "incremental_append",
        "incremental_merge",
        "replace",
        "partition_replace",
        "snapshot_diff",
        "scd2",
        "backfill",
    }
    try:
        for strategy_name in sorted(grouped):
            work_dir = tmp_path / strategy_name
            config = soak_load_config(
                strategy_name,
                target_database=governed_mssql_live_campaign.target_database,
                work_dir=work_dir,
            )
            target = governed_mssql_live_campaign.target
            target.execute_query(f"DROP TABLE IF EXISTS [{config.target_schema}].[{config.target_table}]")
            route = governed_mssql_live_campaign.route(
                target_schema=config.target_schema,
                target_table=config.target_table,
            )
            runner = GovernedStandardEtlRunner(
                route,
                postgres,
                logger=QuietIntegrationLogger(),
                source_type=GovernedPostgresSnapshotSource,
            )
            warmup_name = mutate_soak_source(postgres, 0)
            warmup = runner.run(config, label=f"wide_soak_{strategy_name}_warmup")
            _require_success(warmup)
            assert_soak_correctness(
                postgres,
                target,
                config,
                strategy_name,
                columns,
                run_number=0,
                expected_name=warmup_name,
            )
            assert staging_count(target, config) == 0, staging_objects(target, config)
            assert not tuple(work_dir.iterdir())
            previous_name = warmup_name
            durations: list[float] = []
            reviewed_cases = sorted(
                grouped[strategy_name],
                key=lambda case: int(json.loads(case.config_json)["parameters"]["run_number"]),
            )
            assert len(reviewed_cases) == 7
            for reviewed in reviewed_cases:
                parameters = json.loads(reviewed.config_json)["parameters"]
                run_number = int(parameters["run_number"])
                before = target_image(target, config, strategy_name, previous_name, columns)
                expected_name = mutate_soak_source(postgres, run_number)
                started = time.perf_counter()
                result = runner.run(config, label=f"wide_soak_{strategy_name}_run_{run_number:02d}")
                duration_seconds = time.perf_counter() - started
                durations.append(duration_seconds)
                _require_success(result)
                after = assert_soak_correctness(
                    postgres,
                    target,
                    config,
                    strategy_name,
                    columns,
                    run_number=run_number,
                    expected_name=expected_name,
                )
                assert before != after
                assert staging_count(target, config) == 0, staging_objects(target, config)
                assert not tuple(work_dir.iterdir())
                route_live_recorder.observe_case(
                    "wide_performance_soak",
                    reviewed.case_id,
                    before_image=before,
                    after_image=after,
                    observations={
                        "strategy": strategy_name,
                        "run_number": run_number,
                        "duration_seconds": duration_seconds,
                        "source_rows": SOAK_ROW_COUNT,
                        "business_columns": 128,
                        "source_boundary": "postgres_complete_relation_snapshot_test_adapter",
                        "production_column_cursor_claim": False,
                        "execution_surface": parameters["execution_surface"],
                        "staging_objects_after": 0,
                        "artifact_entries_after": [],
                        "correctness_oracle": parameters["correctness_oracle"],
                    },
                )
                previous_name = expected_name
            assert len(durations) == 7
            assert all(duration >= 0 for duration in durations)
    finally:
        postgres.close()


def _require_success(result: Any) -> None:
    if not isinstance(result, dict):
        result = dict(result)
    assert result["status"] == "success"
    assert int(result["extracted_rows"]) == SOAK_ROW_COUNT
