"""Real SQL Server target-behaviour policy matrix for governed ETL.

Every supported object is provisioned in a disposable SQL Server database
and classified by the production catalog reader before PostgreSQL COPY.  The
pinned Linux vendor has no FILESTREAM/FileTable capability; those reviewed
cells prove that platform boundary directly instead of manufacturing catalog
rows for an object the release image cannot create.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder
from tools.route_live_certification.reviewed_cases_runtime import target_behavior_suite

from tests.integration.postgres.postgres_live_support import (
    mssql_connector,
    postgres_connector,
    postgres_mssql_enabled,
    wait_until_ready,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedMssqlCampaign,
    GovernedStandardEtlRunner,
    governed_mssql_campaign,
)
from tests.integration.postgres.postgres_mssql_target_behavior_live_support import (
    STAGING_SCHEMA,
    TARGET_SCHEMA,
    ObservedSnapshotSource,
    catalog,
    ensure_source,
    exception_text,
    load_config,
    observe_filetable_platform_boundary,
    provision,
    require_catalog_behavior,
    safe_name,
    seed_safe_before,
    staging_count,
    target_rows,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import QuietIntegrationLogger

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
    pytest.mark.route_live_wide,
]


@pytest.fixture(scope="module")
def target_behavior_campaign() -> Iterator[GovernedMssqlCampaign]:
    database = f"dpone_behavior_{uuid.uuid4().hex[:16]}"
    memory_file = f"/var/opt/mssql/data/{database}_mem"
    master = mssql_connector(database="master")
    target = None
    master.execute_query(f"CREATE DATABASE [{database}]")
    try:
        master.execute_query(f"ALTER DATABASE [{database}] ADD FILEGROUP [dpone_mem] CONTAINS MEMORY_OPTIMIZED_DATA")
        master.execute_query(
            f"ALTER DATABASE [{database}] ADD FILE "
            f"(NAME = N'dpone_mem_file', FILENAME = N'{memory_file}') TO FILEGROUP [dpone_mem]"
        )
        target = mssql_connector(database=database)
        target.execute_query(f"CREATE SCHEMA [{TARGET_SCHEMA}] AUTHORIZATION [dbo]")
        target.execute_query(f"CREATE SCHEMA [{STAGING_SCHEMA}] AUTHORIZATION [dbo]")
        with governed_mssql_campaign(target, target_database=database) as campaign:
            yield campaign
    finally:
        if target is not None:
            with suppress(Exception):
                target.close()
        with suppress(Exception):
            master.execute_query(f"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        with suppress(Exception):
            master.execute_query(f"DROP DATABASE [{database}]")
        with suppress(Exception):
            master.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="PostgreSQL/MSSQL Docker IT not configured")
def test_postgres_mssql_target_behavior_matrix_live(
    tmp_path: Path,
    target_behavior_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    postgres = postgres_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_source(postgres)
    try:
        for reviewed in target_behavior_suite().cases:
            parameters = json.loads(reviewed.config_json)["parameters"]
            strategy_group = str(parameters["strategy_group"])
            behavior = str(parameters["target_behavior"])
            if behavior == "filetable":
                observe_filetable_platform_boundary(
                    target_behavior_campaign.target,
                    reviewed.case_id,
                    strategy_group,
                    route_live_recorder,
                )
                continue
            config = load_config(
                reviewed.case_id,
                strategy_group=strategy_group,
                target_database=target_behavior_campaign.target_database,
                tmp_path=tmp_path,
            )
            route = target_behavior_campaign.route(
                target_schema=config.target_schema,
                target_table=config.target_table,
            )
            provision(route.target, config, behavior)
            if reviewed.expected_mutation:
                seed_safe_before(route.target, config, strategy_group)
            before = target_rows(route.target, config)
            catalog_values = catalog(route.target, config)
            require_catalog_behavior(catalog_values, behavior)
            runner = GovernedStandardEtlRunner(
                route,
                postgres,
                logger=QuietIntegrationLogger(),
                source_type=ObservedSnapshotSource,
            )
            source = runner.source
            assert isinstance(source, ObservedSnapshotSource)
            error: Exception | None = None
            result: dict[str, Any] | None = None
            try:
                result = dict(runner.run(config, label=safe_name(reviewed.case_id)))
            except Exception as exc:  # noqa: BLE001 - stable domain token asserted below.
                error = exc
            after = target_rows(route.target, config)
            if reviewed.expected_mutation:
                assert error is None, exception_text(error)
                assert result is not None
                assert source.extract_calls == 1
                assert after != before
            else:
                assert error is not None
                assert "mssql_target_contract." in exception_text(error)
                assert source.extract_calls == 0
                assert after == before
            assert staging_count(route.target, config) == 0
            route_live_recorder.observe_case(
                "target_behavior",
                reviewed.case_id,
                before_image=before,
                after_image=after,
                observations={
                    "source_boundary": "complete_relation_snapshot_test_adapter",
                    "production_column_cursor_claim": False,
                    "source_extracts": source.extract_calls,
                    "staging_objects_after": 0,
                    "catalog": catalog_values,
                    "typed_error": type(error).__name__ if error is not None else None,
                },
            )
    finally:
        postgres.close()
