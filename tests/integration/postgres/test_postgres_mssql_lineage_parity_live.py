"""Reviewed 24-case sink-side lineage parity on PostgreSQL 16 / SQL Server 2022."""

from __future__ import annotations

import hashlib
import json
from contextlib import suppress
from itertools import product
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder
from tools.route_live_certification.reviewed_cases_strategy import lineage_parity_suite

from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from tests.integration.postgres.postgres_live_support import (
    postgres_connector,
    postgres_mssql_enabled,
    wait_until_ready,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedMssqlCampaign,
)
from tests.integration.postgres.postgres_mssql_lineage_parity_live_support import (
    SCOPE_REPLAY_TARGET_TABLE,
    SOURCE_BOUNDARY,
    SOURCE_ROWS,
    STRATEGIES,
    TARGET_SCHEMA,
    TRANSPORTS,
    assert_exact_catalog,
    assert_replace_scope_catalog_binding,
    drop_source,
    ensure_source,
    expected_row_hashes,
    expected_row_ids,
    lineage_cases,
    prove_existing_target_scope_replay,
    prove_temporal_convert_vendor_surface,
    receipt_for_target,
    require_digest,
    run_lineage_case,
    semantic_parity_image,
    target_catalog,
    target_clock,
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

_CASE_PARAMETERS = tuple(product(STRATEGIES, TRANSPORTS))
_CASE_IDS = tuple(f"{strategy}__{transport}" for strategy, transport in _CASE_PARAMETERS)


@pytest.fixture(scope="module")
def lineage_semantic_parity_images() -> dict[str, dict[str, Any]]:
    """Retain one artifact-neutral semantic image for each strategy."""

    return {}


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
@pytest.mark.parametrize(("strategy", "transport"), _CASE_PARAMETERS, ids=_CASE_IDS)
def test_postgres_mssql_lineage_parity_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
    lineage_semantic_parity_images: dict[str, dict[str, Any]],
    strategy: str,
    transport: str,
) -> None:
    """Observe one reviewed item and prove artifact-neutral native lineage."""

    case_id = f"{strategy}__{transport}"
    expected_case_ids = set(_CASE_IDS)
    reviewed = lineage_parity_suite()
    assert len(_CASE_PARAMETERS) == len(reviewed.cases) == 24
    assert {case.case_id for case in reviewed.cases} == expected_case_ids
    reviewed_case = next(case for case in reviewed.cases if case.case_id == case_id)
    parameters = json.loads(reviewed_case.config_json)["parameters"]
    assert parameters["source_boundary"] == SOURCE_BOUNDARY
    assert parameters["production_column_cursor_claim"] is False

    postgres = postgres_connector()
    target = governed_mssql_live_campaign.target
    target_database = governed_mssql_live_campaign.target_database
    config: Any | None = None
    try:
        wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
        ensure_source(postgres)
        cases = lineage_cases(tmp_path)
        assert len(cases) == 24
        assert {case.case_id for case in cases} == expected_case_ids
        case = next(candidate for candidate in cases if candidate.case_id == case_id)
        config = case.load_config
        config.target_database = target_database
        config.staging_database = target_database

        if case_id == _CASE_IDS[0]:
            prove_temporal_convert_vendor_surface(postgres, target)
            replay_route = governed_mssql_live_campaign.route(
                target_schema=TARGET_SCHEMA,
                target_table=SCOPE_REPLAY_TARGET_TABLE,
            )
            prove_existing_target_scope_replay(
                replay_route,
                postgres,
                target_database=target_database,
                work_dir=tmp_path / "portable_scope_replay",
                logger=QuietIntegrationLogger(),
            )

        collation_rows = target.get_records("SELECT CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(), 'Collation'))")
        assert len(collation_rows) == 1 and str(collation_rows[0][0])
        database_collation = str(collation_rows[0][0])
        target.execute_query(f"DROP TABLE IF EXISTS [{TARGET_SCHEMA}].[{config.target_table}]")
        before_catalog = target_catalog(target, config.target_table)
        assert before_catalog["exists"] is False
        target_before_clock = target_clock(target)
        route = governed_mssql_live_campaign.route(
            target_schema=config.target_schema,
            target_table=config.target_table,
        )
        run = run_lineage_case(
            route,
            postgres,
            case,
            logger=QuietIntegrationLogger(),
        )
        target_after_clock = target_clock(target)

        catalog = target_catalog(target, config.target_table)
        rows = target_rows(target, config.target_table)
        receipt = receipt_for_target(route, config.target_table)
        assert_exact_catalog(case, catalog, database_collation)
        assert_replace_scope_catalog_binding(postgres, case, run, catalog)
        expected_business = [
            {"id": row[0], "partition_id": row[1], "value": row[2]}
            for row in SOURCE_ROWS
            if case.strategy != "replace" or row[1] == 1
        ]
        assert [
            {"id": int(row["id"]), "partition_id": int(row["partition_id"]), "value": row["value"]} for row in rows
        ] == expected_business

        result = dict(run.result)
        assert result["status"] == "success"
        assert str(result["commit_outcome"]) == AtomicCommitOutcome.COMMITTED.value
        assert result["commit_receipt_id"] == receipt["receipt_id"]
        assert result["load_id"] == receipt["load_id"]
        assert len(str(result["run_id"])) == 26
        assert len(str(result["load_id"])) == 26

        source_started = run.source_receipt.extraction_started_at.replace(tzinfo=None)
        source_completed = run.source_receipt.extraction_completed_at
        assert source_completed is not None
        source_completed_naive = source_completed.replace(tzinfo=None)
        assert receipt["extraction_started_at_utc"] == source_started
        assert receipt["extraction_completed_at_utc"] == source_completed_naive
        assert receipt["snapshot_acquired_at_utc"] == source_started
        assert receipt["extraction_clock_authority"] == "dpone.orchestrator.utc"
        assert receipt["snapshot_authority"] == "postgresql.repeatable_read"
        assert run.source_receipt.clock_authority == receipt["extraction_clock_authority"]
        assert run.source_receipt.snapshot_authority == receipt["snapshot_authority"]
        assert run.source_receipt.source_token is not None
        assert (
            bytes(receipt["source_token_sha256"])
            == hashlib.sha256(run.source_receipt.source_token.encode("utf-8")).digest()
        )

        expected_count = len(expected_business)
        assert int(receipt["declared_rows"]) == expected_count
        assert int(receipt["actual_raw_rows"]) == expected_count
        assert int(receipt["actual_native_rows"]) == expected_count
        assert int(receipt["staging_rows"]) == expected_count
        assert int(receipt["total_rows"]) == expected_count
        assert target_before_clock <= receipt["loaded_at_utc"] <= target_after_clock
        assert receipt["loaded_at_utc"] <= receipt["committed_at_utc"] <= target_after_clock
        assert receipt["target_database"] == target_database
        assert receipt["target_schema"] == TARGET_SCHEMA
        assert receipt["target_table"] == config.target_table
        assert receipt["strategy"] == case.strategy
        assert int(receipt["generation"]) == int(receipt["current_generation"]) == 1
        assert bytes(receipt["attempt_key"]) == bytes(receipt["current_attempt_key"])
        assert bytes(receipt["route_fingerprint"]) == bytes(receipt["current_route_fingerprint"])
        for label in (
            "operation_key",
            "attempt_key",
            "scope_hash",
            "owner_digest",
            "payload_manifest_sha256",
            "native_contract_sha256",
            "mutation_plan_sha256",
            "target_before_sha256",
            "target_after_sha256",
            "target_identity",
            "route_fingerprint",
        ):
            require_digest(receipt[label], label)

        expected_ids = expected_row_ids(case, rows)
        for row in rows:
            row_id = int(row["id"])
            assert row["__dpone__run_id"] == result["run_id"]
            assert row["__dpone__load_id"] == result["load_id"]
            assert row["__dpone__extracted_at"] == receipt["extraction_started_at_utc"]
            assert row["__dpone__loaded_at"] == receipt["loaded_at_utc"]
            assert row["__dpone__row_id"] == expected_ids[row_id]
        if case.strategy in {"snapshot_diff", "scd2"}:
            expected_hashes = expected_row_hashes(rows)
            for row in rows:
                assert row["__dpone__row_hash"] == expected_hashes[int(row["id"])]

        terminal = run.artifact.terminal_receipt
        assert terminal is not None
        assert terminal.outcome is ArtifactTerminalOutcome.SUCCESS
        assert terminal.cleanup_attempted is True
        assert terminal.cleanup_succeeded is True
        if case.transport == "postgres_copy_mssql_bcp":
            assert not Path(run.artifact.file_path).exists()
        staging_rows = target.get_records(
            "SELECT COUNT_BIG(*) FROM sys.tables AS t "
            "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
            "WHERE s.name = ? AND t.name LIKE ?",
            (config.staging_schema, f"stg_{config.target_table}_%"),
        )
        assert int(staging_rows[0][0]) == 0

        parity_image = semantic_parity_image(case, catalog, rows, receipt)
        first = lineage_semantic_parity_images.setdefault(case.strategy, parity_image)
        assert parity_image == first
        phases = {
            "extract": {
                "source_boundary": SOURCE_BOUNDARY,
                "snapshot_authority": receipt["snapshot_authority"],
                "started_at": receipt["extraction_started_at_utc"],
                "completed_at": receipt["extraction_completed_at_utc"],
            },
            "stage": {
                "declared_rows": receipt["declared_rows"],
                "actual_raw_rows": receipt["actual_raw_rows"],
                "actual_native_rows": receipt["actual_native_rows"],
                "staging_objects_after": 0,
            },
            "mutate": {
                "target_rows": len(rows),
                "target_catalog_columns": len(catalog["columns"]),
            },
            "commit": {
                "receipt_id": receipt["receipt_id"],
                "outcome": str(result["commit_outcome"]),
                "target_clock_loaded_at": receipt["loaded_at_utc"],
            },
            "checkpoint": {
                "mode": "stateless_complete_relation_snapshot",
                "source_checkpoint_writes": 0,
                "terminal_receipt": True,
            },
        }
        identities = {
            "run": {"lineage_run_id": result["run_id"], "scheduler_run_id": run.invocation.run_id},
            "operation": {
                "operation_key": bytes(receipt["operation_key"]).hex(),
                "epoch": receipt["operation_epoch"],
            },
            "source": {
                "physical": run.source_identity,
                "snapshot_token_sha256": bytes(receipt["source_token_sha256"]).hex(),
            },
            "target": {
                "physical_identity": bytes(receipt["target_identity"]).hex(),
                "database": target_database,
                "schema": TARGET_SCHEMA,
                "table": config.target_table,
            },
            "artifact": {
                "transport": case.transport,
                "class": type(run.artifact).__name__,
                "manifest_sha256": bytes(receipt["payload_manifest_sha256"]).hex(),
                "native_contract_sha256": bytes(receipt["native_contract_sha256"]).hex(),
            },
        }
        route_live_recorder.observe_case(
            "lineage_parity",
            case.case_id,
            before_image={"catalog": before_catalog, "rows": []},
            after_image={"catalog": catalog, "rows": rows, "receipt_id": receipt["receipt_id"]},
            observations={
                "strategy": case.strategy,
                "transport": case.transport,
                "source_boundary": SOURCE_BOUNDARY,
                "production_column_cursor_claim": False,
                "phases": phases,
                "identities": identities,
                "semantic_parity_sha256": hashlib.sha256(
                    json.dumps(parity_image, default=str, sort_keys=True).encode("utf-8")
                ).hexdigest(),
                "cleanup": terminal.to_dict(),
            },
        )
        observed, reviewed_count = route_live_recorder.coverage()["lineage_parity"]
        assert reviewed_count == 24
        if observed == reviewed_count:
            assert set(lineage_semantic_parity_images) == set(STRATEGIES)
    finally:
        if config is not None:
            with suppress(Exception):
                target.execute_query(f"DROP TABLE IF EXISTS [{TARGET_SCHEMA}].[{config.target_table}]")
        with suppress(Exception):
            drop_source(postgres)
        postgres.close()
