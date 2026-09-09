"""Frozen source and outcome policy for pre-tag release evidence."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


codec = _load_sibling("dpone_release_candidate_codec_policy", "release_candidate_evidence_codec.py")

SCHEMA = "dpone.release_candidate_evidence_policy.v1"
PROFILE = "native_transfer"
EXECUTION_RUNNER_ID = "refresh-live-certification-replay"
VERIFICATION_RUNNER_ID = "refresh-live-certification-verify"
ROW_COUNT = 25_000
# PostgreSQL source exports remain single-partition until the runtime provides
# one shared MVCC snapshot across workers.  The authority must certify that
# safe producer shape exactly; it must not retain the obsolete parallel shape.
STRESS_PARTITION_COUNT = 1
STRESS_PARTITIONING = {
    "enabled": False,
    "partition_column": None,
    "lower_bound": 1,
    "upper_bound": ROW_COUNT,
    "num_partitions": STRESS_PARTITION_COUNT,
    "export_workers": None,
    "load_workers": None,
}
STRESS_ARTIFACT_TYPE = "FileExportArtifact"
WORKFLOW_NAME = "Release candidate evidence"
WORKFLOW_RUN_NAME_TEMPLATE = "{workflow_name} {release} at {commit_sha}"
WORKFLOW_PATH = ".github/workflows/release-candidate-evidence.yml"
JOB_NAME = "Release candidate evidence"
APP_ID = 15_368
APP_SLUG = "github-actions"
ARTIFACT_PREFIX = "release-candidate-evidence"
SOURCE_PATHS = {
    "exact_commit_checks": "preflight/exact_commit_checks.json",
    "merge_receipt": "preflight/exact_commit_merge_receipt.json",
    "service_markers": "live/service_markers.json",
    "mysql_route_cells": "live/mysql/mysql_local_route_cells.json",
    "native_transfer_fixtures": "live/native_transfer_live_fixtures.json",
    "mssql_clickhouse_junit": "live/refresh-executor/mssql-clickhouse/junit_evidence.json",
    "mssql_clickhouse_execution": "live/refresh-executor/mssql-clickhouse/route_refresh_execution.json",
    "mssql_clickhouse_verification": "live/refresh-executor/mssql-clickhouse/route_refresh_verification.json",
    "postgres_mssql_junit": "live/refresh-executor/postgres-mssql/junit_evidence.json",
    "postgres_mssql_execution": "live/refresh-executor/postgres-mssql/route_refresh_execution.json",
    "postgres_mssql_verification": "live/refresh-executor/postgres-mssql/route_refresh_verification.json",
    "cdc_state_junit": "live/cdc-state/junit_evidence.json",
    "stress_benchmark": "live/benchmarks/postgres_mssql_native_fast_path.json",
}
JUNIT_CASES = {
    "service_markers": (
        "tests.integration.mysql.test_mysql_source_integration::test_mysql_connector_selects_and_exports_mssql_delimited",
        "tests.integration.postgres.test_postgres_connector_integration::test_postgres_connector_executes_queries_and_streams_rows",
        "tests.integration.postgres.test_postgres_connector_integration::test_postgres_connector_copy_from_iter_loads_csv_rows",
        "tests.integration.mssql.test_mssql_optional_integration::test_mssql_bcp_import_roundtrip",
        "tests.integration.mssql.test_mssql_optional_integration::test_mssql_verified_empty_payload_has_receipt_without_bcp",
        "tests.integration.mssql.test_mssql_optional_integration::test_mssql_real_bcp_reject_cleans_staging_and_preserves_target",
        "tests.integration.kafka.test_kafka_optional_integration::test_kafka_json_sink_and_source_round_trip",
        "tests.integration.kafka.test_kafka_optional_integration::test_schema_registry_client_can_be_created_when_url_is_configured",
    ),
    # MSSQL is deliberately excluded from this lightweight MySQL group. Its
    # target-MAX incremental mode is unsafe, while the old direct-sink tests
    # bypass mandatory MSSQL transaction admission.
    "mysql_route_cells": (
        "tests.integration.mysql.test_mysql_to_postgres_native_transfer_integration::test_mysql_to_postgres_full_refresh_csv_copy",
        "tests.integration.mysql.test_mysql_to_postgres_native_transfer_integration::test_mysql_to_postgres_incremental_merge_watermark",
        "tests.integration.mysql.test_mysql_to_clickhouse_native_transfer_integration::test_mysql_to_clickhouse_full_refresh_tsv",
        "tests.integration.mysql.test_mysql_to_clickhouse_native_transfer_integration::test_mysql_to_clickhouse_incremental_merge_watermark",
        "tests.integration.mysql.test_mysql_to_kafka_native_transfer_integration::test_mysql_to_kafka_full_refresh_csv_produce",
    ),
    # PostgreSQL -> MSSQL has a separate exhaustive, provider-bound authority
    # artifact; stale direct-sink fixtures cannot substitute for it.
    "native_transfer_fixtures": (
        "tests.integration.mssql.test_mssql_to_clickhouse_native_transfer_integration::test_mssql_bcp_queryout_loads_clickhouse_via_native_http",
        "tests.integration.mssql.test_mssql_to_clickhouse_native_transfer_integration::test_mssql_datetimeoffset_modes_load_clickhouse_via_native_http",
        "tests.integration.mssql.test_mssql_to_clickhouse_native_transfer_integration::test_mssql_naive_datetime_modes_order_by_and_schema_evolution_live",
    ),
    "mssql_clickhouse_junit": (
        "tests.integration.mssql.test_mssql_clickhouse_refresh_executor_live_integration::test_mssql_clickhouse_refresh_executor_replays_wide_chunks_and_verifies_exactly",
    ),
    "postgres_mssql_junit": (
        "tests.integration.mssql.test_postgres_mssql_refresh_executor_live_integration::test_postgres_mssql_refresh_executor_replays_wide_chunks_and_verifies_exactly",
    ),
    "cdc_state_junit": (
        "tests.integration.mssql.test_mssql_clickhouse_live_cdc_runtime_integration::test_mssql_change_tracking_runtime_applies_to_clickhouse_and_commits_offsets",
        "tests.integration.mssql.test_mssql_clickhouse_live_cdc_runtime_integration::test_mssql_change_tracking_runtime_materializes_clickhouse_current_state",
        "tests.integration.mssql.test_mssql_clickhouse_live_cdc_runtime_integration::test_mssql_change_tracking_retention_gap_resyncs_clickhouse_without_committing_offsets",
    ),
}
ROUTES = {
    "mssql_clickhouse": "mssql_to_clickhouse__incremental_merge",
    "postgres_mssql": "postgres_to_mssql__incremental_merge",
}
ROUTE_PROFILES = {
    "mssql_clickhouse": {
        "key": {
            "source": "mssql",
            "sink": "clickhouse",
            "strategy": "incremental_merge",
            "pair_id": "mssql_to_clickhouse",
            "case_id": "mssql_to_clickhouse__incremental_merge",
            "colon_id": "mssql:clickhouse:incremental_merge",
        },
        "docs_link": "docs/source-sink/mssql-to-clickhouse.md",
        "install_extras": ["mssql", "clickhouse"],
        "required_profiles": ["mssql_local", "clickhouse_local"],
        "live_profiles": ["mssql_live", "clickhouse_live"],
        "local_service_supported": True,
        "external_credentials_required": False,
        "certification_status": "contract_gate",
        "native_fast_path": "mssql_bcp_queryout_to_clickhouse_typed_wire",
        "required_evidence": [
            "matrix_case",
            "manifest_example",
            "strategy_plan",
            "quality_reconciliation",
            "run_artifact",
            "route_execution_ledger",
            "state_promotion",
            "docs_runbook",
            "wide_120_column_gate",
            "route_schema_evolution",
            "route_reconciliation_repair",
            "type_fidelity",
            "typed_hash",
            "wide_type_certification",
            "benchmark_slo",
            "schema_evolution",
        ],
        "default_slo_hints": {"phase": "source_export", "rows_per_second_min": 30_000},
    },
    "postgres_mssql": {
        "key": {
            "source": "postgres",
            "sink": "mssql",
            "strategy": "incremental_merge",
            "pair_id": "postgres_to_mssql",
            "case_id": "postgres_to_mssql__incremental_merge",
            "colon_id": "postgres:mssql:incremental_merge",
        },
        "docs_link": "docs/source-sink/postgres-to-mssql.md",
        "install_extras": ["postgres", "mssql"],
        "required_profiles": ["postgres_local", "mssql_local"],
        "live_profiles": ["postgres_live", "mssql_live"],
        "local_service_supported": True,
        "external_credentials_required": False,
        "certification_status": "contract_gate",
        "native_fast_path": "postgres_copy_to_mssql_bcp",
        "required_evidence": [
            "matrix_case",
            "manifest_example",
            "strategy_plan",
            "quality_reconciliation",
            "run_artifact",
            "route_execution_ledger",
            "state_promotion",
            "docs_runbook",
            "wide_120_column_gate",
            "route_schema_evolution",
            "route_reconciliation_repair",
            "lossless_transport_contract",
            "benchmark_slo",
            "resume_checkpoint",
            "type_matrix",
        ],
        "default_slo_hints": {"phase": "target_load_finalize", "rows_per_second_min": 80_000},
    },
}
STRESS_METRICS = (
    "prepare_postgres_source",
    "postgres_to_mssql_full_refresh",
    "mssql_to_clickhouse_full_refresh",
)
MINIMUM_ROWS_PER_SECOND = 500.0
CHECKLIST_ROLES = {
    "cli_help_surface": "exact_commit_checks",
    "cli_output_contracts": "exact_commit_checks",
    "run_cli_manifest": "exact_commit_checks",
    "run_python_api_manifest": "exact_commit_checks",
    "nested_hierarchical_identity": "exact_commit_checks",
    "nested_parent_child_integrity": "exact_commit_checks",
    "source_sink_strategy_matrix": "native_transfer_fixtures",
    "source_sink_artifacts": "mssql_clickhouse_verification",
    "docker_live_routes": "cdc_state_junit",
    "contracts_guardrails": "exact_commit_checks",
    "documentation_yaml_examples": "exact_commit_checks",
    "documentation_links": "exact_commit_checks",
    "documentation_mkdocs": "exact_commit_checks",
    "ci_cd_quality": "exact_commit_checks",
    "package": "exact_commit_checks",
}
POLICY_PROJECTION = {
    "schema": SCHEMA,
    "profile": PROFILE,
    "row_count": ROW_COUNT,
    "stress_partition_count": STRESS_PARTITION_COUNT,
    "stress_partitioning": STRESS_PARTITIONING,
    "stress_artifact_type": STRESS_ARTIFACT_TYPE,
    "workflow_name": WORKFLOW_NAME,
    "workflow_run_name_template": WORKFLOW_RUN_NAME_TEMPLATE,
    "workflow_path": WORKFLOW_PATH,
    "job_name": JOB_NAME,
    "source_paths": dict(sorted(SOURCE_PATHS.items())),
    "junit_cases": {key: list(value) for key, value in sorted(JUNIT_CASES.items())},
    "routes": dict(sorted(ROUTES.items())),
    "route_profiles": {key: value for key, value in sorted(ROUTE_PROFILES.items())},
    "execution_runner_id": EXECUTION_RUNNER_ID,
    "verification_runner_id": VERIFICATION_RUNNER_ID,
    "stress_metrics": list(STRESS_METRICS),
    "minimum_rows_per_second": MINIMUM_ROWS_PER_SECOND,
    "failure_rate_maximum": 0.0,
    "checklist_roles": dict(sorted(CHECKLIST_ROLES.items())),
}
POLICY_SHA256 = codec.sha256_bytes(codec.canonical_json_bytes(POLICY_PROJECTION))


def artifact_name(commit_sha: str, run_id: int, run_attempt: int) -> str:
    return f"{ARTIFACT_PREFIX}-{commit_sha}-{run_id}-{run_attempt}"


def workflow_run_name(*, release: str, commit_sha: str) -> str:
    """Return the exact provider run name rendered by the workflow."""

    return WORKFLOW_RUN_NAME_TEMPLATE.format(
        workflow_name=WORKFLOW_NAME,
        release=release,
        commit_sha=commit_sha,
    )


__all__ = [
    "APP_ID",
    "APP_SLUG",
    "ARTIFACT_PREFIX",
    "CHECKLIST_ROLES",
    "EXECUTION_RUNNER_ID",
    "JOB_NAME",
    "JUNIT_CASES",
    "MINIMUM_ROWS_PER_SECOND",
    "POLICY_PROJECTION",
    "POLICY_SHA256",
    "PROFILE",
    "ROUTE_PROFILES",
    "ROUTES",
    "ROW_COUNT",
    "SOURCE_PATHS",
    "STRESS_METRICS",
    "STRESS_ARTIFACT_TYPE",
    "STRESS_PARTITION_COUNT",
    "STRESS_PARTITIONING",
    "WORKFLOW_NAME",
    "WORKFLOW_RUN_NAME_TEMPLATE",
    "WORKFLOW_PATH",
    "VERIFICATION_RUNNER_ID",
    "artifact_name",
    "workflow_run_name",
]
