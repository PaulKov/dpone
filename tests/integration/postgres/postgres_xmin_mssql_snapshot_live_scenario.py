"""Assertions and scenario operations for the XMin → MSSQL live proof.

Keeping these operations outside the pytest module makes the lifecycle easy to
read while retaining detailed, reusable checks for exact operational evidence.
The module never provisions infrastructure and never contains credentials.
"""

from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from psycopg import sql

from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    DAG_ID,
    PROCESS_NAME,
    SOURCE_TABLE,
    SPECIAL_KEY,
    SPECIAL_NOTE,
)

EVIDENCE_PATH = Path("test_artifacts/live_certification/postgres_mssql_xmin_reconciliation.json")


def assert_baseline_fidelity(rows: dict[str, dict[str, Any]]) -> None:
    """Assert all high-risk wire types plus the retired-cutoff regression."""

    assert set(rows) == {SPECIAL_KEY, "empty", "nullable"}
    special = rows[SPECIAL_KEY]
    assert special["note"] == SPECIAL_NOTE
    assert special["__dpone__deleted_at"] is None
    assert math.isclose(
        float(special["metric_value"]),
        1.23456789012345,
        rel_tol=0.0,
        abs_tol=0.0,
    )
    assert str(special["occurred_at"]).startswith("2024-06-15 00:04:05.123456")
    assert special["occurred_at"].year < 2026
    assert rows["empty"]["note"] == ""
    assert rows["nullable"]["note"] is None
    assert float(rows["nullable"]["metric_value"]) == 9007199254740991.0


def assert_run_evidence(
    route: Any,
    result: dict[str, Any],
    execution_date: datetime,
    *,
    expected: dict[str, int],
    previous_checkpoint: int | None,
    expected_outcome: AtomicCommitOutcome = AtomicCommitOutcome.COMMITTED,
) -> int:
    """Assert result, receipt, checkpoint, audit, and run-state as one contract."""

    assert result["status"] == "success"
    assert result["commit_outcome"] == expected_outcome
    for field, value in expected.items():
        assert int(result[field]) == value, field
    metrics = result["reconciliation_metrics"]
    assert metrics is not None
    metric_to_result = {
        "inserted": "inserted_rows",
        "updated": "updated_rows",
        "reactivated": "reactivated_rows",
        "unchanged": "unchanged_rows",
        "soft_deleted": "soft_deleted_rows",
        "active": "active_rows",
        "total": "total_rows",
        "staging": "staging_rows",
    }
    for metric_field, result_field in metric_to_result.items():
        assert int(metrics[metric_field]) == expected[result_field]

    evidence = route.evidence(str(result["load_id"]), execution_date)
    receipt = evidence["receipt"]
    checkpoint = evidence["checkpoint"]
    audit = evidence["audit"]
    run_state = evidence["run_state"]
    candidate = int(receipt["candidate_xmin"])
    assert receipt["receipt_id"] == result["commit_receipt_id"] == result["load_id"]
    assert receipt["load_id"] == result["load_id"]
    assert str(receipt["source_snapshot_token"]).startswith("sha256:")
    if previous_checkpoint is None:
        assert receipt["previous_xmin"] is None
    else:
        assert int(receipt["previous_xmin"]) == previous_checkpoint
        assert candidate >= previous_checkpoint
    assert bytes(receipt["state_key"]) == bytes(checkpoint["state_key"])
    assert int(checkpoint["xmin_value"]) == candidate
    assert bool(checkpoint["is_initial"]) is (previous_checkpoint is None)
    assert checkpoint["last_load_id"] == result["load_id"]
    assert checkpoint["source_snapshot_token"] == receipt["source_snapshot_token"]
    assert checkpoint["environment"] == "integration"
    assert checkpoint["process_name"] == PROCESS_NAME
    assert checkpoint["target_database"] == route.target_database
    assert checkpoint["target_schema"] == "sample_metrics"
    assert checkpoint["target_table"] == "metric_values"

    assert audit["status"] == "committed"
    assert audit["process_name"] == DAG_ID
    assert audit["commit_receipt_id"] == result["commit_receipt_id"]
    assert audit["commit_outcome"] == expected_outcome.value
    assert int(audit["extracted_rows"]) == expected["staging_rows"]
    assert int(audit["loaded_rows"]) == expected["total_rows"]
    assert int(audit["deleted_rows"]) == expected["soft_deleted_rows"]
    audit_to_result = (
        ("staged_rows", "staging_rows"),
        ("inserted_rows", "inserted_rows"),
        ("updated_rows", "updated_rows"),
        ("reactivated_rows", "reactivated_rows"),
        ("unchanged_rows", "unchanged_rows"),
        ("soft_deleted_rows", "soft_deleted_rows"),
        ("active_rows", "active_rows"),
        ("total_rows", "total_rows"),
    )
    for audit_field, result_field in audit_to_result:
        assert int(audit[audit_field]) == int(result[result_field]), audit_field

    assert run_state["state"] == "success"
    assert run_state["dag_id"] == DAG_ID
    assert run_state["process_name"] == PROCESS_NAME
    assert int(run_state["rows_read"]) == expected["staging_rows"]
    assert int(run_state["rows_written"]) == expected["inserted_rows"] + expected["updated_rows"]
    assert int(run_state["rows_updated"]) == expected["updated_rows"]
    assert int(run_state["rows_deleted"]) == expected["soft_deleted_rows"]
    return candidate


def mutate_update_insert_delete(route: Any) -> None:
    """Create one changed, one new, and one physically absent key."""

    table = _source_table(route)
    route.postgres.execute_query(
        sql.SQL("UPDATE {} SET metric_value = %s, note = %s, occurred_at = %s WHERE metric_code = %s").format(table),
        (1.0000000000000002, "updated\tpayload\nmarker\x1d", "2026-04-04T12:00:00+04:00", SPECIAL_KEY),
    )
    route.postgres.execute_query(
        sql.SQL("INSERT INTO {} (metric_code, metric_value, note, occurred_at) VALUES (%s, %s, %s, %s)").format(table),
        ("new-Ω", -9876543210.125, "", "2026-05-05T05:05:05.555555+00:00"),
    )
    route.postgres.execute_query(
        sql.SQL("DELETE FROM {} WHERE metric_code = %s").format(table),
        ("empty",),
    )


def update_special(route: Any, *, metric_value: float, note: str) -> None:
    route.postgres.execute_query(
        sql.SQL("UPDATE {} SET metric_value = %s, note = %s WHERE metric_code = %s").format(_source_table(route)),
        (metric_value, note, SPECIAL_KEY),
    )


def reactivate(route: Any) -> None:
    route.postgres.execute_query(
        sql.SQL("INSERT INTO {} (metric_code, metric_value, note, occurred_at) VALUES (%s, %s, %s, %s)").format(
            _source_table(route)
        ),
        ("empty", 2.5, "reactivated", "2026-06-06T06:06:06.666666+00:00"),
    )


def delete_again(route: Any) -> None:
    route.postgres.execute_query(
        sql.SQL("DELETE FROM {} WHERE metric_code = %s").format(_source_table(route)),
        ("empty",),
    )


def source_key_query(route: Any) -> Any:
    return sql.SQL("SELECT metric_code FROM {}").format(_source_table(route))


def rows_by_key(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row["metric_code"]): row for row in rows}


def state_counts(route: Any) -> dict[str, int]:
    row = route.state.get_records(
        f"SELECT "
        f"(SELECT COUNT_BIG(*) FROM [{route.state_database}].[system].[dpone_commit_receipt]) "
        f"AS receipt_count, "
        f"(SELECT COUNT_BIG(*) FROM [{route.state_database}].[system].[dpone_source_state]) "
        f"AS checkpoint_count",
        as_dict=True,
    )[0]
    return {
        "receipt_count": int(row["receipt_count"]),
        "checkpoint_count": int(row["checkpoint_count"]),
    }


def current_checkpoint(route: Any) -> int:
    row = route.state.get_records(
        f"SELECT xmin_value FROM [{route.state_database}].[system].[dpone_source_state]",
        as_dict=True,
    )[0]
    return int(row["xmin_value"])


def staging_table_count(route: Any) -> int:
    row = route.target.get_records(
        f"SELECT COUNT_BIG(*) AS staging_count "
        f"FROM [{route.target_database}].sys.tables AS t "
        f"INNER JOIN [{route.target_database}].sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = N'staging'",
        as_dict=True,
    )[0]
    return int(row["staging_count"])


def assert_failed_run_evidence(route: Any, execution_date: datetime) -> None:
    """Assert failed audit/run evidence without a checkpoint or receipt advance."""

    run_states = route.state.get_records(
        f"SELECT state, error_message "
        f"FROM [{route.state_database}].[system].[dpone_run_state] "
        "WHERE dag_id = ? AND process_name = ? AND execution_date = ?",
        (DAG_ID, PROCESS_NAME, execution_date),
        as_dict=True,
    )
    assert len(run_states) == 1
    assert run_states[0]["state"] == "failed"
    assert "simulated failure before server commit" in str(run_states[0]["error_message"])

    failed_audits = route.state.get_records(
        f"SELECT status, commit_receipt_id, commit_outcome, error_message "
        f"FROM [{route.state_database}].[system].[dpone_load_audit] "
        "WHERE status = N'failed'",
        as_dict=True,
    )
    assert len(failed_audits) == 1
    assert failed_audits[0]["commit_receipt_id"] is None
    assert failed_audits[0]["commit_outcome"] is None
    assert "simulated failure before server commit" in str(failed_audits[0]["error_message"])


def write_certification_evidence(
    *,
    route: Any,
    final_checkpoint: int,
    source_keys: set[str],
    state_counts: dict[str, int],
    staging_count: int,
    commit_ack_outcome: AtomicCommitOutcome,
    successful_runs: list[tuple[str, dict[str, Any]]],
) -> None:
    """Write the sanitized machine-readable acceptance artifact."""

    commit_sha = os.environ.get("GITHUB_SHA") or os.environ.get("CI_COMMIT_SHA")
    running_in_ci = os.environ.get("CI", "").lower() == "true" or os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
    if running_in_ci and not commit_sha:
        raise AssertionError("live certification requires GITHUB_SHA or CI_COMMIT_SHA")
    if commit_sha and re.fullmatch(r"[0-9a-fA-F]{40,64}", commit_sha) is None:
        raise AssertionError("live certification commit SHA has an invalid shape")

    postgres_version = route.postgres.get_records("SHOW server_version", as_dict=True)[0]["server_version"]
    mssql_version = route.target.get_records(
        "SELECT CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')) AS product_version, "
        "CONVERT(nvarchar(max), @@VERSION) AS version_banner",
        as_dict=True,
    )[0]
    receipts = route.state.get_records(
        f"SELECT receipt_id, load_id, previous_xmin, candidate_xmin, "
        f"source_snapshot_token, committed_at "
        f"FROM [{route.state_database}].[system].[dpone_commit_receipt] "
        "ORDER BY committed_at, receipt_id",
        as_dict=True,
    )
    failed_audit = route.state.get_records(
        f"SELECT load_id, status, commit_receipt_id, commit_outcome "
        f"FROM [{route.state_database}].[system].[dpone_load_audit] "
        "WHERE status = N'failed'",
        as_dict=True,
    )
    evidence = {
        "schema_version": "1.0",
        "status": "passed_partial",
        "release_ready": False,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="microseconds"),  # noqa: UP017
        "release": {
            "commit_sha": commit_sha or "local-unpinned",
            "ci_pinned": bool(commit_sha),
        },
        "vendor_versions": {
            "postgres": str(postgres_version),
            "mssql_product_version": str(mssql_version["product_version"]),
            "mssql_version_banner": str(mssql_version["version_banner"]),
        },
        "route": "postgres_xmin_same_snapshot_keys_to_mssql",
        "runtime_path": ["ETLProcessor", "ExtractedPayloadLoadService", "PayloadLoadService", "MSSQLSink"],
        "state_contract": {
            "provisioning": "external",
            "atomicity": "target_atomic",
            "separate_same_instance_database": True,
            "receipt_count": state_counts["receipt_count"],
            "checkpoint_count": state_counts["checkpoint_count"],
            "final_checkpoint": final_checkpoint,
            "receipts": [_receipt_summary(row) for row in receipts],
        },
        "lifecycle": {
            "successful_runs": 9,
            "baseline_includes_pre_2026_row": True,
            "identical_rerun_no_business_dml": True,
            "update_insert_delete": True,
            "repeated_delete_timestamp_stable": True,
            "reactivation_clears_timestamp": True,
            "reactivation_noop_stable": True,
            "delete_again_timestamp_refreshed": True,
        },
        "fault_matrix": {
            "commit_ack_loss": {
                "receipt_probe_outcome": commit_ack_outcome.value,
                "blind_replay": False,
            },
            "before_commit": {
                "typed_outcome_unknown": True,
                "target_rolled_back": True,
                "checkpoint_rolled_back": True,
                "recovery_run_succeeded": True,
                "failed_attempt": _failed_attempt_summary(failed_audit),
            },
        },
        "coverage": {
            "completed": [
                "sequential_lifecycle",
                "commit_ack_loss_receipt_probe",
                "precommit_rollback_and_recovery",
            ],
            "required_not_executed": [
                "parallel_applock_and_stale_cas",
                "concurrent_source_mutation_same_snapshot",
                "state_permission_denial",
                "per_dml_and_checkpoint_fault_injection",
                "empty_guard_and_checksum_negative_cases",
            ],
        },
        "runs": [_run_summary(name, result) for name, result in successful_runs],
        "fidelity": {
            "text_key_control_characters_and_unicode": True,
            "null_and_empty_text_distinct": True,
            "float53_exact": True,
            "timestamptz_utc": True,
        },
        "reconciliation": {
            "active_keys": sorted(source_keys),
            "bidirectional_key_diff_zero": True,
            "successful_run_staging_cleanup_zero": True,
            "commit_unknown_forensic_staging_tables_preserved": staging_count,
            "forensic_staging_removed_by_disposable_database_teardown": True,
        },
    }
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE_PATH.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _run_summary(name: str, result: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "staging_rows",
        "inserted_rows",
        "updated_rows",
        "reactivated_rows",
        "unchanged_rows",
        "soft_deleted_rows",
        "active_rows",
        "total_rows",
    )
    outcome = result["commit_outcome"]
    return {
        "scenario": name,
        "load_id": str(result["load_id"]),
        "receipt_id": str(result["commit_receipt_id"]),
        "commit_outcome": outcome.value if isinstance(outcome, AtomicCommitOutcome) else str(outcome),
        "metrics": {field: int(result[field]) for field in fields},
    }


def _receipt_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "receipt_id": str(row["receipt_id"]),
        "load_id": str(row["load_id"]),
        "previous_xmin": None if row["previous_xmin"] is None else int(row["previous_xmin"]),
        "candidate_xmin": int(row["candidate_xmin"]),
        "source_snapshot_token": str(row["source_snapshot_token"]),
        "committed_at": row["committed_at"].isoformat(),
    }


def _failed_attempt_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    assert len(rows) == 1
    row = rows[0]
    return {
        "load_id": str(row["load_id"]),
        "status": str(row["status"]),
        "receipt_id": row["commit_receipt_id"],
        "commit_outcome": row["commit_outcome"],
    }


def _source_table(route: Any) -> Any:
    return sql.SQL("{}.{}").format(
        sql.Identifier(route.source_schema),
        sql.Identifier(SOURCE_TABLE),
    )
