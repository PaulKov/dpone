"""Vendor-live lifecycle proof for PostgreSQL XMin → MSSQL reconciliation."""

from __future__ import annotations

import math
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from tests.integration.postgres import postgres_xmin_mssql_snapshot_live_matrix as live_matrix
from tests.integration.postgres import postgres_xmin_mssql_snapshot_live_scenario as scenario
from tests.integration.postgres.postgres_live_support import postgres_mssql_enabled
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    SPECIAL_KEY,
    provision_snapshot_route,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
    pytest.mark.route_live_wide,
]

_BASE_EXECUTION_DATE = datetime(2026, 8, 15, 6, 30)


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_xmin_key_snapshot_soft_delete_lifecycle_via_standard_runtime(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Prove atomic state, exact metrics, fidelity, and tombstone semantics."""

    with provision_snapshot_route(tmp_path) as route:
        assert route.target_database != route.state_database
        before_baseline = _xmin_business_image(route)
        baseline = route.run(_execution_date(0))
        first_checkpoint = scenario.assert_run_evidence(
            route,
            baseline,
            _execution_date(0),
            expected={
                "staging_rows": 3,
                "inserted_rows": 3,
                "updated_rows": 0,
                "reactivated_rows": 0,
                "unchanged_rows": 0,
                "soft_deleted_rows": 0,
                "active_rows": 3,
                "total_rows": 3,
            },
            previous_checkpoint=None,
        )
        baseline_rows = scenario.rows_by_key(route.target_rows())
        scenario.assert_baseline_fidelity(baseline_rows)
        published_columns = {
            name.lower()
            for name, _dtype in route.target.fetch_schema(
                "sample_metrics",
                "metric_values",
                database=route.target_database,
            )
        }
        assert "__dpone__xmin" not in published_columns
        assert "__dpone__deleted_at" in published_columns
        assert "__dpone__is_deleted" not in published_columns
        stable_hash = baseline_rows[SPECIAL_KEY]["__dpone__row_hash"]
        stable_loaded_at = baseline_rows[SPECIAL_KEY]["__dpone__loaded_at"]
        after_baseline = _xmin_business_image(route)
        route_live_recorder.observe_case(
            "xmin_reconciliation",
            "baseline",
            before_image=before_baseline,
            after_image=after_baseline,
            observations=_xmin_observations(route, baseline, first_checkpoint),
        )

        before_identical = _xmin_business_image(route)
        identical = route.run(_execution_date(1))
        second_checkpoint = scenario.assert_run_evidence(
            route,
            identical,
            _execution_date(1),
            expected={
                "staging_rows": 0,
                "inserted_rows": 0,
                "updated_rows": 0,
                "reactivated_rows": 0,
                "unchanged_rows": 3,
                "soft_deleted_rows": 0,
                "active_rows": 3,
                "total_rows": 3,
            },
            previous_checkpoint=first_checkpoint,
        )
        identical_rows = scenario.rows_by_key(route.target_rows())
        assert identical_rows[SPECIAL_KEY]["__dpone__row_hash"] == stable_hash
        assert identical_rows[SPECIAL_KEY]["__dpone__loaded_at"] == stable_loaded_at
        route_live_recorder.observe_case(
            "xmin_reconciliation",
            "identical_rerun",
            before_image=before_identical,
            after_image=_xmin_business_image(route),
            observations=_xmin_observations(route, identical, second_checkpoint),
        )

        scenario.mutate_update_insert_delete(route)
        before_changed = _xmin_business_image(route)
        changed = route.run(_execution_date(2))
        third_checkpoint = scenario.assert_run_evidence(
            route,
            changed,
            _execution_date(2),
            expected={
                "staging_rows": 2,
                "inserted_rows": 1,
                "updated_rows": 1,
                "reactivated_rows": 0,
                "unchanged_rows": 1,
                "soft_deleted_rows": 1,
                "active_rows": 3,
                "total_rows": 4,
            },
            previous_checkpoint=second_checkpoint,
        )
        changed_rows = scenario.rows_by_key(route.target_rows())
        first_deleted_at = changed_rows["empty"]["__dpone__deleted_at"]
        assert first_deleted_at is not None
        assert changed_rows[SPECIAL_KEY]["note"] == "updated\tpayload\nmarker\x1d"
        assert changed_rows[SPECIAL_KEY]["__dpone__row_hash"] != stable_hash
        assert changed_rows[SPECIAL_KEY]["__dpone__loaded_at"] > stable_loaded_at
        assert math.isclose(
            float(changed_rows[SPECIAL_KEY]["metric_value"]),
            1.0000000000000002,
            rel_tol=0.0,
            abs_tol=0.0,
        )
        assert changed_rows["new-Ω"]["note"] == ""
        route_live_recorder.observe_case(
            "xmin_reconciliation",
            "update_insert_delete",
            before_image=before_changed,
            after_image=_xmin_business_image(route),
            observations=_xmin_observations(route, changed, third_checkpoint),
        )

        before_repeated_delete = _xmin_business_image(route)
        repeated_delete = route.run(_execution_date(3))
        fourth_checkpoint = scenario.assert_run_evidence(
            route,
            repeated_delete,
            _execution_date(3),
            expected={
                "staging_rows": 0,
                "inserted_rows": 0,
                "updated_rows": 0,
                "reactivated_rows": 0,
                "unchanged_rows": 3,
                "soft_deleted_rows": 0,
                "active_rows": 3,
                "total_rows": 4,
            },
            previous_checkpoint=third_checkpoint,
        )
        assert scenario.rows_by_key(route.target_rows())["empty"]["__dpone__deleted_at"] == first_deleted_at
        route_live_recorder.observe_case(
            "xmin_reconciliation",
            "repeated_delete",
            before_image=before_repeated_delete,
            after_image=_xmin_business_image(route),
            observations=_xmin_observations(route, repeated_delete, fourth_checkpoint),
        )

        scenario.reactivate(route)
        before_reactivated = _xmin_business_image(route)
        reactivated = route.run(_execution_date(4))
        fifth_checkpoint = scenario.assert_run_evidence(
            route,
            reactivated,
            _execution_date(4),
            expected={
                "staging_rows": 1,
                "inserted_rows": 0,
                "updated_rows": 0,
                "reactivated_rows": 1,
                "unchanged_rows": 3,
                "soft_deleted_rows": 0,
                "active_rows": 4,
                "total_rows": 4,
            },
            previous_checkpoint=fourth_checkpoint,
        )
        active_again = scenario.rows_by_key(route.target_rows())["empty"]
        assert active_again["__dpone__deleted_at"] is None
        assert active_again["note"] == "reactivated"
        assert float(active_again["metric_value"]) == 2.5
        assert active_again["__dpone__row_hash"] != changed_rows["empty"]["__dpone__row_hash"]
        assert active_again["__dpone__loaded_at"] > changed_rows["empty"]["__dpone__loaded_at"]
        reactivated_hash = active_again["__dpone__row_hash"]
        reactivated_loaded_at = active_again["__dpone__loaded_at"]
        route_live_recorder.observe_case(
            "xmin_reconciliation",
            "reactivate",
            before_image=before_reactivated,
            after_image=_xmin_business_image(route),
            observations=_xmin_observations(route, reactivated, fifth_checkpoint),
        )

        before_identical_after_reactivate = _xmin_business_image(route)
        identical_after_reactivation = route.run(_execution_date(5))
        sixth_checkpoint = scenario.assert_run_evidence(
            route,
            identical_after_reactivation,
            _execution_date(5),
            expected={
                "staging_rows": 0,
                "inserted_rows": 0,
                "updated_rows": 0,
                "reactivated_rows": 0,
                "unchanged_rows": 4,
                "soft_deleted_rows": 0,
                "active_rows": 4,
                "total_rows": 4,
            },
            previous_checkpoint=fifth_checkpoint,
        )
        stable_reactivation = scenario.rows_by_key(route.target_rows())["empty"]
        assert stable_reactivation["__dpone__row_hash"] == reactivated_hash
        assert stable_reactivation["__dpone__loaded_at"] == reactivated_loaded_at
        route_live_recorder.observe_case(
            "xmin_reconciliation",
            "identical_after_reactivate",
            before_image=before_identical_after_reactivate,
            after_image=_xmin_business_image(route),
            observations=_xmin_observations(route, identical_after_reactivation, sixth_checkpoint),
        )

        scenario.delete_again(route)
        deleted_again = route.run(_execution_date(6))
        seventh_checkpoint = scenario.assert_run_evidence(
            route,
            deleted_again,
            _execution_date(6),
            expected={
                "staging_rows": 0,
                "inserted_rows": 0,
                "updated_rows": 0,
                "reactivated_rows": 0,
                "unchanged_rows": 3,
                "soft_deleted_rows": 1,
                "active_rows": 3,
                "total_rows": 4,
            },
            previous_checkpoint=sixth_checkpoint,
        )
        second_deleted_at = scenario.rows_by_key(route.target_rows())["empty"]["__dpone__deleted_at"]
        assert second_deleted_at is not None
        assert second_deleted_at > first_deleted_at

        scenario.update_special(route, metric_value=3.5, note="commit acknowledged only by receipt")
        before_commit_probe = _xmin_business_image(route)
        real_commit = route.target.commit_transaction

        def commit_then_lose_ack() -> None:
            real_commit()
            raise RuntimeError("simulated commit acknowledgement loss")

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(route.target, "commit_transaction", commit_then_lose_ack)
            acknowledged_by_receipt = route.run(_execution_date(7))
        eighth_checkpoint = scenario.assert_run_evidence(
            route,
            acknowledged_by_receipt,
            _execution_date(7),
            expected={
                "staging_rows": 1,
                "inserted_rows": 0,
                "updated_rows": 1,
                "reactivated_rows": 0,
                "unchanged_rows": 2,
                "soft_deleted_rows": 0,
                "active_rows": 3,
                "total_rows": 4,
            },
            previous_checkpoint=seventh_checkpoint,
            expected_outcome=AtomicCommitOutcome.COMMITTED_AFTER_RECEIPT_PROBE,
        )
        assert scenario.staging_table_count(route) == 0
        route_live_recorder.observe_case(
            "xmin_reconciliation",
            "commit_outcome_unknown_fresh_probe",
            before_image=before_commit_probe,
            after_image=_xmin_business_image(route),
            observations=_xmin_observations(route, acknowledged_by_receipt, eighth_checkpoint),
        )

        # Persist the receipt/checkpoint inside the target transaction, then
        # fail before the runtime invokes COMMIT.  This is a provably safe
        # pre-commit boundary: target DML plus state DML must roll back
        # together and the next attempt may retry normally.
        before_fault_rows = route.target_rows()
        before_fault_receipts = scenario.state_counts(route)
        before_fault_session = route.target_transaction_state()
        assert before_fault_session["transaction_count"] == 0
        scenario.update_special(route, metric_value=-7.25, note="must rollback before commit")
        real_compare_and_set = route.checkpoint_storage.compare_and_set_with_receipt

        def persist_receipt_then_fail(**kwargs):
            real_compare_and_set(**kwargs)
            raise RuntimeError("simulated failure before server commit")

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(route.checkpoint_storage, "compare_and_set_with_receipt", persist_receipt_then_fail)
            with pytest.raises(RuntimeError, match="simulated failure before server commit"):
                route.run(_execution_date(8))

        assert route.target_rows() == before_fault_rows
        after_fault_counts = scenario.state_counts(route)
        assert after_fault_counts == before_fault_receipts
        assert scenario.current_checkpoint(route) == eighth_checkpoint
        scenario.assert_failed_run_evidence(route, _execution_date(8))
        after_fault_session = route.target_transaction_state()
        assert after_fault_session == {
            "session_id": before_fault_session["session_id"],
            "transaction_count": 0,
        }
        assert route.blocked_by_target_session(after_fault_session["session_id"]) == 0
        assert scenario.staging_table_count(route) == 0

        recovery_started = time.monotonic()
        recovered = route.run(_execution_date(9))
        recovery_seconds = time.monotonic() - recovery_started
        assert recovery_seconds < 120
        final_checkpoint = scenario.assert_run_evidence(
            route,
            recovered,
            _execution_date(9),
            expected={
                "staging_rows": 1,
                "inserted_rows": 0,
                "updated_rows": 1,
                "reactivated_rows": 0,
                "unchanged_rows": 2,
                "soft_deleted_rows": 0,
                "active_rows": 3,
                "total_rows": 4,
            },
            previous_checkpoint=eighth_checkpoint,
        )
        source_keys = {
            str(row["metric_code"])
            for row in route.postgres.get_records(
                scenario.source_key_query(route),
                as_dict=True,
            )
        }
        active_target_keys = {
            key for key, row in scenario.rows_by_key(route.target_rows()).items() if row["__dpone__deleted_at"] is None
        }
        assert source_keys == active_target_keys == {SPECIAL_KEY, "nullable", "new-Ω"}

        state_counts = scenario.state_counts(route)
        assert state_counts == {"receipt_count": 9, "checkpoint_count": 1}
        staging_count = scenario.staging_table_count(route)
        assert staging_count == 0
        scenario.write_certification_evidence(
            route=route,
            final_checkpoint=final_checkpoint,
            source_keys=source_keys,
            state_counts=state_counts,
            staging_count=staging_count,
            commit_ack_outcome=acknowledged_by_receipt["commit_outcome"],
            successful_runs=[
                ("baseline", baseline),
                ("identical_rerun", identical),
                ("update_insert_delete", changed),
                ("repeated_delete", repeated_delete),
                ("reactivation", reactivated),
                ("reactivation_noop", identical_after_reactivation),
                ("delete_again", deleted_again),
                ("commit_ack_loss", acknowledged_by_receipt),
                ("recovery_after_precommit_fault", recovered),
            ],
        )


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_xmin_mssql_real_vendor_failure_and_concurrency_matrix(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Complete the acceptance matrix before enriching partial evidence."""

    completed = live_matrix.run_vendor_live_matrix(
        tmp_path / "matrix",
        route_live_recorder=route_live_recorder,
    )

    assert completed == live_matrix.REQUIRED_LIVE_MATRIX_CASES
    evidence = scenario.EVIDENCE_PATH.read_text(encoding="utf-8")
    assert '"status": "passed_partial"' in evidence
    assert '"release_ready": false' in evidence
    assert '"required_not_executed": []' in evidence


def _execution_date(offset: int) -> datetime:
    return _BASE_EXECUTION_DATE + timedelta(minutes=offset)


def _xmin_business_image(route: object) -> dict[str, object]:
    target_rows = getattr(route, "target_rows")
    return {"target_rows": target_rows()}


def _xmin_observations(
    route: object,
    result: dict[str, object],
    checkpoint: int,
) -> dict[str, object]:
    return {
        "result": dict(result),
        "checkpoint": checkpoint,
        "state_counts": scenario.state_counts(route),
        "staging_objects": scenario.staging_table_count(route),
    }
