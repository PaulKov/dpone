"""Reviewed 30-case PostgreSQL→MSSQL backfill orchestration proof.

Each pytest item owns one disposable PostgreSQL relation and two disposable
SQL Server databases.  A baseline is installed through ``DefaultProcessRunner``
and the reviewed backfill is then executed through the same public entrypoint.
No item records an observation until target, receipts, ledger, catalog,
staging, session, and artifact-cleanup assertions all pass.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from tests.integration.postgres.postgres_live_support import postgres_mssql_enabled
from tests.integration.postgres.postgres_mssql_backfill_lifecycle_live_support import (
    ReviewedBackfillLifecycleController,
    ReviewedBackfillOrchestratorFactory,
)
from tests.integration.postgres.postgres_mssql_backfill_orchestration_live_support import (
    SUITE_ID,
    ReviewedBackfillCase,
    apply_runtime_environment,
    backfill_load_config,
    invoke_public_process,
    operational_image,
    reviewed_backfill_cases,
    seed_target_with_public_runner,
    semantic_image,
    target_primary_key,
)
from tests.integration.postgres.postgres_mssql_production_hydration_live_support import (
    production_hydration_live_fixture,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
]

_CASES = reviewed_backfill_cases()


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
@pytest.mark.parametrize("case", _CASES, ids=lambda case: case.case_id)
def test_postgres_mssql_backfill_orchestration_reviewed_case_live(
    case: ReviewedBackfillCase,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Execute exactly one reviewed case and publish only real-vendor evidence."""

    case_root = tmp_path / case.case_id
    with production_hydration_live_fixture(case_root) as live:
        apply_runtime_environment(monkeypatch, live)
        baseline = seed_target_with_public_runner(live)
        assert baseline.status == "success"
        primary_key = target_primary_key(live)
        assert [row["column_name"] for row in primary_key] == ["id"]
        assert all(bool(row["is_primary_key"]) and bool(row["is_unique"]) for row in primary_key)

        before = semantic_image(live)
        before_operational = operational_image(live, live.transfer_root)
        load_config = backfill_load_config(
            live,
            case,
            state_dir=case_root / "backfill-state",
        )
        lifecycle = ReviewedBackfillLifecycleController(
            case.lifecycle,
            retry_barrier_marker=(case_root / "retry-primary-fault.marker")
            if case.lifecycle == "retry_resume" and case.parallel_workers > 1
            else None,
        )
        orchestrator_factory = ReviewedBackfillOrchestratorFactory(lifecycle)
        invocation_id = f"backfill-{case.case_id}"
        retry_boundary = None
        if case.lifecycle == "retry_resume":
            first_attempt = invoke_public_process(
                live,
                load_config,
                invocation_id=invocation_id,
                backfill_orchestrator_factory=orchestrator_factory,
            )
            after_first_attempt = semantic_image(live)
            after_first_operational = operational_image(live, live.transfer_root)
            assert first_attempt.error is not None
            assert "DPONE_BACKFILL_CHUNK_LEASE_LOST" in str(first_attempt.error)
            assert len(after_first_attempt["committed_receipts"]) == len(before["committed_receipts"]) + 1
            assert after_first_attempt["committed_chunks"] == ()
            assert after_first_operational["staging_objects"] == ()
            assert after_first_operational["transfer_files"] == ()
            lifecycle.begin_retry()
            retry_boundary = {
                "first_attempt_error": str(first_attempt.error),
                "after_first_attempt": after_first_attempt,
                "after_first_counts": after_first_operational["generic_state_counts"],
                "after_first_chunks": after_first_operational["backfill_chunks"],
            }
        invocation = invoke_public_process(
            live,
            load_config,
            invocation_id=invocation_id,
            backfill_orchestrator_factory=orchestrator_factory,
        )
        after = semantic_image(live)
        after_operational = operational_image(live, live.transfer_root)

        if retry_boundary is not None:
            retry_boundary["receipt_rows_created_by_retry"] = (
                after_operational["generic_state_counts"]["dpone_load_receipt"]
                - retry_boundary["after_first_counts"]["dpone_load_receipt"]
            )
            retry_boundary["operation_rows_created_by_retry"] = (
                after_operational["generic_state_counts"]["dpone_load_operation"]
                - retry_boundary["after_first_counts"]["dpone_load_operation"]
            )
            if (
                retry_boundary["receipt_rows_created_by_retry"] != 2
                or retry_boundary["operation_rows_created_by_retry"] != 2
            ):
                pytest.fail(
                    "DPONE_BACKFILL_PARALLEL_RETRY_GAP:\n"
                    + json.dumps(
                        {
                            "case_id": case.case_id,
                            "invocation_error": str(invocation.error or ""),
                            "invocation_traceback": invocation.traceback,
                            "process_status": getattr(invocation.result, "status", None),
                            "backfill_result": (getattr(invocation.result, "details", None) or {}).get("backfill"),
                            "after_first_counts": retry_boundary["after_first_counts"],
                            "final_counts": after_operational["generic_state_counts"],
                            "after_first_chunks": _compact_chunk_states(retry_boundary["after_first_chunks"]),
                            "final_chunks": _compact_chunk_states(after_operational["backfill_chunks"]),
                            "final_campaign": _compact_latest_campaign(after_operational["backfill_campaigns"]),
                            "lifecycle_events": lifecycle.events,
                            "receipt_rows_created_by_retry": retry_boundary["receipt_rows_created_by_retry"],
                            "operation_rows_created_by_retry": retry_boundary["operation_rows_created_by_retry"],
                        },
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                        default=str,
                    ),
                    pytrace=False,
                )
            replay_events = tuple(event for event in lifecycle.events if event["event"] == "source_free_receipt_replay")
            assert len(replay_events) == 1
            assert replay_events[0]["replay_suppressed"] is True
            retry_boundary["source_free_receipt_replay"] = replay_events[0]

        error_text = str(invocation.error or "")
        if _is_orchestration_contract_gap(error_text) or (
            invocation.error is not None and not _is_expected_lifecycle_error(case, error_text)
        ):
            pytest.fail(
                "DPONE_BACKFILL_ORCHESTRATION_PRODUCTION_GAP:\n"
                + json.dumps(
                    {
                        "case_id": case.case_id,
                        "reviewed_parameters": case.parameters,
                        "error_type": type(invocation.error).__name__,
                        "error": error_text,
                        "traceback": invocation.traceback,
                        "lifecycle_events": lifecycle.events,
                        "business_target_unchanged": before["business_rows"] == after["business_rows"],
                        "committed_checkpoint_unchanged": before == after,
                        "before": before_operational,
                        "after": after_operational,
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                    default=str,
                ),
                pytrace=False,
            )

        if case.lifecycle in {"success", "retry_resume"}:
            assert invocation.error is None
            assert invocation.result is not None
            assert invocation.result.status == "success"
            assert before != after
            assert len(after["committed_chunks"]) == 3
            assert after_operational["staging_objects"] == ()
            assert after_operational["transfer_files"] == ()
        else:
            if invocation.error is None:
                pytest.fail(
                    "DPONE_BACKFILL_LIFECYCLE_FAULT_INJECTION_GAP:\n"
                    + json.dumps(
                        {
                            "case_id": case.case_id,
                            "reviewed_parameters": case.parameters,
                            "expected_lifecycle": case.lifecycle,
                            "actual_status": getattr(invocation.result, "status", None),
                            "business_target_unchanged": before["business_rows"] == after["business_rows"],
                            "committed_checkpoint_unchanged": before == after,
                            "before": before_operational,
                            "after": after_operational,
                        },
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                        default=str,
                    ),
                    pytrace=False,
                )
            assert before == after
            assert after_operational["staging_objects"] == ()
            assert after_operational["transfer_files"] == ()

        route_live_recorder.observe_parameters(
            SUITE_ID,
            case.parameters,
            before_image=before,
            after_image=after,
            observations=_observations(
                case,
                invocation,
                after_operational,
                lifecycle_events=lifecycle.events,
                retry_boundary=retry_boundary,
            ),
        )


def _is_orchestration_contract_gap(error: str) -> bool:
    normalized = error.casefold()
    return (
        ("mssql.strategy.backfill.backfill" in normalized and ("unknown" in normalized or "unsupported" in normalized))
        or "lease_seconds" in normalized
        or "heartbeat_seconds" in normalized
    )


def _is_expected_lifecycle_error(case: ReviewedBackfillCase, error: str) -> bool:
    """Accept only the stable fence corresponding to the reviewed fault."""

    expected_tokens = {
        "heartbeat_failure": (
            "DPONE_BACKFILL_CHUNK_LEASE_HEARTBEAT_LOST",
            "mssql_transaction.operation_lease_heartbeat_lost",
        ),
        "lease_expiry": (
            "DPONE_BACKFILL_CHUNK_LEASE_LOST",
            "mssql_transaction.operation_lease_expired",
        ),
        "worker_failure": ("DPONE_BACKFILL_WORKER_FAILURE",),
    }
    return any(token in error for token in expected_tokens.get(case.lifecycle, ()))


def _compact_chunk_states(rows: Any) -> tuple[dict[str, Any], ...]:
    compact = []
    for row in rows:
        details = json.loads(str(row["details_json"]))
        compact.append(
            {
                "index": row["chunk_index"],
                "status": row["status"],
                "attempts": details.get("attempts"),
                "lease_owner": details.get("lease_owner"),
                "lease_expires_at": details.get("lease_expires_at"),
                "error": details.get("error"),
                "updated_at": details.get("updated_at"),
            }
        )
    return tuple(compact)


def _compact_latest_campaign(rows: Any) -> dict[str, Any] | None:
    if not rows:
        return None
    row = rows[-1]
    details = json.loads(str(row["details_json"]))
    return {
        "run_key": row["run_key"],
        "status": row["status"],
        "plan_hash": row["plan_hash"],
        "config_hash": row["config_hash"],
        "lock_owner": details.get("lock_owner"),
        "lock_expires_at": details.get("lock_expires_at"),
        "updated_at": details.get("updated_at"),
    }


def _observations(
    case: ReviewedBackfillCase,
    invocation: Any,
    image: dict[str, Any],
    *,
    lifecycle_events: tuple[dict[str, Any], ...],
    retry_boundary: dict[str, Any] | None,
) -> dict[str, Any]:
    details = getattr(invocation.result, "details", None) or {}
    return {
        "runtime_entrypoint": "DefaultProcessRunner/ETLProcessor",
        "inner_mode": case.inner_mode,
        "parallel_workers": case.parallel_workers,
        "lifecycle": case.lifecycle,
        "lease_ttl_minutes": case.lease_ttl_minutes,
        "error_type": type(invocation.error).__name__ if invocation.error is not None else None,
        "error": str(invocation.error) if invocation.error is not None else None,
        "lifecycle_events": lifecycle_events,
        "retry_boundary": retry_boundary,
        "backfill_result": details.get("backfill"),
        "generic_state_counts": image["generic_state_counts"],
        "backfill_chunks": image["backfill_chunks"],
        "backfill_campaigns": image["backfill_campaigns"],
        "target_catalog": image["target_catalog"],
        "target_primary_key": image["target_primary_key"],
        "staging_objects": image["staging_objects"],
        "target_sessions": image["target_sessions"],
        "transfer_files": image["transfer_files"],
    }
