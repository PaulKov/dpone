"""Vendor-live concurrency, MVCC, permission, and fault acceptance matrix."""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from psycopg import sql
from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from tests.integration.postgres import postgres_xmin_mssql_snapshot_live_scenario as scenario
from tests.integration.postgres.postgres_live_support import postgres_connector
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_matrix_failures import (
    run_failure_matrix,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_matrix_support import (
    TransactionLockProbe,
    deny_checkpoint_writes,
    fork_snapshot_route,
    provision_limited_principal,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    SOURCE_TABLE,
    SPECIAL_KEY,
    provision_snapshot_route,
)

REQUIRED_LIVE_MATRIX_CASES = frozenset(
    {
        "parallel_applock_and_stale_cas",
        "concurrent_source_mutation_same_snapshot",
        "state_permission_denial",
        "per_dml_and_checkpoint_fault_injection",
        "empty_guard_and_checksum_negative_cases",
    }
)
_BASE_DATE = datetime(2026, 8, 16, 6, 30)


def run_vendor_live_matrix(
    root: Path,
    *,
    route_live_recorder: RouteLiveObservationRecorder,
) -> set[str]:
    """Execute every required real-vendor case, then update partial evidence."""

    completed: set[str] = set()
    _same_snapshot_source_mutation(
        _work_dir(root, "same-snapshot"),
        route_live_recorder,
    )
    completed.add("concurrent_source_mutation_same_snapshot")
    _parallel_applock_and_stale_cas(
        _work_dir(root, "parallel"),
        route_live_recorder,
    )
    completed.add("parallel_applock_and_stale_cas")
    _state_permission_denial(
        _work_dir(root, "permission"),
        route_live_recorder,
    )
    completed.add("state_permission_denial")
    completed.update(
        run_failure_matrix(
            _work_dir(root, "failures"),
            route_live_recorder=route_live_recorder,
        )
    )
    _record_completed_matrix(completed)
    return completed


def _same_snapshot_source_mutation(
    work_dir: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with provision_snapshot_route(work_dir) as route:
        first_checkpoint = _baseline(route, _date(0))
        before = _business_image(route)
        mutator = postgres_connector()
        strategy = route.processor.source._xmin_extract
        real_delta_export = strategy._export_to_file_whole
        real_key_export = strategy._export_key_snapshot_file
        delta_export_calls = 0
        key_export_calls = 0

        def export_delta(*args: Any, **kwargs: Any) -> Any:
            nonlocal delta_export_calls
            # This fault boundary wraps a private export method whose lifecycle
            # keywords evolve with snapshot semantics.  Forward the complete
            # call so the fixture cannot silently drop completion authority.
            artifact = real_delta_export(*args, **kwargs)
            delta_export_calls += 1
            if delta_export_calls == 1:
                _insert_source_row(
                    mutator,
                    route.source_schema,
                    "snapshot-concurrent",
                    17.25,
                    "committed between delta and key scans",
                )
            return artifact

        def export_keys(
            query: Any,
            load_config: Any,
            *,
            snapshot_lease: Any,
        ) -> Any:
            nonlocal key_export_calls
            artifact = real_key_export(
                query,
                load_config,
                snapshot_lease=snapshot_lease,
            )
            key_export_calls += 1
            return artifact

        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(strategy, "_export_to_file_whole", export_delta)
                patch.setattr(strategy, "_export_key_snapshot_file", export_keys)
                isolated = route.run(_date(1))
        finally:
            mutator.close()

        assert delta_export_calls == key_export_calls == 1
        second_checkpoint = scenario.assert_run_evidence(
            route,
            isolated,
            _date(1),
            expected=_metrics(staging=0, unchanged=3, active=3, total=3),
            previous_checkpoint=first_checkpoint,
        )
        assert "snapshot-concurrent" not in scenario.rows_by_key(route.target_rows())
        source_keys = {
            str(row["metric_code"])
            for row in route.postgres.get_records(scenario.source_key_query(route), as_dict=True)
        }
        assert "snapshot-concurrent" in source_keys

        caught_up = route.run(_date(2))
        scenario.assert_run_evidence(
            route,
            caught_up,
            _date(2),
            expected=_metrics(staging=1, inserted=1, unchanged=3, active=4, total=4),
            previous_checkpoint=second_checkpoint,
        )
        assert "snapshot-concurrent" in scenario.rows_by_key(route.target_rows())
        route_live_recorder.observe_case(
            "xmin_reconciliation",
            "concurrent_source_mutation_same_snapshot",
            before_image=before,
            after_image=_business_image(route),
            observations={
                "isolated_checkpoint": second_checkpoint,
                "isolated_run": dict(isolated),
                "catch_up_run": dict(caught_up),
                "source_keys": sorted(source_keys),
                "same_exported_snapshot": True,
            },
        )


def _parallel_applock_and_stale_cas(
    work_dir: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with provision_snapshot_route(work_dir) as route:
        _baseline(route, _date(0))
        before_xmin, before_revision = _state_version(route)
        scenario.update_special(route, metric_value=19.5, note="parallel-attempt")
        before = _business_image(route)
        with (
            fork_snapshot_route(route, _work_dir(work_dir, "attempt-a")) as first,
            fork_snapshot_route(route, _work_dir(work_dir, "attempt-b")) as second,
        ):
            extraction_barrier = threading.Barrier(2)
            _hold_after_extract(first, extraction_barrier)
            _hold_after_extract(second, extraction_barrier)
            lock_probe = TransactionLockProbe()
            lock_probe.wrap(first.target)
            lock_probe.wrap(second.target)
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = (
                    pool.submit(first.run, _date(1)),
                    pool.submit(second.run, _date(2)),
                )
                try:
                    assert lock_probe.first_acquired.wait(timeout=60)
                    assert lock_probe.both_requested.wait(timeout=60)
                    assert lock_probe.acquired_count == 1
                finally:
                    lock_probe.release_first.set()
                outcomes = [_future_outcome(future) for future in futures]

        successes = [value for value in outcomes if isinstance(value, dict)]
        failures = [value for value in outcomes if isinstance(value, BaseException)]
        assert len(successes) == len(failures) == 1
        assert "DPONE_XMIN_CHECKPOINT_CAS_MISMATCH" in str(failures[0])
        assert lock_probe.request_count == lock_probe.acquired_count == 2
        assert int(successes[0]["updated_rows"]) == 1
        assert int(successes[0]["unchanged_rows"]) == 2
        after_xmin, after_revision = _state_version(route)
        assert after_xmin >= before_xmin
        assert after_revision == before_revision + 1
        assert scenario.state_counts(route) == {"receipt_count": 2, "checkpoint_count": 1}
        assert scenario.rows_by_key(route.target_rows())[SPECIAL_KEY]["note"] == "parallel-attempt"
        assert scenario.staging_table_count(route) == 0
        route_live_recorder.observe_case(
            "xmin_reconciliation",
            "parallel_applock_and_stale_cas",
            before_image=before,
            after_image=_business_image(route),
            observations={
                "success_count": len(successes),
                "failure_count": len(failures),
                "stale_cas_blocker": str(failures[0]),
                "applock_requests": lock_probe.request_count,
                "applock_acquisitions": lock_probe.acquired_count,
                "state_revision_before": before_revision,
                "state_revision_after": after_revision,
            },
        )


def _state_permission_denial(
    work_dir: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    with provision_snapshot_route(work_dir) as route:
        with provision_limited_principal(route) as principal:
            with fork_snapshot_route(route, work_dir / "restricted", principal=principal) as restricted:
                _baseline(restricted, _date(0))
                strategy = restricted.processor.source._xmin_extract
                assert len(strategy.physical_target_identity(restricted.load_config)) == 32
                scenario.update_special(restricted, metric_value=23.75, note="permission-rollback")
                before_rows = restricted.target_rows()
                before_state = (scenario.state_counts(restricted), _state_version(restricted))
                before = {"target_rows": before_rows}
                observed_updates = 0
                real_get_records = restricted.target.get_records

                def get_records(query: Any, params: Any = None, as_dict: bool = False) -> list[Any]:
                    nonlocal observed_updates
                    rows = real_get_records(query, params, as_dict)
                    if "output n'updated'" in str(query).lower():
                        assert rows and int(rows[0]["action_count"]) == 1
                        observed_updates += 1
                    return rows

                deny_checkpoint_writes(route, principal)
                with pytest.MonkeyPatch.context() as patch:
                    patch.setattr(restricted.target, "get_records", get_records)
                    try:
                        restricted.run(_date(1))
                    except Exception as exc:  # noqa: BLE001 - vendor driver exception hierarchy varies.
                        permission_error = exc
                    else:  # pragma: no cover - must fail against the live DENY.
                        raise AssertionError("state checkpoint permission denial did not fail")

                assert "permission" in str(permission_error).lower()
                assert "denied" in str(permission_error).lower()
                assert observed_updates == 1
                assert restricted.target_rows() == before_rows
                assert (scenario.state_counts(restricted), _state_version(restricted)) == before_state
                assert scenario.staging_table_count(restricted) == 0
                route_live_recorder.observe_case(
                    "xmin_reconciliation",
                    "state_permission_denial",
                    before_image=before,
                    after_image=_business_image(restricted),
                    observations={
                        "permission_error_type": type(permission_error).__name__,
                        "permission_error": str(permission_error),
                        "observed_updates_before_rollback": observed_updates,
                        "state_before_after_equal": True,
                    },
                )


def _record_completed_matrix(completed: set[str]) -> None:
    if completed != REQUIRED_LIVE_MATRIX_CASES:
        missing = sorted(REQUIRED_LIVE_MATRIX_CASES - completed)
        unexpected = sorted(completed - REQUIRED_LIVE_MATRIX_CASES)
        raise AssertionError(f"live matrix incomplete: missing={missing}, unexpected={unexpected}")
    if not scenario.EVIDENCE_PATH.is_file():
        raise AssertionError("sequential live evidence must exist before matrix completion")

    evidence = json.loads(scenario.EVIDENCE_PATH.read_text(encoding="utf-8"))
    coverage = evidence.setdefault("coverage", {})
    prior = {str(value) for value in coverage.get("completed", [])}
    coverage["completed"] = sorted(prior | completed)
    coverage["required_not_executed"] = []
    coverage["matrix_execution"] = {
        "vendor_transactions": True,
        "connector_doubles": False,
        "completed_cases": sorted(completed),
    }
    coverage["remaining_release_gates"] = [
        "published_release_pin",
        "production_environment_baseline_and_soak",
        "production_compression_benchmark",
    ]
    coverage["supplemental_not_live_certified"] = [
        "repair_authority_identity_transfer",
    ]
    evidence["fault_matrix"].update(
        {
            "parallel_attempts": {"applock_serialized": True, "stale_cas_rolled_back": True},
            "same_snapshot_source_mutation": {"delta_and_keys_shared_mvcc_view": True},
            "state_permission_denial": {"target_before_image_restored": True},
            "post_step_faults": {
                "points": ["reactivate", "update", "insert", "soft_delete", "checkpoint"],
                "target_and_checkpoint_before_image_restored": True,
            },
            "negative_admission": {
                "empty_snapshot": "failed_closed",
                "delete_guard": "failed_closed",
                "delta_checksum": "failed_closed",
                "key_checksum": "failed_closed",
            },
        }
    )
    # The disposable matrix is complete, but it cannot certify a release pin,
    # production topology, baseline, compression choice, or seven-run soak.
    evidence["status"] = "passed_partial"
    evidence["release_ready"] = False
    scenario.EVIDENCE_PATH.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _hold_after_extract(route: Any, barrier: threading.Barrier) -> None:
    source = route.processor.source
    real_extract = source.extract

    def extract(load_config: Any, last_state: Any) -> Any:
        result = real_extract(load_config, last_state)
        barrier.wait(timeout=60)
        return result

    source.extract = extract


def _future_outcome(future: Any) -> dict[str, Any] | BaseException:
    try:
        return future.result(timeout=120)
    except BaseException as exc:  # noqa: BLE001 - the exact loser error is asserted by the caller.
        return exc


def _baseline(route: Any, execution_date: datetime) -> int:
    result = route.run(execution_date)
    return scenario.assert_run_evidence(
        route,
        result,
        execution_date,
        expected=_metrics(staging=3, inserted=3, active=3, total=3),
        previous_checkpoint=None,
    )


def _state_version(route: Any) -> tuple[int, int]:
    row = route.state.get_records(
        f"SELECT xmin_value, state_revision "
        f"FROM [{route.state_database}].[system].[dpone_source_state] "
        "WHERE superseded_at_utc IS NULL",
        as_dict=True,
    )[0]
    return int(row["xmin_value"]), int(row["state_revision"])


def _business_image(route: Any) -> dict[str, object]:
    return {"target_rows": route.target_rows()}


def _insert_source_row(
    connector: Any,
    source_schema: str,
    metric_code: str,
    metric_value: float,
    note: str | None,
) -> None:
    connector.execute_query(
        sql.SQL("INSERT INTO {}.{} (metric_code, metric_value, note, occurred_at) VALUES (%s, %s, %s, %s)").format(
            sql.Identifier(source_schema),
            sql.Identifier(SOURCE_TABLE),
        ),
        (metric_code, metric_value, note, "2026-08-16T07:00:00+00:00"),
    )


def _metrics(
    *,
    staging: int,
    active: int,
    total: int,
    inserted: int = 0,
    updated: int = 0,
    reactivated: int = 0,
    unchanged: int = 0,
    deleted: int = 0,
) -> dict[str, int]:
    return {
        "staging_rows": staging,
        "inserted_rows": inserted,
        "updated_rows": updated,
        "reactivated_rows": reactivated,
        "unchanged_rows": unchanged,
        "soft_deleted_rows": deleted,
        "active_rows": active,
        "total_rows": total,
    }


def _date(offset: int) -> datetime:
    return _BASE_DATE + timedelta(minutes=offset)


def _work_dir(root: Path, name: str) -> Path:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    return path


__all__ = ["REQUIRED_LIVE_MATRIX_CASES", "run_vendor_live_matrix"]
