"""Real vendor negative and transactional fault scenarios for XMin -> MSSQL."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from psycopg import sql
from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.runtime.artifact_integrity import ArtifactIntegrityError
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from tests.integration.postgres import postgres_xmin_mssql_snapshot_live_scenario as scenario
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    SOURCE_TABLE,
    provision_snapshot_route,
)

_BASE_DATE = datetime(2026, 8, 17, 6, 30)
_FAULT_MARKERS = {
    "reactivate": "output n'reactivated'",
    "update": "output n'updated'",
    "insert": "output n'inserted'",
    "soft_delete": "output n'soft_deleted'",
    "checkpoint": "dpone_xmin_checkpoint_cas_mismatch",
}


@dataclass(frozen=True, slots=True)
class _RollbackObservation:
    name: str
    before: dict[str, object]
    after: dict[str, object]
    facts: dict[str, object]


def run_failure_matrix(
    root: Path,
    *,
    route_live_recorder: RouteLiveObservationRecorder,
) -> set[str]:
    """Execute fail-closed guards, receipt integrity, and every DML fault."""

    negative_observations = [
        _empty_snapshot_fails_closed(_work_dir(root, "empty")),
        _delete_guard_fails_closed(_work_dir(root, "guard")),
    ]
    for artifact_kind in ("delta", "keys"):
        negative_observations.append(
            _checksum_fails_closed(
                _work_dir(root, f"checksum-{artifact_kind}"),
                artifact_kind,
            )
        )
    _record_rollback_group(
        route_live_recorder,
        "empty_guard_and_checksum_negative_cases",
        negative_observations,
    )
    fault_observations: list[_RollbackObservation] = []
    for fault_point, marker in _FAULT_MARKERS.items():
        fault_observations.append(
            _transaction_fault_rolls_back(
                _work_dir(root, f"fault-{fault_point}"),
                fault_point=fault_point,
                marker=marker,
            )
        )
    _record_rollback_group(
        route_live_recorder,
        "per_dml_and_checkpoint_fault_injection",
        fault_observations,
    )
    return {
        "empty_guard_and_checksum_negative_cases",
        "per_dml_and_checkpoint_fault_injection",
    }


def _empty_snapshot_fails_closed(work_dir: Path) -> _RollbackObservation:
    with provision_snapshot_route(work_dir) as route:
        _baseline(route)
        before_rows = route.target_rows()
        before_state = _state_before_image(route)
        before = _failure_image(route)
        route.postgres.execute_query(sql.SQL("DELETE FROM {}").format(_source_table(route)))

        with pytest.raises(SnapshotReconciliationError, match="repair_authority.required"):
            route.run(_date(1))

        _assert_common_before_image(route, before_rows, before_state)
        return _RollbackObservation(
            "empty_snapshot",
            before,
            _failure_image(route),
            {"blocker": "repair_authority.required"},
        )


def _delete_guard_fails_closed(work_dir: Path) -> _RollbackObservation:
    with provision_snapshot_route(work_dir) as route:
        _baseline(route)
        options = copy.deepcopy(route.load_config.options)
        options["reconciliation"]["guards"] = {
            "max_delete_ratio": 0.0,
            "max_delete_rows": 0,
        }
        route.load_config.options = options
        before_rows = route.target_rows()
        before_state = _state_before_image(route)
        before = _failure_image(route)
        _delete_key(route, "empty")

        with pytest.raises(SnapshotReconciliationError, match="repair_authority.required"):
            route.run(_date(1))

        _assert_common_before_image(route, before_rows, before_state)
        return _RollbackObservation(
            "delete_guard",
            before,
            _failure_image(route),
            {"blocker": "repair_authority.required"},
        )


def _checksum_fails_closed(
    work_dir: Path,
    artifact_kind: str,
) -> _RollbackObservation:
    with provision_snapshot_route(work_dir) as route:
        _baseline(route)
        scenario.update_special(route, metric_value=8.25, note=f"corrupt-{artifact_kind}")
        before_rows = route.target_rows()
        before_state = _state_before_image(route)
        before = _failure_image(route)
        source = route.processor.source
        real_extract = source.extract

        def extract(load_config: Any, last_state: Any) -> Any:
            result = real_extract(load_config, last_state)
            envelope = result.snapshot_envelope
            assert envelope is not None
            artifact = envelope.delta_artifact if artifact_kind == "delta" else envelope.key_artifact
            with Path(artifact.file_path).open("ab") as handle:
                handle.write(b"vendor-live-corruption\n")
            return result

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(source, "extract", extract)
            with pytest.raises(ArtifactIntegrityError, match="artifact_integrity.byte_count_mismatch"):
                route.run(_date(1))

        _assert_common_before_image(route, before_rows, before_state)
        return _RollbackObservation(
            f"checksum_{artifact_kind}",
            before,
            _failure_image(route),
            {"artifact_kind": artifact_kind, "typed_snapshot_reject": True},
        )


def _transaction_fault_rolls_back(
    work_dir: Path,
    *,
    fault_point: str,
    marker: str,
) -> _RollbackObservation:
    with provision_snapshot_route(work_dir) as route:
        first_checkpoint = _baseline(route)
        _delete_key(route, "empty")
        tombstone = route.run(_date(1))
        second_checkpoint = scenario.assert_run_evidence(
            route,
            tombstone,
            _date(1),
            expected=_metrics(staging=0, unchanged=2, deleted=1, active=2, total=3),
            previous_checkpoint=first_checkpoint,
        )

        scenario.reactivate(route)
        scenario.update_special(route, metric_value=11.5, note=f"fault-{fault_point}")
        _insert_route_row(route, "new-matrix", -1.25, "new", "2026-08-17T08:00:00+00:00")
        _delete_key(route, "nullable")
        before_rows = route.target_rows()
        before_state = _state_before_image(route)
        before = _failure_image(route)
        observed = {"count": 0}
        real_get_records = route.target.get_records

        def get_records(query: Any, params: Any = None, as_dict: bool = False) -> list[Any]:
            rows = real_get_records(query, params, as_dict)
            if marker not in str(query).lower():
                return rows
            if fault_point != "checkpoint":
                assert rows and int(rows[0]["action_count"]) > 0
            observed["count"] += 1
            raise RuntimeError(f"vendor_live_fault_after_{fault_point}")

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(route.target, "get_records", get_records)
            with pytest.raises(RuntimeError, match=f"vendor_live_fault_after_{fault_point}"):
                route.run(_date(2))

        assert observed["count"] == 1
        _assert_common_before_image(route, before_rows, before_state)
        after = _failure_image(route)

        recovered = route.run(_date(3))
        scenario.assert_run_evidence(
            route,
            recovered,
            _date(3),
            expected={
                "staging_rows": 3,
                "inserted_rows": 1,
                "updated_rows": 1,
                "reactivated_rows": 1,
                "unchanged_rows": 0,
                "soft_deleted_rows": 1,
                "active_rows": 3,
                "total_rows": 4,
            },
            previous_checkpoint=second_checkpoint,
        )
        return _RollbackObservation(
            fault_point,
            before,
            after,
            {
                "fault_point": fault_point,
                "fault_marker": marker,
                "fault_observed_count": observed["count"],
                "recovery_status": recovered["status"],
            },
        )


def _baseline(route: Any) -> int:
    result = route.run(_date(0))
    return scenario.assert_run_evidence(
        route,
        result,
        _date(0),
        expected=_metrics(staging=3, inserted=3, active=3, total=3),
        previous_checkpoint=None,
    )


def _state_before_image(route: Any) -> tuple[dict[str, int], tuple[int, int]]:
    row = route.state.get_records(
        f"SELECT xmin_value, state_revision "
        f"FROM [{route.state_database}].[system].[dpone_source_state] "
        "WHERE superseded_at_utc IS NULL",
        as_dict=True,
    )[0]
    return scenario.state_counts(route), (int(row["xmin_value"]), int(row["state_revision"]))


def _assert_common_before_image(
    route: Any,
    target_rows: list[dict[str, Any]],
    state: tuple[dict[str, int], tuple[int, int]],
) -> None:
    assert route.target_rows() == target_rows
    assert _state_before_image(route) == state
    assert scenario.staging_table_count(route) == 0


def _failure_image(route: Any) -> dict[str, object]:
    return {
        "target_rows": route.target_rows(),
        "state": _state_before_image(route),
        "staging_objects": scenario.staging_table_count(route),
    }


def _record_rollback_group(
    recorder: RouteLiveObservationRecorder,
    case_id: str,
    observations: list[_RollbackObservation],
) -> None:
    assert observations
    assert all(observation.before == observation.after for observation in observations)
    recorder.observe_case(
        "xmin_reconciliation",
        case_id,
        before_image={
            "scenarios": [{"name": observation.name, "image": observation.before} for observation in observations]
        },
        after_image={
            "scenarios": [{"name": observation.name, "image": observation.after} for observation in observations]
        },
        observations={
            "all_vendor_faults_executed": True,
            "scenario_facts": [observation.facts for observation in observations],
        },
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


def _insert_route_row(
    route: Any,
    metric_code: str,
    metric_value: float,
    note: str | None,
    occurred_at: str,
) -> None:
    route.postgres.execute_query(
        sql.SQL("INSERT INTO {} (metric_code, metric_value, note, occurred_at) VALUES (%s, %s, %s, %s)").format(
            _source_table(route)
        ),
        (metric_code, metric_value, note, occurred_at),
    )


def _delete_key(route: Any, metric_code: str) -> None:
    route.postgres.execute_query(
        sql.SQL("DELETE FROM {} WHERE metric_code = %s").format(_source_table(route)),
        (metric_code,),
    )


def _source_table(route: Any) -> Any:
    return sql.SQL("{}.{}").format(
        sql.Identifier(route.source_schema),
        sql.Identifier(SOURCE_TABLE),
    )


def _date(offset: int) -> datetime:
    return _BASE_DATE + timedelta(minutes=offset)


def _work_dir(root: Path, name: str) -> Path:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    return path


__all__ = ["run_failure_matrix"]
