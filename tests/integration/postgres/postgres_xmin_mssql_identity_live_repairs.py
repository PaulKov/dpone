"""Real-vendor repair/authority cases executed through the standard route."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

from psycopg import sql
from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.contracts.repair_authority import TargetAuthorityTransfer
from tests.integration.postgres.postgres_xmin_mssql_identity_live_authority import (
    authority_consumptions,
    clone_config,
    hex_digest,
    ownership_ledger,
    provision_authority,
    resolve_key,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_case_support import (
    active_owner as _active_owner,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_case_support import (
    atomic_before_image as _atomic_before_image,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_case_support import (
    count_exports as _count_exports,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_case_support import (
    expect_error as _expect_error,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_case_support import (
    ledger_row as _ledger_row,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_case_support import (
    run_route as _run,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_case_support import (
    staging_count as _staging_count,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_case_support import (
    state_exists as _state_exists,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_case_support import (
    target_row as _row,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    SOURCE_TABLE,
    SPECIAL_KEY,
    provision_snapshot_route,
)


def run_config_coordinate_drift(
    root: Path,
    *,
    route_live_recorder: RouteLiveObservationRecorder,
) -> dict[str, Any]:
    """Prove post-extract coordinate drift fails before any target staging."""

    with provision_snapshot_route(root / "coordinate-drift") as route:
        before = _atomic_before_image(route)
        real_load = route.processor.sink.load

        def drift_then_load(runtime_config: Any, payload: Any) -> Any:
            runtime_config.target_table = "coordinate_drift_decoy"
            return real_load(runtime_config, payload)

        route.processor.sink.load = drift_then_load
        error = _expect_error(lambda: _run(route, route.load_config, 0), "target_coordinates_changed")
        route.processor.sink.load = real_load
        assert _atomic_before_image(route) == before
        assert _staging_count(route) == 0

        recovered = _run(route, route.load_config, 1)
        assert recovered["status"] == "success"
        assert int(recovered["inserted_rows"]) == 3
        result = {
            "status": "passed",
            "failure_code": error,
            "staging_rows_before_failure": 0,
            "state_or_target_mutated": False,
            "recovery_via_standard_route": True,
        }
        route_live_recorder.observe_case(
            "target_identity_authority",
            "target_config_coordinate_drift",
            before_image=before,
            after_image=_atomic_before_image(route),
            observations={**result, "recovery_result": dict(recovered)},
        )
        return result


def run_empty_snapshot_repairs(
    root: Path,
    *,
    route_live_recorder: RouteLiveObservationRecorder,
) -> dict[str, Any]:
    """Prove all-delete bounds, invalid approvals, consumption and reuse."""

    with provision_snapshot_route(root / "empty-all-delete") as route:
        baseline = _run(route, route.load_config, 10)
        assert int(baseline["inserted_rows"]) == 3
        key, previous = resolve_key(route, route.load_config)
        assert previous is not None
        route.postgres.execute_query(
            sql.SQL("DELETE FROM {}.{}").format(
                sql.Identifier(route.source_schema),
                sql.Identifier(SOURCE_TABLE),
            )
        )
        before = _atomic_before_image(route)

        insufficient = provision_authority(
            route,
            key=key,
            expected=previous,
            full_baseline=False,
            max_delete_rows=2,
            max_delete_ratio=1.0,
        )
        insufficient_config = clone_config(route, repair_authority_ref=insufficient.authority_id)
        error_insufficient = _expect_error(
            lambda: _run(route, insufficient_config, 11),
            "empty_snapshot_delete_bounds_denied",
        )
        assert _atomic_before_image(route) == before
        assert authority_consumptions(route, insufficient.authority_id) == []

        expired = provision_authority(
            route,
            key=key,
            expected=previous,
            full_baseline=False,
            max_delete_rows=3,
            max_delete_ratio=1.0,
            expires_delta=timedelta(seconds=-1),
        )
        with _count_exports(route) as expired_exports:
            error_expired = _expect_error(
                lambda: _run(route, clone_config(route, repair_authority_ref=expired.authority_id), 12),
                "repair_authority.expired",
            )
        assert expired_exports[0] == 0
        assert _atomic_before_image(route) == before

        foreign_config = clone_config(route, process="integration.foreign-repair-owner")
        foreign_key, foreign_checkpoint = resolve_key(route, foreign_config)
        assert foreign_checkpoint is None and foreign_key.digest != key.digest
        mismatch = provision_authority(
            route,
            key=foreign_key,
            expected=None,
            full_baseline=True,
        )
        with _count_exports(route) as mismatch_exports:
            error_mismatch = _expect_error(
                lambda: _run(route, clone_config(route, repair_authority_ref=mismatch.authority_id), 13),
                "repair_authority.state_key_mismatch",
            )
        assert mismatch_exports[0] == 0
        assert _atomic_before_image(route) == before

        approved = provision_authority(
            route,
            key=key,
            expected=previous,
            full_baseline=False,
            max_delete_rows=3,
            max_delete_ratio=1.0,
        )
        approved_config = clone_config(route, repair_authority_ref=approved.authority_id)
        repaired = _run(route, approved_config, 14)
        assert int(repaired["soft_deleted_rows"]) == 3
        assert int(repaired["active_rows"]) == 0
        assert int(repaired["total_rows"]) == 3
        tombstones = route.target_rows()
        assert len(tombstones) == 3
        assert all(row["__dpone__deleted_at"] is not None for row in tombstones)
        consumption = authority_consumptions(route, approved.authority_id)
        assert len(consumption) == 1
        assert int(consumption[0]["observed_delete_rows"]) == 3
        assert float(consumption[0]["observed_delete_ratio"]) == 1.0
        deleted_at = tuple(row["__dpone__deleted_at"] for row in tombstones)
        after_repair = _atomic_before_image(route)

        with _count_exports(route) as reuse_exports:
            error_reuse = _expect_error(
                lambda: _run(route, approved_config, 15),
                "repair_authority.already_consumed",
            )
        assert reuse_exports[0] == 0
        assert _atomic_before_image(route) == after_repair
        assert tuple(row["__dpone__deleted_at"] for row in route.target_rows()) == deleted_at
        assert len(authority_consumptions(route, approved.authority_id)) == 1
        result = {
            "status": "passed",
            "invalid_codes": [error_insufficient, error_expired, error_mismatch],
            "approved_rows": 3,
            "approved_ratio": 1.0,
            "tombstones": 3,
            "consumed_exactly_once": True,
            "reuse_code": error_reuse,
            "invalid_preview_export_count": expired_exports[0] + mismatch_exports[0],
        }
        route_live_recorder.observe_case(
            "target_identity_authority",
            "repair_empty_snapshot_all_delete",
            before_image=before,
            after_image=after_repair,
            observations=result,
        )
        return result


def run_target_authority_transfer(
    root: Path,
    *,
    route_live_recorder: RouteLiveObservationRecorder,
) -> dict[str, Any]:
    """Prove exact transfer, mismatch, transactional rollback, reuse and stale owner."""

    with provision_snapshot_route(root / "authority-transfer") as route:
        _run(route, route.load_config, 40)
        old_key, old_checkpoint = resolve_key(route, route.load_config)
        assert old_checkpoint is not None
        route.postgres.execute_query(
            sql.SQL("UPDATE {}.{} SET metric_value = %s, note = %s WHERE metric_code = %s").format(
                sql.Identifier(route.source_schema),
                sql.Identifier(SOURCE_TABLE),
            ),
            (77.25, "identity transfer payload", SPECIAL_KEY),
        )
        new_config = clone_config(route, process="integration.postgres_xmin_mssql_snapshot.v2")
        new_key, new_checkpoint = resolve_key(route, new_config)
        assert new_checkpoint is None
        assert new_key.digest != old_key.digest
        assert new_key.target_identity == old_key.target_identity
        before = _atomic_before_image(route)

        mismatch = provision_authority(
            route,
            key=new_key,
            expected=None,
            full_baseline=True,
            transfer_from=TargetAuthorityTransfer(
                old_key.digest,
                old_checkpoint.xmin_value,
                old_checkpoint.revision + 1,
            ),
        )
        mismatch_error = _expect_error(
            lambda: _run(
                route,
                clone_config(
                    route,
                    process="integration.postgres_xmin_mssql_snapshot.v2",
                    repair_authority_ref=mismatch.authority_id,
                ),
                41,
            ),
            "DPONE_XMIN_TARGET_AUTHORITY_TRANSFER_BINDING_MISMATCH",
        )
        assert _atomic_before_image(route) == before
        assert authority_consumptions(route, mismatch.authority_id) == []

        authority = provision_authority(
            route,
            key=new_key,
            expected=None,
            full_baseline=True,
            transfer_from=TargetAuthorityTransfer(
                old_key.digest,
                old_checkpoint.xmin_value,
                old_checkpoint.revision,
            ),
        )
        approved_config = clone_config(
            route,
            process="integration.postgres_xmin_mssql_snapshot.v2",
            repair_authority_ref=authority.authority_id,
        )
        storage = route.processor.source.state_storage
        real_consume = storage.consume_repair_authority

        def consume_then_fail(**kwargs: Any) -> None:
            real_consume(**kwargs)
            raise RuntimeError("injected_after_authority_consumption_before_commit")

        storage.consume_repair_authority = consume_then_fail
        rollback_error = _expect_error(
            lambda: _run(route, approved_config, 42),
            "injected_after_authority_consumption_before_commit",
        )
        storage.consume_repair_authority = real_consume
        assert _atomic_before_image(route) == before
        assert authority_consumptions(route, authority.authority_id) == []
        assert _active_owner(route, old_key.digest)
        assert not _state_exists(route, new_key.digest)

        transferred = _run(route, approved_config, 43)
        assert transferred["reconciliation_metrics"]["repair_target_authority_transfer"] is True
        assert float(_row(route, SPECIAL_KEY)["metric_value"]) == 77.25
        assert len(authority_consumptions(route, authority.authority_id)) == 1
        ledger = ownership_ledger(route)
        old = _ledger_row(ledger, old_key.digest)
        new = _ledger_row(ledger, new_key.digest)
        assert old["superseded_at_utc"] is not None
        assert bytes(old["superseded_by_state_key"]) == new_key.digest
        assert new["superseded_at_utc"] is None

        after_transfer = _atomic_before_image(route)
        with _count_exports(route) as reuse_exports:
            reuse_error = _expect_error(
                lambda: _run(route, approved_config, 44),
                "repair_authority.already_consumed",
            )
        assert reuse_exports[0] == 0
        assert _atomic_before_image(route) == after_transfer

        with _count_exports(route) as stale_exports:
            stale_error = _expect_error(
                lambda: _run(route, route.load_config, 45),
                "postgres_xmin_state_missing_for_existing_target_full_baseline_required",
            )
        assert stale_exports[0] == 0
        assert _atomic_before_image(route) == after_transfer
        result = {
            "status": "passed",
            "old_state_key": hex_digest(old_key.digest),
            "new_state_key": hex_digest(new_key.digest),
            "target_identity": hex_digest(new_key.target_identity),
            "mismatch_code": mismatch_error,
            "rollback_code": rollback_error,
            "rollback_restored_old_owner": True,
            "exact_transfer_committed": True,
            "old_history_and_receipt_retained": True,
            "reuse_code": reuse_error,
            "stale_old_process_code": stale_error,
            "consumed_exactly_once": True,
        }
        route_live_recorder.observe_case(
            "target_identity_authority",
            "repair_target_authority_transfer",
            before_image=before,
            after_image=after_transfer,
            observations=result,
        )
        return result


__all__ = [
    "run_config_coordinate_drift",
    "run_empty_snapshot_repairs",
    "run_target_authority_transfer",
]
