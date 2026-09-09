"""Real-vendor freeze/wrap repair baselines through the standard route."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.runtime.state.xmin_storage import XMinState
from tests.integration.postgres.postgres_xmin_mssql_identity_live_authority import (
    authority_consumptions,
    checkpoint,
    clone_config,
    force_checkpoint,
    provision_authority,
    resolve_key,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_case_support import (
    atomic_before_image,
    count_exports,
    expect_error,
    run_route,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    SOURCE_TABLE,
    provision_snapshot_route,
)

_UTC = timezone.utc  # noqa: UP017 - mypy uses the supported Python 3.10 stubs.


@dataclass(frozen=True, slots=True)
class _RepairExecution:
    evidence: dict[str, Any]
    before: object
    after: object


def run_unsafe_window_repairs(
    root: Path,
    *,
    route_live_recorder: RouteLiveObservationRecorder,
) -> dict[str, Any]:
    """Prove approved freeze and injected-wrap full baselines end to end."""

    freeze = _run_freeze_repair(root / "freeze")
    wraparound = _run_wraparound_repair(root / "wraparound")
    result = {
        "status": "passed",
        "freeze": freeze.evidence,
        "wraparound": wraparound.evidence,
    }
    route_live_recorder.observe_case(
        "target_identity_authority",
        "repair_wraparound_and_freeze_full_baseline",
        before_image={"freeze": freeze.before, "wraparound": wraparound.before},
        after_image={"freeze": freeze.after, "wraparound": wraparound.after},
        observations=result,
    )
    return result


def _run_freeze_repair(root: Path) -> _RepairExecution:
    with provision_snapshot_route(root) as route:
        run_route(route, route.load_config, 20)
        key, _current = resolve_key(route, route.load_config)
        rows = route.postgres.get_records(
            "SELECT c.relfrozenxid::text::bigint AS relfrozenxid "
            "FROM pg_catalog.pg_class AS c "
            "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s",
            (route.source_schema, SOURCE_TABLE),
            as_dict=True,
        )
        frozen_checkpoint = max(3, int(rows[0]["relfrozenxid"]) - 1)
        unsafe = force_checkpoint(route, key, xmin=frozen_checkpoint)
        before = atomic_before_image(route)
        with count_exports(route) as denied_exports:
            denied = expect_error(
                lambda: run_route(route, route.load_config, 21),
                "postgres_xmin_frozen_checkpoint_full_baseline_required",
            )
        assert denied_exports[0] == 0
        assert atomic_before_image(route) == before

        authority = provision_authority(
            route,
            key=key,
            expected=unsafe,
            full_baseline=True,
        )
        repaired = run_route(
            route,
            clone_config(route, repair_authority_ref=authority.authority_id),
            22,
        )
        assert repaired["reconciliation_metrics"]["repair_full_baseline"] is True
        consumption = authority_consumptions(route, authority.authority_id)
        assert len(consumption) == 1 and bool(consumption[0]["used_full_baseline"])
        active_checkpoint = checkpoint(route, key)
        assert active_checkpoint is not None and active_checkpoint.xmin_value > frozen_checkpoint
        return _RepairExecution(
            {
                "denied_code": denied,
                "source_relation_frozen_xid": int(rows[0]["relfrozenxid"]),
                "arranged_checkpoint": frozen_checkpoint,
                "repair_full_baseline": True,
                "consumed_exactly_once": True,
            },
            before,
            atomic_before_image(route),
        )


def _run_wraparound_repair(root: Path) -> _RepairExecution:
    with provision_snapshot_route(root) as route:
        run_route(route, route.load_config, 30)
        key, previous = resolve_key(route, route.load_config)
        assert previous is not None
        manager = route.processor.source._xmin_extract.xmin_manager
        real_calculate = manager.calculate_safe_xmin

        def inject_wrap(current: int, candidate_previous: XMinState | None = None) -> XMinState:
            if candidate_previous is not None:
                return XMinState(
                    current,
                    datetime.now(_UTC),
                    wraparound_detected=True,
                    revision=candidate_previous.revision,
                )
            return real_calculate(current, candidate_previous)

        manager.calculate_safe_xmin = inject_wrap
        before = atomic_before_image(route)
        with count_exports(route) as denied_exports:
            denied = expect_error(
                lambda: run_route(route, route.load_config, 31),
                "postgres_xmin_wraparound_full_baseline_required",
            )
        assert denied_exports[0] == 0
        assert atomic_before_image(route) == before
        authority = provision_authority(
            route,
            key=key,
            expected=previous,
            full_baseline=True,
        )
        repaired = run_route(
            route,
            clone_config(route, repair_authority_ref=authority.authority_id),
            32,
        )
        manager.calculate_safe_xmin = real_calculate
        assert repaired["reconciliation_metrics"]["repair_full_baseline"] is True
        assert len(authority_consumptions(route, authority.authority_id)) == 1
        return _RepairExecution(
            {
                "denied_code": denied,
                "arrangement": "xmin_manager_wraparound_fault_injection",
                "repair_full_baseline": True,
                "consumed_exactly_once": True,
            },
            before,
            atomic_before_image(route),
        )


__all__ = ["run_unsafe_window_repairs"]
