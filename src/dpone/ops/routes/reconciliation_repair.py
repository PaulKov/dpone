"""Route-level reconciliation repair evidence aggregation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.ops.reconciliation import ReconciliationReport, ReconciliationService
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.models import (
    RouteKey,
    RouteReconciliationRepairReport,
    RouteRepairAction,
    RouteRepairPlan,
)


class RouteReconciliationRepairService:
    """Build a route-level repair receipt from reconciliation evidence."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        reconciliation_service: ReconciliationService | None = None,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._reconciliation_service = reconciliation_service or ReconciliationService()

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        source_rows: Sequence[Mapping[str, Any]],
        target_rows: Sequence[Mapping[str, Any]],
        key_columns: Sequence[str],
        compare_columns: Sequence[str] | None = None,
        delete_column: str | None = None,
        source_boundary: str = "full-snapshot",
        target_boundary: str = "full-snapshot",
    ) -> RouteReconciliationRepairReport:
        directory = Path(output_dir)
        key = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(key)
        reconciliation = self._reconciliation_service.reconcile(
            output_dir=directory / "reconciliation",
            source_rows=source_rows,
            target_rows=target_rows,
            key_columns=key_columns,
            compare_columns=compare_columns,
            delete_column=delete_column,
        )
        repair_plan = RouteRepairPlan(
            source_boundary=source_boundary,
            target_boundary=target_boundary,
            actions=_repair_actions(reconciliation),
        )
        blockers = _blockers(key=key, profile_exists=profile is not None, repair_plan=repair_plan)
        report = RouteReconciliationRepairReport(
            route=key,
            profile=profile,
            passed=not blockers,
            level="ready" if not blockers else ("unknown" if profile is None else "blocked"),
            blockers=blockers,
            warnings=tuple(),
            next_actions=_next_actions(blockers),
            repair_plan=repair_plan,
            reconciliation=reconciliation.to_dict(),
            output_dir=str(directory),
            json_path=str(directory / "route_reconciliation_repair.json"),
            markdown_path=str(directory / "route_reconciliation_repair.md"),
        )
        report.write()
        return report


def _repair_actions(report: ReconciliationReport) -> tuple[RouteRepairAction, ...]:
    actions: list[RouteRepairAction] = []
    for item in report.repair_actions:
        if item.action in {"insert_target_row", "update_target_row"}:
            actions.append(
                RouteRepairAction(
                    action="replay_source_row",
                    key=item.key,
                    reason=item.reason,
                    source_action=item.action,
                )
            )
        elif item.action == "delete_target_row":
            actions.append(
                RouteRepairAction(
                    action="delete_target_row",
                    key=item.key,
                    reason=item.reason,
                    source_action=item.action,
                )
            )
    return tuple(actions)


def _blockers(
    *,
    key: RouteKey,
    profile_exists: bool,
    repair_plan: RouteRepairPlan,
) -> tuple[str, ...]:
    blockers: list[str] = []
    if not profile_exists:
        blockers.append(f"route.unsupported:{key.colon_id}")
    if repair_plan.actions:
        blockers.append("route_reconciliation_repair.unrepaired_differences")
    return tuple(blockers)


def _next_actions(blockers: tuple[str, ...]) -> tuple[str, ...]:
    actions: list[str] = []
    for blocker in blockers:
        if blocker.startswith("route.unsupported"):
            actions.append("Add the route to the integration matrix before evaluating reconciliation repair.")
        elif blocker == "route_reconciliation_repair.unrepaired_differences":
            actions.append("Execute the repair plan, then rerun route reconciliation repair evidence.")
        else:
            actions.append(f"Resolve `{blocker}` before route state promotion.")
    return tuple(dict.fromkeys(actions))


__all__ = ["RouteReconciliationRepairService"]
