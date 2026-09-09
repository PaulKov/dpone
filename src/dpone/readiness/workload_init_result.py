"""Result assembly for plan-first workload initialization."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.readiness.workload_init_catalog_merge import CatalogPatchPlan


from pathlib import Path
from typing import Any, Protocol

from dpone.readiness.airflow_self_service_models import Change


class _WorkloadInitRequestView(Protocol):
    @property
    def apply(self) -> bool: ...

    @property
    def layout(self) -> str: ...

    @property
    def workload_set(self) -> str: ...


def merge_workload_init_changes(
    scaffold_changes: tuple[Change, ...],
    domain_plan: CatalogPatchPlan,
    ownership_plan: CatalogPatchPlan,
    *,
    repo_root: Path,
) -> tuple[Change, ...]:
    """Append catalog plans to the ordered scaffold receipt."""

    return (
        *scaffold_changes,
        _catalog_change(domain_plan, repo_root=repo_root),
        _catalog_change(ownership_plan, repo_root=repo_root),
    )


def validation_summary(validation: dict[str, Any] | None) -> dict[str, Any]:
    """Render the stable public validation summary."""

    if validation is None:
        return {"status": "planned"}
    if validation.get("passed") is True:
        return {"status": "passed", "manifest": validation.get("manifest")}
    return {"status": "failed", "message": validation.get("message")}


def workload_init_details(
    *,
    request: _WorkloadInitRequestView,
    domain: str,
    workload_id: str,
    dag_id: str,
    manifest_rel: str,
    validation: dict[str, Any],
    rollback_journal: dict[str, Any] | None = None,
    unit_of_work: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Render backward-compatible workload-init result details."""

    details = {
        "mode": "apply" if request.apply else "plan",
        "layout": request.layout,
        "domain": domain,
        "workload_id": workload_id,
        "dag_id": dag_id,
        "workload_set": request.workload_set,
        "manifest": manifest_rel,
        "validation": validation,
        "next_commands": [
            f"dpone gitops airflow pack --workload-set {request.workload_set} --mode plan",
            f"dpone gitops airflow reconcile --workload-set {request.workload_set}",
        ],
    }
    if rollback_journal is not None:
        details["rollback_journal"] = rollback_journal
    if unit_of_work is not None:
        details["unit_of_work"] = unit_of_work
    return details


def _catalog_change(plan: CatalogPatchPlan, *, repo_root: Path) -> Change:
    try:
        rel_path = plan.path.relative_to(repo_root).as_posix()
    except ValueError:
        rel_path = plan.path.as_posix()
    return Change(plan.action, rel_path, plan.reason or "", plan.diff or "")


__all__ = [
    "merge_workload_init_changes",
    "validation_summary",
    "workload_init_details",
]
