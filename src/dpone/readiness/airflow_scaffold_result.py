"""Canonical result mapping for scaffold plans and apply receipts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from dpone.readiness.airflow_self_service_models import Change, SelfServiceResult, dpone_error
from dpone.readiness.error_contract import error_docs_url, manual_fix


class ScaffoldResultPlan(Protocol):
    """Read-only scaffold receipt surface consumed by result mapping."""

    @property
    def changes(self) -> tuple[Change, ...]: ...

    @property
    def rollback_journal(self) -> dict[str, Any]: ...

    @property
    def rollback_issues(self) -> tuple[str, ...]: ...

    @property
    def recovery_artifacts(self) -> tuple[str, ...]: ...

    @property
    def apply_failed(self) -> bool: ...

    @property
    def has_conflict(self) -> bool: ...

    @property
    def recovery_required(self) -> bool: ...


def scaffold_result(
    plan: ScaffoldResultPlan,
    *,
    stage: str,
    details: Mapping[str, Any],
) -> SelfServiceResult:
    """Map ordinary plans and apply-time safety failures without losing receipts."""

    receipt_details = {
        **details,
        "rollback_journal": plan.rollback_journal,
    }
    if not plan.apply_failed and not plan.recovery_required:
        return SelfServiceResult(
            passed=not plan.has_conflict,
            changes=plan.changes,
            details=receipt_details,
        )
    return scaffold_apply_failure_result(
        plan,
        stage=stage,
        details=receipt_details,
    )


def scaffold_apply_failure_result(
    plan: ScaffoldResultPlan,
    *,
    stage: str,
    details: Mapping[str, Any] | None = None,
) -> SelfServiceResult:
    """Return the stable safety error for one failed scaffold transaction."""

    recovery_required = plan.recovery_required
    return SelfServiceResult(
        passed=False,
        changes=plan.changes,
        errors=(
            dpone_error(
                "DPONE_SCAFFOLD_APPLY_FAILED",
                (
                    "Scaffold write failed and manual recovery is required."
                    if recovery_required
                    else "Scaffold write failed; operation-owned files were rolled back."
                ),
                stage=stage,
                fixes=[manual_fix("inspect_scaffold_recovery")],
                docs_url=error_docs_url("DPONE_SCAFFOLD_APPLY_FAILED"),
                extra={"recovery_required": recovery_required},
            ),
        ),
        details={
            **(details or {}),
            "recovery_required": recovery_required,
            "rollback_journal": plan.rollback_journal,
            "rollback_issues": list(plan.rollback_issues),
            "recovery_artifacts": list(plan.recovery_artifacts),
        },
        exit_code=4,
    )


__all__ = ["scaffold_apply_failure_result", "scaffold_result"]
