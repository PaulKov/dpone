"""Pure decision rules for deployment cache recovery planning and apply."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.runtime.deployment_cache_recovery_models import (
    DeploymentCacheRecoveryCandidate,
    DeploymentCacheRecoveryIssue,
    DeploymentCacheRecoveryPlan,
)


def pointer_identity(pointer: Mapping[str, Any] | None, *, environment: str) -> tuple[str | None, str | None]:
    """Return deployment/release identities only for the requested environment."""

    if pointer is None or pointer.get("environment") != environment:
        return None, None
    deployment_id = _canonical_identity(pointer.get("deployment_id"))
    release_id = _canonical_identity(pointer.get("release_id"))
    return deployment_id, release_id


def _canonical_identity(value: object) -> str | None:
    return str(value) if is_canonical_sha256_digest(value) else None


def preferred_repair_id(
    candidates: Sequence[DeploymentCacheRecoveryCandidate],
    *,
    preferred_deployment_ids: tuple[str | None, ...],
) -> str | None:
    """Choose one deterministic candidate without guessing between alternatives."""

    candidate_ids = {candidate.deployment_id for candidate in candidates}
    for deployment_id in preferred_deployment_ids:
        if deployment_id in candidate_ids:
            return deployment_id
    return candidates[0].deployment_id if len(candidates) == 1 else None


def recovery_status(
    issues: Sequence[DeploymentCacheRecoveryIssue],
    *,
    preferred_deployment_id: str | None,
    current_identity_unavailable: bool,
) -> str:
    """Classify a plan as healthy, repairable, or blocked."""

    if not any(issue.severity == "error" for issue in issues):
        return "ok"
    if current_identity_unavailable or preferred_deployment_id is None:
        return "blocked"
    return "repairable"


def is_audit_only_repair(plan: DeploymentCacheRecoveryPlan) -> bool:
    """Return whether only the append-only audit trail requires repair."""

    blocking_codes = {issue.code for issue in plan.issues if issue.severity == "error"}
    return bool(blocking_codes) and blocking_codes <= {
        "DPONE_CURRENT_POINTER_AUDIT_MISSING",
        "DPONE_CURRENT_POINTER_AUDIT_MISMATCH",
        "DPONE_CURRENT_POINTER_AUDIT_UNREADABLE",
    }


def has_unidentifiable_current(plan: DeploymentCacheRecoveryPlan) -> bool:
    """Return whether automatic recovery lacks a safe stale-plan CAS identity."""

    return any(issue.code == "DPONE_CURRENT_PATH_ID_UNAVAILABLE" for issue in plan.issues)


__all__ = [
    "has_unidentifiable_current",
    "is_audit_only_repair",
    "pointer_identity",
    "preferred_repair_id",
    "recovery_status",
]
