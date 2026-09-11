"""Pure current-pointer assessment and deployment cache recovery decisions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_deployment import (
    CurrentPointerViolation,
    current_pointer_violation,
    is_canonical_sha256_digest,
)
from dpone.runtime.deployment_cache_recovery_models import (
    DeploymentCacheRecoveryCandidate,
    DeploymentCacheRecoveryIssue,
    DeploymentCacheRecoveryPlan,
)


@dataclass(frozen=True, slots=True)
class PointerAssessment:
    """Report a contract violation and independently usable diagnostic IDs.

    Canonical IDs can survive invalid authorization. They do not establish a
    healthy pointer, a verified projection or a safe recovery candidate.
    """

    violation: CurrentPointerViolation | None
    deployment_id: str | None
    release_id: str | None


def assess_pointer(pointer: Mapping[str, Any] | None, *, expected_environment: str | None = None) -> PointerAssessment:
    """Assess an observed pointer without replacing file-level absence checks.

    An explicit environment restricts diagnostic IDs to that environment.
    Otherwise IDs retain the pointer's own raw environment, including when
    another contract violation makes the pointer unsuitable for admission.
    """

    if pointer is None:
        return PointerAssessment(None, None, None)
    violation = current_pointer_violation(pointer, expected_environment=expected_environment)
    environment: Any = pointer.get("environment") if expected_environment is None else expected_environment
    deployment_id, release_id = pointer_identity(pointer, environment=environment)
    return PointerAssessment(violation, deployment_id, release_id)


def assess_current_identity(
    pointer: Mapping[str, Any],
    current_metadata: Mapping[str, Any],
    *,
    physical_deployment_id: str | None,
    expected_environment: str | None = None,
) -> str | None:
    """Compare pointer, metadata and observed path identities for strict reads.

    The caller remains responsible for no-follow reads, complete projection
    validation and recovery errors. This comparison performs no observation.
    """

    pointer_id = str(pointer.get("deployment_id") or "")
    current_id = str(current_metadata.get("deployment_id") or "")
    pointer_environment = str(pointer.get("environment") or "")
    current_environment = str(current_metadata.get("environment") or "")
    pointer_release_id = str(pointer.get("release_id") or "")
    current_release_id = str(current_metadata.get("release_ref") or "")
    assessment = assess_pointer(pointer, expected_environment=expected_environment)
    if (
        assessment.violation is not None
        or not pointer_environment
        or pointer_environment != current_environment
        or expected_environment is not None
        and pointer_environment != expected_environment
        or not pointer_id
        or physical_deployment_id is None
        or physical_deployment_id != current_id
        or pointer_id != current_id
        or pointer_release_id != current_release_id
    ):
        return None
    return current_id


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
    "PointerAssessment",
    "assess_current_identity",
    "assess_pointer",
    "has_unidentifiable_current",
    "is_audit_only_repair",
    "pointer_identity",
    "preferred_repair_id",
    "recovery_status",
]
