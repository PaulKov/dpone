"""Fresh validation for one reviewed deployment-cache deletion candidate."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.runtime.deployment_cache_common import DeploymentCacheError
from dpone.runtime.deployment_cache_current_state import DeploymentCacheCurrentState
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
from dpone.runtime.deployment_cache_retention_contracts import (
    DeploymentCacheRetentionApplyError,
    DeploymentRetentionPlan,
    DeploymentRetentionPlanItem,
)
from dpone.runtime.deployment_cache_retention_deletion import DirectoryIdentity, directory_identity


class DeploymentCacheRetentionCandidateValidator:
    """Revalidate path, projection, identity and current authority before detach."""

    def __init__(self, cache_root: Path, *, validator: DeploymentCacheProjectionValidator) -> None:
        self._root = cache_root
        self._validator = validator

    def validate(
        self,
        item: DeploymentRetentionPlanItem,
        *,
        environment: str,
    ) -> tuple[Path, DirectoryIdentity]:
        if item.deployment_id is None:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_ID_INVALID",
                "deployment cache GC candidate has no canonical deployment identity",
                path=item.path,
            )
        deployment_root = (self._root / "deployments" / environment).resolve(strict=False)
        unresolved_path = Path(item.path)
        if unresolved_path.is_symlink():
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_PATH_ESCAPE",
                "deployment cache GC candidate must not be a symlink",
                path=item.path,
            )
        path = unresolved_path.resolve(strict=False)
        try:
            path.relative_to(deployment_root)
        except ValueError as exc:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_PATH_ESCAPE",
                "deployment cache GC candidate escapes deployment root",
                path=item.path,
            ) from exc
        initial_identity = directory_identity(path)
        try:
            projection = self._validator.validate_details(path, environment=environment)
        except DeploymentCacheError as exc:
            raise DeploymentCacheRetentionApplyError(exc.code, str(exc), path=exc.path) from exc
        if projection.deployment_id != item.deployment_id:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_ID_MISMATCH",
                "deployment cache GC candidate id changed before deletion",
                path=item.path,
            )
        if directory_identity(path) != initial_identity:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_PATH_CHANGED",
                "deployment cache GC candidate changed during validation",
                path=item.path,
            )
        try:
            current_deployment_id = DeploymentCacheCurrentState(self._root).active_deployment_id(
                expected_environment=environment
            )
        except DeploymentCacheError as exc:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED",
                "deployment cache control state changed or is inconsistent; recover it before GC",
                path=exc.path,
            ) from exc
        if current_deployment_id == item.deployment_id:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_CURRENT_CHANGED",
                "deployment cache GC candidate became current before deletion",
                path=(self._root / "current-pointer.json").as_posix(),
            )
        return path, initial_identity

    def canonical_path(self, deployment_id: str, *, environment: str) -> Path:
        """Return the exact managed occurrence path for one deployment identity."""

        deployment_root = (self._root / "deployments" / environment).resolve(strict=False)
        return deployment_root / deployment_id.replace(":", "-", 1)


def candidate_validation_failure(
    item: DeploymentRetentionPlanItem,
    *,
    cause_code: str,
    path: str | None,
) -> DeploymentCacheRetentionApplyError:
    """Describe a fail-before-delete candidate validation error."""

    failed_path = path or item.path
    return DeploymentCacheRetentionApplyError(
        "DPONE_DEPLOYMENT_CACHE_GC_VALIDATION_FAILED",
        "deployment cache GC could not validate every reviewed deletion",
        path=failed_path,
        details={
            "failed_step": "validate_candidates",
            "state_may_have_changed": False,
            "deleted_deployment_ids": [],
            "deleted_count": 0,
            "failed_deployment_id": item.deployment_id,
            "failed_paths": [failed_path],
            "over_budget": None,
            "capacity_state": "unavailable_no_budget_policy",
            "retention_incomplete": True,
            "cause_code": cause_code,
        },
    )


def require_pending_candidates_remain_deletable(
    receipt: Mapping[str, Any],
    *,
    fresh_plan: DeploymentRetentionPlan,
    committed_deployment_ids: Sequence[str],
) -> None:
    """Block replay when a not-yet-committed candidate gained protection."""

    committed = frozenset(committed_deployment_ids)
    pending = {
        str(item["deployment_id"])
        for item in receipt["items"]
        if item["action"] == "pending" and item["deployment_id"] not in committed
    }
    fresh_candidates = set(fresh_plan.delete_candidates).difference(committed)
    no_longer_deletable = sorted(pending.difference(fresh_candidates))
    unexpected_candidates = sorted(fresh_candidates.difference(pending))
    protected_changed = (
        "reviewed_protected_deployment_ids" in receipt
        and tuple(receipt["reviewed_protected_deployment_ids"]) != fresh_plan.protected_deployment_ids
    )
    recovery_changed = (
        "reviewed_recovery_revision" in receipt
        and receipt["reviewed_recovery_revision"] != fresh_plan.recovery_revision
    )
    if no_longer_deletable or unexpected_candidates or protected_changed or recovery_changed:
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED",
            "a pending retention candidate is no longer deletable under the fresh protection plan",
            details={
                "state_may_have_changed": True,
                "pending_deployment_ids": sorted(pending),
                "no_longer_deletable_ids": no_longer_deletable,
                "unexpected_candidate_ids": unexpected_candidates,
                "protected_set_changed": protected_changed,
                "recovery_scope_changed": recovery_changed,
                "fresh_plan_sha256": fresh_plan.plan_sha256,
            },
        )


__all__ = [
    "DeploymentCacheRetentionCandidateValidator",
    "candidate_validation_failure",
    "require_pending_candidates_remain_deletable",
]
