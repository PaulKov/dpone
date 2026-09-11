"""Local recovery planning and apply services for Airflow deployment cache."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer
from dpone.runtime.deployment_cache_audit import inspect_promotion_audit
from dpone.runtime.deployment_cache_common import (
    promotion_lock,
    read_regular_json_object,
    resolve_relative_current_symlink,
)
from dpone.runtime.deployment_cache_current_state import DeploymentCacheCurrentState
from dpone.runtime.deployment_cache_recovery_models import (
    DeploymentCacheRecoveryApplyError,
    DeploymentCacheRecoveryApplyReport,
    DeploymentCacheRecoveryCandidate,
    DeploymentCacheRecoveryIssue,
    DeploymentCacheRecoveryPlan,
)
from dpone.runtime.deployment_cache_recovery_policy import (
    assess_pointer,
    has_unidentifiable_current,
    is_audit_only_repair,
    pointer_identity,
    preferred_repair_id,
    recovery_status,
)


class DeploymentCacheRecoveryPlanner:
    """Diagnose local deployment cache current-pointer/current consistency."""

    def __init__(self, cache_root: str | Path) -> None:
        self._cache_root = Path(cache_root).resolve(strict=False)

    def plan(self, *, environment: str) -> DeploymentCacheRecoveryPlan:
        pointer_path = self._cache_root / "current-pointer.json"
        current_path = self._cache_root / "current"
        issues: list[DeploymentCacheRecoveryIssue] = []
        pointer = self._read_pointer(pointer_path, issues)
        pointer_deployment_id, pointer_release_id = pointer_identity(pointer, environment=environment)
        current_present = current_path.exists() or current_path.is_symlink()
        current_path_deployment_id = DeploymentCacheCurrentState(self._cache_root).actual_deployment_id()
        current_release_id: str | None = None
        if pointer is not None and pointer.get("environment") != environment:
            issues.append(
                DeploymentCacheRecoveryIssue(
                    code="DPONE_CURRENT_POINTER_ENVIRONMENT_MISMATCH",
                    severity="error",
                    message="current pointer does not match requested environment",
                    path=pointer_path.as_posix(),
                )
            )
        if not current_present:
            issues.append(
                DeploymentCacheRecoveryIssue(
                    code="DPONE_CURRENT_PATH_MISSING",
                    severity="error",
                    message="current deployment path is missing",
                    path=current_path.as_posix(),
                )
            )
        else:
            try:
                current_target = resolve_relative_current_symlink(self._cache_root)
                current_projection = DeploymentCacheMaterializer(self._cache_root).validate_current_details(
                    current_target,
                    environment=environment,
                )
                validated_current_id = current_projection.deployment_id
                current_release_id = current_projection.release_id
                if current_path_deployment_id is None:
                    current_path_deployment_id = validated_current_id
            except DeploymentCacheError as exc:
                issues.append(
                    DeploymentCacheRecoveryIssue(
                        code=exc.code,
                        severity="error",
                        message=f"current deployment is invalid: {exc}",
                        path=exc.path or current_path.as_posix(),
                    )
                )
            if (
                current_release_id is not None
                and pointer_deployment_id is not None
                and pointer_release_id != current_release_id
            ):
                issues.append(
                    DeploymentCacheRecoveryIssue(
                        code="DPONE_CURRENT_RELEASE_ID_MISMATCH",
                        severity="error",
                        message="current projection release id does not match current pointer",
                        path=pointer_path.as_posix(),
                    )
                )
            if current_path_deployment_id is None:
                issues.append(
                    DeploymentCacheRecoveryIssue(
                        code="DPONE_CURRENT_PATH_ID_UNAVAILABLE",
                        severity="error",
                        message="current path has no canonical deployment identity for recovery CAS",
                        path=current_path.as_posix(),
                    )
                )
            if (
                current_path_deployment_id is not None
                and pointer_deployment_id is not None
                and current_path_deployment_id != pointer_deployment_id
            ):
                issues.append(
                    DeploymentCacheRecoveryIssue(
                        code="DPONE_CURRENT_DEPLOYMENT_ID_MISMATCH",
                        severity="error",
                        message="current path deployment id does not match current pointer",
                        path=current_path.as_posix(),
                    )
                )
        if pointer is not None and pointer_deployment_id is not None:
            audit_path = self._cache_root / "current-pointer-audit.jsonl"
            issues.extend(
                DeploymentCacheRecoveryIssue(
                    code=issue.code,
                    severity=issue.severity,
                    message=issue.message,
                    path=issue.path,
                )
                for issue in inspect_promotion_audit(
                    audit_path,
                    expected_pointer=pointer,
                    environment=environment,
                )
            )
        recovery_issues = issues if issues else None
        candidates = self._repair_candidates(
            environment=environment,
            preferred_deployment_ids=(current_path_deployment_id, pointer_deployment_id),
            issues=recovery_issues,
        )
        preferred_id = preferred_repair_id(
            candidates,
            preferred_deployment_ids=(current_path_deployment_id, pointer_deployment_id),
        )
        status = recovery_status(
            issues,
            preferred_deployment_id=preferred_id,
            current_identity_unavailable=current_present and current_path_deployment_id is None,
        )
        return DeploymentCacheRecoveryPlan(
            environment=environment,
            status=status,
            current_deployment_id=pointer_deployment_id,
            current_path_deployment_id=current_path_deployment_id,
            preferred_repair_deployment_id=preferred_id,
            issues=tuple(issues),
            repair_candidates=tuple(candidates),
        )

    def _read_pointer(self, pointer_path: Path, issues: list[DeploymentCacheRecoveryIssue]) -> dict[str, Any] | None:
        if pointer_path.is_symlink():
            issues.append(
                DeploymentCacheRecoveryIssue(
                    code="DPONE_CURRENT_POINTER_UNSAFE",
                    severity="error",
                    message="current pointer must not be a symlink",
                    path=pointer_path.as_posix(),
                )
            )
            return None
        if not pointer_path.exists():
            issues.append(
                DeploymentCacheRecoveryIssue(
                    code="DPONE_CURRENT_POINTER_NOT_FOUND",
                    severity="error",
                    message="current pointer is missing",
                    path=pointer_path.as_posix(),
                )
            )
            return None
        try:
            pointer = read_regular_json_object(
                pointer_path,
                missing_code="DPONE_CURRENT_POINTER_NOT_FOUND",
                invalid_code="DPONE_CURRENT_POINTER_INVALID",
                label="current pointer",
                root=self._cache_root,
            )
        except (DeploymentCacheError, UnicodeError, OSError) as exc:
            issues.append(
                DeploymentCacheRecoveryIssue(
                    code="DPONE_CURRENT_POINTER_INVALID",
                    severity="error",
                    message=f"current pointer is invalid: {exc}",
                    path=pointer_path.as_posix(),
                )
            )
            return None
        violation = assess_pointer(pointer).violation
        if violation is not None:
            issues.append(
                DeploymentCacheRecoveryIssue(
                    code=violation.code,
                    severity="error",
                    message=violation.message,
                    path=pointer_path.as_posix(),
                )
            )
        return pointer

    def _repair_candidates(
        self,
        *,
        environment: str,
        preferred_deployment_ids: tuple[str | None, ...],
        issues: list[DeploymentCacheRecoveryIssue] | None = None,
    ) -> list[DeploymentCacheRecoveryCandidate]:
        deployment_root = self._cache_root / "deployments" / environment
        if not deployment_root.exists():
            return []
        candidates: list[DeploymentCacheRecoveryCandidate] = []
        materializer = DeploymentCacheMaterializer(self._cache_root)
        for path in deployment_root.iterdir():
            try:
                deployment = materializer.validate(path, environment=environment)
            except (DeploymentCacheError, OSError, UnicodeError, json.JSONDecodeError) as exc:
                if issues is not None:
                    code = exc.code if isinstance(exc, DeploymentCacheError) else "DPONE_DEPLOYMENT_INVALID"
                    error_path = exc.path if isinstance(exc, DeploymentCacheError) else None
                    issues.append(
                        DeploymentCacheRecoveryIssue(
                            code=code,
                            severity="error",
                            message=f"deployment projection is not a safe recovery candidate: {exc}",
                            path=error_path or path.as_posix(),
                        )
                    )
                continue
            deployment_id = str(deployment["deployment_id"])
            candidates.append(
                DeploymentCacheRecoveryCandidate(
                    deployment_id=deployment_id,
                    path=path.as_posix(),
                    reason="current_state" if deployment_id in preferred_deployment_ids else "complete",
                )
            )
        return sorted(
            candidates,
            key=lambda candidate: (candidate.reason != "current_state", candidate.deployment_id),
        )


class DeploymentCacheRecoveryApplier:
    """Repair current pointer/current path by promoting one explicit deployment."""

    def __init__(self, cache_root: str | Path) -> None:
        self._cache_root = Path(cache_root).resolve(strict=False)

    def apply(
        self,
        *,
        environment: str,
        deployment_id: str,
        confirm_repair: bool,
        promoted_by: str,
        expected_current_deployment_id: str | None,
        allowed_promoters: tuple[str, ...] = (),
    ) -> DeploymentCacheRecoveryApplyReport:
        if not confirm_repair:
            raise DeploymentCacheRecoveryApplyError(
                "DPONE_DEPLOYMENT_CACHE_RECOVERY_CONFIRMATION_REQUIRED",
                "deployment cache recovery requires explicit repair confirmation",
            )
        if not promoted_by:
            raise DeploymentCacheRecoveryApplyError(
                "DPONE_CURRENT_POINTER_PROMOTER_MISSING",
                "deployment cache recovery requires a CI/service identity",
            )
        try:
            with promotion_lock(self._cache_root):
                plan = DeploymentCacheRecoveryPlanner(self._cache_root).plan(environment=environment)
                if plan.current_path_deployment_id != expected_current_deployment_id:
                    raise DeploymentCacheRecoveryApplyError(
                        "DPONE_CURRENT_POINTER_CAS_MISMATCH",
                        "active current deployment changed after the recovery plan was reviewed",
                        path=(self._cache_root / "current").as_posix(),
                    )
                if has_unidentifiable_current(plan):
                    raise DeploymentCacheRecoveryApplyError(
                        "DPONE_DEPLOYMENT_CACHE_RECOVERY_BLOCKED",
                        "deployment cache recovery is blocked until current has a canonical CAS identity",
                        path=(self._cache_root / "current").as_posix(),
                    )
                if plan.status == "ok":
                    raise DeploymentCacheRecoveryApplyError(
                        "DPONE_DEPLOYMENT_CACHE_RECOVERY_NOT_REQUIRED",
                        "deployment cache is healthy; recovery apply is not allowed",
                    )
                deployment_dir = self._cache_root / "deployments" / environment / _deployment_dir_name(deployment_id)
                if not deployment_dir.exists():
                    raise DeploymentCacheRecoveryApplyError(
                        "DPONE_DEPLOYMENT_NOT_FOUND",
                        "selected recovery deployment does not exist",
                        path=deployment_dir.as_posix(),
                    )
                candidate_ids = {candidate.deployment_id for candidate in plan.repair_candidates}
                if deployment_id not in candidate_ids:
                    raise DeploymentCacheRecoveryApplyError(
                        "DPONE_DEPLOYMENT_CACHE_RECOVERY_CANDIDATE_INVALID",
                        "selected deployment is not a fully validated recovery candidate",
                    )
                materializer = DeploymentCacheMaterializer(
                    self._cache_root,
                    allowed_promoters=allowed_promoters,
                )
                if is_audit_only_repair(plan) and deployment_id == expected_current_deployment_id:
                    current = materializer.repair_audit(
                        environment=environment,
                        recovery_actor=promoted_by,
                        expected_current_deployment_id=deployment_id,
                    )
                else:
                    current = materializer.recover(
                        deployment_dir,
                        environment=environment,
                        promoted_by=promoted_by,
                        expected_current_deployment_id=expected_current_deployment_id,
                    )
        except DeploymentCacheError as exc:
            raise DeploymentCacheRecoveryApplyError(
                exc.code,
                str(exc),
                path=exc.path,
                details=exc.details,
            ) from exc
        return DeploymentCacheRecoveryApplyReport(
            environment=environment,
            recovered_deployment_id=current.deployment_id,
            release_id=current.release_id,
            current_path=current.current_path.as_posix(),
            pointer_path=current.pointer_path.as_posix(),
        )


def _deployment_dir_name(deployment_id: str) -> str:
    return deployment_id.replace(":", "-", 1)


__all__ = [
    "DeploymentCacheRecoveryApplier",
    "DeploymentCacheRecoveryApplyError",
    "DeploymentCacheRecoveryApplyReport",
    "DeploymentCacheRecoveryCandidate",
    "DeploymentCacheRecoveryIssue",
    "DeploymentCacheRecoveryPlan",
    "DeploymentCacheRecoveryPlanner",
]
