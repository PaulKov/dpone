"""Self-service facade for local Airflow deployment cache sync."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error, manual_fix
from dpone.runtime.deployment_cache import (
    DeploymentCacheCurrentState,
    DeploymentCacheError,
    DeploymentCacheMaterializer,
)

_RESTORE_RELEASE_CODES = frozenset(
    {
        "DPONE_CACHE_ARTIFACT_NOT_FOUND",
        "DPONE_CACHE_ARTIFACT_READ_FAILED",
        "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH",
        "DPONE_CACHE_CHECKSUM_MISMATCH",
        "DPONE_RELEASE_READ_FAILED",
    }
)
_REBUILD_RELEASE_CODES = frozenset(
    {
        "DPONE_RELEASE_ARTIFACTS_INVALID",
        "DPONE_RELEASE_FINGERPRINT_MISMATCH",
        "DPONE_RELEASE_ID_INVALID",
        "DPONE_RELEASE_INVALID",
        "DPONE_RELEASE_SET_INVALID",
        "DPONE_RELEASE_SCHEMA_INVALID",
        "DPONE_DEPLOYMENT_DIGEST_INVALID",
    }
)
_REBUILD_PROJECTION_CODES = frozenset(
    {
        "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
        "DPONE_AIRFLOW_INDEX_INVALID",
        "DPONE_AIRFLOW_INDEX_NOT_FOUND",
        "DPONE_AIRFLOW_INDEX_SCHEMA_INVALID",
        "DPONE_CACHE_PATH_ESCAPE",
        "DPONE_CACHE_REFERENCE_INVALID",
        "DPONE_CACHE_UNPINNED_REFERENCE",
        "DPONE_DEPLOYMENT_FINGERPRINT_MISMATCH",
        "DPONE_DEPLOYMENT_ID_INVALID",
        "DPONE_DEPLOYMENT_ID_MISMATCH",
        "DPONE_DEPLOYMENT_INDEX_MIRROR_MISMATCH",
        "DPONE_DEPLOYMENT_INVALID",
        "DPONE_DEPLOYMENT_NOT_FOUND",
        "DPONE_DEPLOYMENT_SCHEMA_INVALID",
        "DPONE_RELEASE_ID_MISMATCH",
        "DPONE_RELEASE_INDEX_ARTIFACT_MISMATCH",
    }
)


@dataclass(frozen=True, slots=True)
class PromotionPrecondition:
    """Final build-plane assertion evaluated at the promotion linearization point."""

    check: Callable[[], bool]
    code: str
    message: str

    def enforce(self) -> None:
        self._enforce(self.check)

    def _enforce(self, check: Callable[[], bool]) -> None:
        try:
            passed = check()
        except DeploymentCacheError:
            raise
        except Exception as exc:
            raise DeploymentCacheError(self.code, self.message) from exc
        if not passed:
            raise DeploymentCacheError(self.code, self.message)


def cache_sync_result(
    *,
    cache_root: str | Path,
    deployment_dir: str | Path,
    environment: str,
    promoted_by: str,
    confirm_promote: bool,
    allowed_promoters: tuple[str, ...] = (),
    expected_current_deployment_id: str | None = None,
    expect_current_absent: bool = False,
    source_commit: str | None = None,
    attestation_ref: str | None = None,
    promotion_precondition: PromotionPrecondition | None = None,
) -> SelfServiceResult:
    """Promote one complete deployment projection with explicit platform acknowledgement."""

    if not confirm_promote:
        return _failed(
            "DPONE_DEPLOYMENT_CACHE_SYNC_CONFIRMATION_REQUIRED",
            "deployment cache sync requires explicit promotion confirmation",
            path=str(deployment_dir),
            environment=environment,
        )
    policy_error = _promoter_policy_error(
        promoted_by=promoted_by,
        allowed_promoters=allowed_promoters,
        path=str(deployment_dir),
        environment=environment,
    )
    if policy_error is not None:
        return policy_error
    try:
        current = DeploymentCacheMaterializer(cache_root, allowed_promoters=allowed_promoters).promote(
            deployment_dir,
            environment=environment,
            promoted_by=promoted_by,
            expected_current_deployment_id=expected_current_deployment_id,
            expect_current_absent=expect_current_absent,
            source_commit=source_commit,
            attestation_ref=attestation_ref,
            precommit_check=(promotion_precondition.enforce if promotion_precondition is not None else None),
        )
    except DeploymentCacheError as exc:
        return _failed(
            exc.code,
            str(exc),
            path=exc.path or str(deployment_dir),
            environment=environment,
            failure_details=exc.details,
        )
    return SelfServiceResult(passed=True, details=current.to_dict())


def local_cache_sync_result(
    *,
    cache_root: str | Path,
    deployment_dir: str | Path,
    environment: str,
    promoted_by: str,
    promotion_precondition: PromotionPrecondition | None = None,
    expected_current_deployment_id: str | None = None,
) -> SelfServiceResult:
    """Promote a local facade projection with an exact actor policy and CAS."""

    root = Path(cache_root).resolve(strict=False)
    current_deployment_id = expected_current_deployment_id
    if current_deployment_id is None:
        try:
            current_deployment_id = DeploymentCacheCurrentState(root).active_deployment_id()
        except DeploymentCacheError as exc:
            return _failed(
                exc.code,
                str(exc),
                path=exc.path or str(root / "current"),
                environment=environment,
                failure_details=exc.details,
            )
    return cache_sync_result(
        cache_root=root,
        deployment_dir=deployment_dir,
        environment=environment,
        promoted_by=promoted_by,
        allowed_promoters=(promoted_by,),
        confirm_promote=True,
        expected_current_deployment_id=current_deployment_id,
        expect_current_absent=current_deployment_id is None,
        promotion_precondition=promotion_precondition,
    )


def _promoter_policy_error(
    *,
    promoted_by: str,
    allowed_promoters: tuple[str, ...],
    path: str,
    environment: str,
) -> SelfServiceResult | None:
    if not promoted_by:
        return _failed(
            "DPONE_CURRENT_POINTER_PROMOTER_MISSING",
            "current pointer promotion requires promoted_by",
            path=path,
            environment=environment,
        )
    allowed = frozenset(actor for actor in allowed_promoters if actor)
    if not allowed or promoted_by not in allowed:
        return _failed(
            "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED",
            "current pointer promotion requires an allowed CI/service identity",
            path=path,
            environment=environment,
        )
    return None


def _failed(
    code: str,
    message: str,
    *,
    path: str,
    environment: str,
    failure_details: dict[str, object] | None = None,
) -> SelfServiceResult:
    fixes = _fixes_for(code)
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                code,
                message,
                stage="cache_sync",
                path=path,
                fixes=fixes or None,
                docs_url=(
                    "https://paulkov.github.io/dpone/errors/DPONE_CACHE_CHECKSUM_MISMATCH/"
                    if code == "DPONE_CACHE_CHECKSUM_MISMATCH"
                    else None
                ),
            ),
        ),
        details={"environment": environment, **(failure_details or {})},
        exit_code=4,
    )


def _fixes_for(code: str) -> list[dict[str, str]]:
    if code == "DPONE_DEPLOYMENT_CACHE_SYNC_CONFIRMATION_REQUIRED":
        return [manual_fix("confirm_cache_promotion")]
    if code in {"DPONE_CURRENT_POINTER_PROMOTER_MISSING", "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED"}:
        return [manual_fix("use_allowed_promotion_identity")]
    if code in {"DPONE_CURRENT_POINTER_CAS_INVALID", "DPONE_CURRENT_POINTER_CAS_MISMATCH"}:
        return [manual_fix("refresh_current_pointer_and_retry")]
    if code == "DPONE_DEPLOYMENT_PATH_OUTSIDE_CACHE_ROOT":
        return [manual_fix("select_deployment_inside_cache_root")]
    if code == "DPONE_DEPLOYMENT_PATH_INVALID":
        return [manual_fix("use_canonical_deployment_cache_layout")]
    if code in {
        "DPONE_CACHE_PROMOTION_LOCK_FAILED",
        "DPONE_CACHE_PROMOTION_WRITE_FAILED",
        "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED",
    }:
        return [manual_fix("plan_cache_recovery")]
    if code in _RESTORE_RELEASE_CODES:
        return [manual_fix("restore_pinned_release_artifacts")]
    if code in _REBUILD_RELEASE_CODES:
        return [manual_fix("rebuild_release_and_deployment")]
    if code in _REBUILD_PROJECTION_CODES:
        return [manual_fix("rebuild_deployment_projection")]
    if code == "DPONE_RELEASE_NOT_FOUND":
        return [manual_fix("materialize_release_or_rebuild_projection")]
    if code == "DPONE_CACHE_ARTIFACT_TOO_LARGE":
        return [manual_fix("review_cache_artifact_size_policy")]
    return []


__all__ = ["PromotionPrecondition", "cache_sync_result", "local_cache_sync_result"]
