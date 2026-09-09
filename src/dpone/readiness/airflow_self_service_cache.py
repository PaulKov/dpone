"""Self-service result builders for local Airflow deployment cache operations."""

from __future__ import annotations

from pathlib import Path

from dpone.readiness.airflow_cache_recovery import AirflowCacheRecoveryError, AirflowCacheRecoveryService
from dpone.readiness.airflow_cache_retention import AirflowCacheRetentionError, AirflowCacheRetentionService
from dpone.readiness.airflow_self_service_models import SelfServiceResult
from dpone.readiness.error_contract import dpone_error


def cache_retention_plan_result(
    *,
    cache_root: str | Path,
    environment: str,
    protected_deployment_ids: tuple[str, ...] = (),
    evidence_files: tuple[str | Path, ...] = (),
) -> SelfServiceResult:
    try:
        plan = AirflowCacheRetentionService(cache_root=cache_root).plan(
            environment=environment,
            protected_deployment_ids=protected_deployment_ids,
            evidence_files=evidence_files,
        )
        return SelfServiceResult(passed=True, details=plan)
    except AirflowCacheRetentionError as exc:
        return _error_result(
            exc.code,
            str(exc),
            exc.path,
            exit_code=2 if exc.code == "DPONE_DEPLOYMENT_ID_INVALID" else None,
        )


def cache_retention_apply_result(
    *,
    cache_root: str | Path,
    environment: str,
    confirm_delete: bool,
    promoted_by: str,
    allowed_promoters: tuple[str, ...],
    expected_plan_sha256: str | None = None,
    review_id: str | None = None,
    loader_ack_file: str | Path | None = None,
    evidence_version: str = "v1",
    protected_deployment_ids: tuple[str, ...] = (),
    evidence_files: tuple[str | Path, ...] = (),
) -> SelfServiceResult:
    try:
        report = AirflowCacheRetentionService(cache_root=cache_root).apply(
            environment=environment,
            confirm_delete=confirm_delete,
            promoted_by=promoted_by,
            allowed_promoters=allowed_promoters,
            expected_plan_sha256=expected_plan_sha256,
            review_id=review_id,
            loader_ack_file=loader_ack_file,
            evidence_version=evidence_version,
            protected_deployment_ids=protected_deployment_ids,
            evidence_files=evidence_files,
        )
        return SelfServiceResult(passed=True, details=report)
    except AirflowCacheRetentionError as exc:
        exit_code = (
            4
            if exc.code
            in {
                "DPONE_DEPLOYMENT_CACHE_GC_CONFIRMATION_REQUIRED",
                "DPONE_CURRENT_POINTER_PROMOTER_MISSING",
                "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED",
            }
            else 2
            if exc.code == "DPONE_DEPLOYMENT_ID_INVALID"
            else 1
        )
        return _error_result(
            exc.code,
            str(exc),
            exc.path,
            exit_code=exit_code,
            failure_details=exc.details,
        )


def cache_recovery_plan_result(
    *,
    cache_root: str | Path,
    environment: str,
) -> SelfServiceResult:
    plan = AirflowCacheRecoveryService(cache_root=cache_root).plan(environment=environment)
    return SelfServiceResult(passed=True, details=plan)


def cache_recovery_apply_result(
    *,
    cache_root: str | Path,
    environment: str,
    deployment_id: str,
    confirm_repair: bool,
    promoted_by: str,
    expected_current_deployment_id: str | None,
    allowed_promoters: tuple[str, ...] = (),
) -> SelfServiceResult:
    try:
        report = AirflowCacheRecoveryService(cache_root=cache_root).apply(
            environment=environment,
            deployment_id=deployment_id,
            confirm_repair=confirm_repair,
            promoted_by=promoted_by,
            expected_current_deployment_id=expected_current_deployment_id,
            allowed_promoters=allowed_promoters,
        )
        return SelfServiceResult(passed=True, details=report)
    except AirflowCacheRecoveryError as exc:
        exit_code = (
            4
            if exc.code
            in {
                "DPONE_DEPLOYMENT_CACHE_RECOVERY_CONFIRMATION_REQUIRED",
                "DPONE_CURRENT_POINTER_PROMOTER_MISSING",
                "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED",
            }
            else 1
        )
        return _error_result(
            exc.code,
            str(exc),
            exc.path,
            exit_code=exit_code,
            environment=environment,
            failure_details=exc.details,
        )


def _error_result(
    code: str,
    message: str,
    path: str | None,
    *,
    exit_code: int | None = None,
    environment: str | None = None,
    failure_details: dict[str, object] | None = None,
) -> SelfServiceResult:
    return SelfServiceResult(
        passed=False,
        errors=(dpone_error(code, message, stage="airflow_cache", path=path),),
        details={"environment": environment, **(failure_details or {})} if environment else failure_details,
        exit_code=exit_code,
    )


__all__ = [
    "cache_recovery_apply_result",
    "cache_recovery_plan_result",
    "cache_retention_apply_result",
    "cache_retention_plan_result",
]
