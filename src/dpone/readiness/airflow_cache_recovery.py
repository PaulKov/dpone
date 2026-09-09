"""Readiness facade for local Airflow deployment cache recovery."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.runtime.deployment_cache_recovery import (
    DeploymentCacheRecoveryApplier,
    DeploymentCacheRecoveryApplyError,
    DeploymentCacheRecoveryPlanner,
)


class AirflowCacheRecoveryService:
    """Expose local cache recovery diagnostics and explicit repair operations."""

    def __init__(self, *, cache_root: str | Path = ".dpone-cache") -> None:
        self._cache_root = cache_root

    def plan(self, *, environment: str) -> dict[str, Any]:
        return DeploymentCacheRecoveryPlanner(self._cache_root).plan(environment=environment).to_dict()

    def apply(
        self,
        *,
        environment: str,
        deployment_id: str,
        confirm_repair: bool,
        promoted_by: str,
        expected_current_deployment_id: str | None,
        allowed_promoters: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        try:
            return (
                DeploymentCacheRecoveryApplier(self._cache_root)
                .apply(
                    environment=environment,
                    deployment_id=deployment_id,
                    confirm_repair=confirm_repair,
                    promoted_by=promoted_by,
                    expected_current_deployment_id=expected_current_deployment_id,
                    allowed_promoters=allowed_promoters,
                )
                .to_dict()
            )
        except DeploymentCacheRecoveryApplyError as exc:
            raise AirflowCacheRecoveryError(
                exc.code,
                str(exc),
                path=exc.path,
                details=exc.details,
            ) from exc


class AirflowCacheRecoveryError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        path: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.path = path
        self.details = dict(details or {})


__all__ = ["AirflowCacheRecoveryError", "AirflowCacheRecoveryService"]
