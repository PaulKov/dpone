"""Fail-closed readiness adapter for the active Airflow deployment cache."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.safe_sample_deployment_context import AirflowDeploymentContext


from pathlib import Path

from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    promotion_lock,
    resolve_relative_current_symlink,
)
from dpone.runtime.deployment_cache_materializer import DeploymentCacheMaterializer
from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner
from dpone.services.safe_sample_deployment_context import (
    AirflowDeploymentContextResolution,
    resolve_airflow_deployment_context_from_projection,
)


def load_verified_airflow_deployment_context(
    cache_root: str | Path = ".dpone-cache",
    *,
    expected_environment: str | None = None,
) -> AirflowDeploymentContext | None:
    """Return current only when all durable cache controls and bytes agree."""

    return resolve_verified_airflow_deployment_context(
        cache_root,
        expected_environment=expected_environment,
    ).context


def resolve_verified_airflow_deployment_context(
    cache_root: str | Path = ".dpone-cache",
    *,
    expected_environment: str | None = None,
) -> AirflowDeploymentContextResolution:
    """Resolve verified current context or an explicit safe-sample compatibility error."""

    root = Path(cache_root).resolve(strict=False)
    try:
        with promotion_lock(root):
            current = root / "current"
            target = resolve_relative_current_symlink(root)
            environment = _target_environment(target, cache_root=root)
            if environment is None or expected_environment is not None and environment != expected_environment:
                return AirflowDeploymentContextResolution()
            plan = DeploymentCacheRecoveryPlanner(root).plan(environment=environment)
            if (
                plan.status != "ok"
                or plan.current_deployment_id is None
                or plan.current_deployment_id != plan.current_path_deployment_id
            ):
                return AirflowDeploymentContextResolution()
            projection = DeploymentCacheMaterializer(root).validate_current_details(
                target,
                environment=environment,
            )
            if projection.deployment_id != plan.current_deployment_id:
                return AirflowDeploymentContextResolution()
            return resolve_airflow_deployment_context_from_projection(
                deployment=projection.deployment,
                index=projection.airflow_index,
                index_path=current / "airflow-index.json",
                deployment_path=current / "deployment.json",
            )
    except (DeploymentCacheError, OSError, UnicodeError, ValueError):
        return AirflowDeploymentContextResolution()


def _target_environment(target: Path, *, cache_root: Path) -> str | None:
    try:
        parts = target.relative_to(cache_root).parts
    except ValueError:
        return None
    if len(parts) != 3 or parts[0] not in {"activations", "deployments"}:
        return None
    return parts[1]


__all__ = [
    "load_verified_airflow_deployment_context",
    "resolve_verified_airflow_deployment_context",
]
