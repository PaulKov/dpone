"""Public deployment-cache facade and compatibility constructors."""

from __future__ import annotations

from pathlib import Path

from dpone.ports.airflow_desired_state import DesiredStateCheckpointReader
from dpone.runtime.deployment_cache_common import DeploymentCacheError
from dpone.runtime.deployment_cache_current_state import DeploymentCacheCurrentState
from dpone.runtime.deployment_cache_materializer import CurrentDeployment, DeploymentCacheMaterializer
from dpone.runtime.deployment_cache_retention_applier import InjectedDeploymentCacheRetentionApplier
from dpone.runtime.deployment_cache_retention_compatibility import (
    build_legacy_deployment_cache_retention_applier,
)
from dpone.runtime.deployment_cache_retention_contracts import (
    AirflowLoaderAcknowledgementReader,
    DeploymentCacheRetentionApplyError,
    DeploymentRetentionApplyItem,
    DeploymentRetentionApplyReport,
    DeploymentRetentionPlan,
    DeploymentRetentionPlanItem,
)
from dpone.runtime.deployment_cache_retention_planner import DeploymentCacheRetentionPlanner


def build_deployment_cache_retention_applier(
    cache_root: str | Path,
    *,
    allowed_promoters: tuple[str, ...] = (),
) -> InjectedDeploymentCacheRetentionApplier:
    """Build the historical default through the explicit compatibility assembly."""

    return build_legacy_deployment_cache_retention_applier(
        cache_root,
        allowed_promoters=allowed_promoters,
    )


class DeploymentCacheRetentionApplier:
    """Backward-compatible facade with an explicit, immutable delegate."""

    def __init__(
        self,
        cache_root: str | Path,
        *,
        allowed_promoters: tuple[str, ...] = (),
    ) -> None:
        self._cache_root = Path(cache_root).resolve(strict=False)
        self._delegate = build_deployment_cache_retention_applier(
            self._cache_root,
            allowed_promoters=allowed_promoters,
        )

    def apply(
        self,
        *,
        environment: str,
        confirm_delete: bool,
        promoted_by: str,
        expected_plan_sha256: str | None = None,
        review_id: str | None = None,
        loader_ack_reader: AirflowLoaderAcknowledgementReader | None = None,
        checkpoint_reader: DesiredStateCheckpointReader | None = None,
        protected_deployment_ids: tuple[str, ...] = (),
    ) -> DeploymentRetentionApplyReport:
        return self._delegate.apply(
            environment=environment,
            confirm_delete=confirm_delete,
            promoted_by=promoted_by,
            expected_plan_sha256=expected_plan_sha256,
            review_id=review_id,
            loader_ack_reader=loader_ack_reader,
            checkpoint_reader=checkpoint_reader,
            protected_deployment_ids=protected_deployment_ids,
        )


__all__ = [
    "CurrentDeployment",
    "DeploymentCacheCurrentState",
    "DeploymentCacheError",
    "DeploymentCacheMaterializer",
    "DeploymentCacheRetentionApplier",
    "InjectedDeploymentCacheRetentionApplier",
    "DeploymentCacheRetentionApplyError",
    "DeploymentCacheRetentionPlanner",
    "DeploymentRetentionApplyItem",
    "DeploymentRetentionApplyReport",
    "DeploymentRetentionPlan",
    "DeploymentRetentionPlanItem",
    "build_deployment_cache_retention_applier",
]
