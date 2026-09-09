"""Application composition root for destructive deployment-cache retention."""

from __future__ import annotations

from pathlib import Path

from dpone.adapters.deployment_cache_retention_receipt_store import DeploymentCacheRetentionReceiptStore
from dpone.runtime.deployment_cache_retention_applier import InjectedDeploymentCacheRetentionApplier
from dpone.runtime.deployment_cache_retention_factory import (
    DeploymentCacheActivationHistory,
    DeploymentCacheProjectionValidator,
    DeploymentCacheRetentionCandidateValidator,
    DeploymentCacheRetentionPlanner,
    DeploymentCacheRetentionReceipts,
    DeploymentCacheRetentionTransactionCoordinator,
    assemble_deployment_cache_retention_applier,
)


def build_deployment_cache_retention_applier(
    cache_root: str | Path,
    *,
    allowed_promoters: tuple[str, ...] = (),
    validator: DeploymentCacheProjectionValidator | None = None,
    transactions: DeploymentCacheRetentionTransactionCoordinator | None = None,
    activation_history: DeploymentCacheActivationHistory | None = None,
    planner: DeploymentCacheRetentionPlanner | None = None,
    receipts: DeploymentCacheRetentionReceipts | None = None,
    candidate_validator: DeploymentCacheRetentionCandidateValidator | None = None,
) -> InjectedDeploymentCacheRetentionApplier:
    """Assemble runtime policy and concrete persistence at the app boundary."""

    root = Path(cache_root).resolve(strict=False)
    resolved_receipts = receipts or DeploymentCacheRetentionReceipts(DeploymentCacheRetentionReceiptStore(root))
    return assemble_deployment_cache_retention_applier(
        root,
        applier_factory=InjectedDeploymentCacheRetentionApplier,
        receipts=resolved_receipts,
        allowed_promoters=allowed_promoters,
        validator=validator,
        transactions=transactions,
        activation_history=activation_history,
        planner=planner,
        candidate_validator=candidate_validator,
    )


__all__ = ["build_deployment_cache_retention_applier"]
