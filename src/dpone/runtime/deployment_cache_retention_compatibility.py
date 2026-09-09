"""Explicit concrete assembly retained only for the historical runtime constructor."""

from __future__ import annotations

from pathlib import Path

from dpone.adapters.deployment_cache_retention_receipt_store import DeploymentCacheRetentionReceiptStore
from dpone.runtime.deployment_cache_retention_applier import InjectedDeploymentCacheRetentionApplier
from dpone.runtime.deployment_cache_retention_factory import assemble_deployment_cache_retention_applier
from dpone.runtime.deployment_cache_retention_receipts import DeploymentCacheRetentionReceipts


def build_legacy_deployment_cache_retention_applier(
    cache_root: str | Path,
    *,
    allowed_promoters: tuple[str, ...] = (),
) -> InjectedDeploymentCacheRetentionApplier:
    """Adapt the legacy constructor to the canonical injected runtime service."""

    root = Path(cache_root).resolve(strict=False)
    receipts = DeploymentCacheRetentionReceipts(DeploymentCacheRetentionReceiptStore(root))
    return assemble_deployment_cache_retention_applier(
        root,
        applier_factory=InjectedDeploymentCacheRetentionApplier,
        receipts=receipts,
        allowed_promoters=allowed_promoters,
    )


__all__ = ["build_legacy_deployment_cache_retention_applier"]
