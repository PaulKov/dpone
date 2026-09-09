"""Explicit compatibility composition for deployment-cache retention."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

from dpone.runtime.deployment_cache_activation_history import DeploymentCacheActivationHistory
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
from dpone.runtime.deployment_cache_retention_candidate import DeploymentCacheRetentionCandidateValidator
from dpone.runtime.deployment_cache_retention_planner import DeploymentCacheRetentionPlanner
from dpone.runtime.deployment_cache_retention_receipts import DeploymentCacheRetentionReceipts
from dpone.runtime.deployment_cache_retention_transaction import DeploymentCacheRetentionTransactionCoordinator

_ApplierT = TypeVar("_ApplierT")


def assemble_deployment_cache_retention_applier(
    cache_root: str | Path,
    *,
    applier_factory: Callable[..., _ApplierT],
    receipts: DeploymentCacheRetentionReceipts,
    allowed_promoters: tuple[str, ...] = (),
    validator: DeploymentCacheProjectionValidator | None = None,
    transactions: DeploymentCacheRetentionTransactionCoordinator | None = None,
    activation_history: DeploymentCacheActivationHistory | None = None,
    planner: DeploymentCacheRetentionPlanner | None = None,
    candidate_validator: DeploymentCacheRetentionCandidateValidator | None = None,
) -> _ApplierT:
    """Assemble concrete filesystem dependencies without a circular import."""

    root = Path(cache_root).resolve(strict=False)
    resolved_validator = validator if validator is not None else DeploymentCacheProjectionValidator(root)
    return applier_factory(
        root,
        allowed_promoters=allowed_promoters,
        validator=resolved_validator,
        transactions=(
            transactions
            if transactions is not None
            else DeploymentCacheRetentionTransactionCoordinator(root, validator=resolved_validator)
        ),
        activation_history=(
            activation_history if activation_history is not None else DeploymentCacheActivationHistory(root)
        ),
        planner=planner if planner is not None else DeploymentCacheRetentionPlanner(root),
        receipts=receipts,
        candidate_validator=(
            candidate_validator
            if candidate_validator is not None
            else DeploymentCacheRetentionCandidateValidator(root, validator=resolved_validator)
        ),
    )


__all__ = [
    "DeploymentCacheActivationHistory",
    "DeploymentCacheProjectionValidator",
    "DeploymentCacheRetentionCandidateValidator",
    "DeploymentCacheRetentionPlanner",
    "DeploymentCacheRetentionReceipts",
    "DeploymentCacheRetentionTransactionCoordinator",
    "assemble_deployment_cache_retention_applier",
]
