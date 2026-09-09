"""Composition of immutable backfill campaign contract fragments."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from dpone.backfill.execution_policy import config_hash, plan_hash

if TYPE_CHECKING:
    from dpone.backfill.campaign_lifecycle import BackfillCampaignLifecyclePort
    from dpone.backfill.target_publication import BackfillTargetPublicationPort
    from dpone.config.load_config import LoadConfig


def compose_campaign_contract(
    load_config: LoadConfig,
    *,
    lifecycle: BackfillCampaignLifecyclePort | None,
    target_publisher: BackfillTargetPublicationPort | None,
) -> Mapping[str, Any] | None:
    """Compose lifecycle and publication contracts without losing ownership."""

    lifecycle_contract = lifecycle.campaign_contract(load_config) if lifecycle is not None else None
    publication_contract = target_publisher.campaign_contract(load_config) if target_publisher is not None else None
    if lifecycle_contract is None:
        return publication_contract
    if publication_contract is None:
        return lifecycle_contract
    return {
        "campaign_lifecycle": dict(lifecycle_contract),
        "target_publication": dict(publication_contract),
    }


def campaign_identity_hashes(
    *,
    chunks: tuple[Any, ...],
    dataset: str,
    execution_policy: Any,
    campaign_contract: Mapping[str, Any] | None,
) -> tuple[str, str]:
    """Hash one immutable plan and configuration from the same contract."""

    return (
        plan_hash(chunks, execution_policy=execution_policy, campaign_contract=campaign_contract),
        config_hash(
            dataset=dataset,
            execution_policy=execution_policy,
            campaign_contract=campaign_contract,
        ),
    )


__all__ = ["campaign_identity_hashes", "compose_campaign_contract"]
