"""Physical-generation classification for external ClickHouse publication."""

from __future__ import annotations

from dpone.contracts.clickhouse_external_replication import (
    MemberGenerationObservation,
    MemberPublicationState,
    PhysicalGeneration,
)


def classify_member_publication(
    observation: MemberGenerationObservation,
    *,
    desired: PhysicalGeneration,
    predecessor: PhysicalGeneration | None,
) -> MemberPublicationState:
    if observation.target == desired:
        if predecessor is None and observation.candidate is None:
            return MemberPublicationState.COMMITTED
        if predecessor is not None and observation.candidate == predecessor:
            return MemberPublicationState.COMMITTED
        if predecessor is not None and observation.candidate is None:
            return MemberPublicationState.CLEANUP_PENDING
    if observation.target == predecessor and observation.candidate == desired:
        return MemberPublicationState.PENDING
    return MemberPublicationState.UNKNOWN


__all__ = ["classify_member_publication"]
