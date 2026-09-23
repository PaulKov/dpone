"""Concrete composition for one local SQL permission-grant release."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from time import monotonic
from typing import Any

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_permission_grant_settlement_evidence_actor import (
    SqlClientPermissionGrantSettlementEvidenceActor,
)
from dpone.app.mssql_tds_coordinator_composition import TdsActorPoolCapability
from dpone.services.mssql_tds_permission_grant_release import (
    PermissionGrantLocallyReleased,
    permission_grant_settlement_evidence_context,
    release_permission_grant_locally,
)


def release_mssql_sqlclient_permission_locally(
    pool: TdsActorPoolCapability,
    owner: Any,
    evidence_root: Path,
    *,
    deadline: float,
    containment_deadline: float | None,
) -> PermissionGrantLocallyReleased:
    """Allocate one pinned evidence actor and stop after exact local exit."""
    if not isinstance(evidence_root, Path) or not evidence_root.is_absolute():
        raise ValueError("mssql_native.sqlclient_permission_release_composition_invalid")

    def evidence_factory(subject, binding):
        @contextmanager
        def writer() -> Iterator[DescriptorPinnedCreateOnlyEvidenceWriter]:
            yield DescriptorPinnedCreateOnlyEvidenceWriter(evidence_root)

        return pool.open(
            lambda actor_deadline, actor_clock: SqlClientPermissionGrantSettlementEvidenceActor(
                writer,
                permission_grant_settlement_evidence_context(subject, binding),
                actor_deadline,
                actor_clock,
            ),
            deadline=deadline,
        )

    return release_permission_grant_locally(
        owner,
        evidence_factory,
        deadline=deadline,
        containment_deadline=containment_deadline,
        clock=monotonic,
    )
