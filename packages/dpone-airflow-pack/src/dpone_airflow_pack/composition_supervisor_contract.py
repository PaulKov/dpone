"""Dependency-light provider contract for composition supervisor authority."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dpone_airflow_pack.connection_names import require_kubernetes_dns_label

SUPERVISOR_SCHEMA = "dpone.composition-supervisor.v1"
SUPERVISOR_FIELDS = frozenset(
    {
        "schema",
        "persistent_volume_claim",
        "child_uid_start",
        "child_gid_start",
        "child_identity_count",
    }
)


@dataclass(frozen=True, slots=True)
class CompositionSupervisorProjection:
    """Canonical non-secret Kubernetes supervisor capability."""

    persistent_volume_claim: str
    child_uid_start: int
    child_gid_start: int
    child_identity_count: int

    def to_dict(self) -> dict[str, str | int]:
        return {
            "schema": SUPERVISOR_SCHEMA,
            "persistent_volume_claim": self.persistent_volume_claim,
            "child_uid_start": self.child_uid_start,
            "child_gid_start": self.child_gid_start,
            "child_identity_count": self.child_identity_count,
        }


def parse_composition_supervisor(
    value: object,
) -> CompositionSupervisorProjection | None:
    """Parse the closed projection or reject without importing core dpone."""

    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != SUPERVISOR_FIELDS:
        raise ValueError("composition_supervisor_invalid")
    try:
        claim = require_kubernetes_dns_label(
            value.get("persistent_volume_claim"),
            context="composition supervisor persistent_volume_claim",
        )
    except ValueError as exc:
        raise ValueError("composition_supervisor_invalid") from exc
    count = value.get("child_identity_count")
    uid_start = value.get("child_uid_start")
    gid_start = value.get("child_gid_start")
    if (
        value.get("schema") != SUPERVISOR_SCHEMA
        or claim != value.get("persistent_volume_claim")
        or type(count) is not int
        or count < 1_000_000
        or any(
            type(start) is not int or start < 1_000_000 or start + count >= 2**31 for start in (uid_start, gid_start)
        )
    ):
        raise ValueError("composition_supervisor_invalid")
    assert isinstance(uid_start, int)
    assert isinstance(gid_start, int)
    return CompositionSupervisorProjection(
        persistent_volume_claim=claim,
        child_uid_start=uid_start,
        child_gid_start=gid_start,
        child_identity_count=count,
    )


__all__ = [
    "CompositionSupervisorProjection",
    "parse_composition_supervisor",
]
