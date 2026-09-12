"""Immutable deployment capability for protected composition supervision."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from dpone.kubernetes_names import require_kubernetes_dns_label

COMPOSITION_SUPERVISOR_SCHEMA = "dpone.composition-supervisor.v1"

_PROJECTION_FIELDS = frozenset(
    {
        "schema",
        "persistent_volume_claim",
        "child_uid_start",
        "child_gid_start",
        "child_identity_count",
    }
)
_MIN_CHILD_IDENTITY = 1_000_000
_MIN_IDENTITY_COUNT = 1_000_000
_MAX_IDENTITY_STOP = 2**31
_COMPOSITION_RELEASE_SCHEMA = "dpone.release-set.v3"
_LEGACY_RELEASE_SCHEMAS = frozenset({"dpone.release-set.v1", "dpone.release-set.v2"})


@dataclass(frozen=True, slots=True)
class CompositionSupervisorProjection:
    """One exact supervisor PVC and reserved child-identity capability."""

    persistent_volume_claim: str
    child_uid_start: int
    child_gid_start: int
    child_identity_count: int
    schema: str = COMPOSITION_SUPERVISOR_SCHEMA

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> CompositionSupervisorProjection:
        """Parse and validate one closed supervisor projection."""

        if set(value) != _PROJECTION_FIELDS:
            raise ValueError("composition_supervisor_shape")
        result = cls(
            schema=cast(str, value["schema"]),
            persistent_volume_claim=cast(str, value["persistent_volume_claim"]),
            child_uid_start=cast(int, value["child_uid_start"]),
            child_gid_start=cast(int, value["child_gid_start"]),
            child_identity_count=cast(int, value["child_identity_count"]),
        )
        result.require_valid()
        return result

    @property
    def child_uid_stop(self) -> int:
        """Return the exclusive UID range stop."""

        return self.child_uid_start + self.child_identity_count

    @property
    def child_gid_stop(self) -> int:
        """Return the exclusive GID range stop."""

        return self.child_gid_start + self.child_identity_count

    def require_valid(self) -> None:
        """Reject incomplete, unsafe, or platform-incompatible capabilities."""

        if self.schema != COMPOSITION_SUPERVISOR_SCHEMA:
            raise ValueError("composition_supervisor_schema")
        try:
            require_kubernetes_dns_label(
                self.persistent_volume_claim,
                context="composition_supervisor persistent_volume_claim",
            )
        except ValueError as exc:
            raise ValueError("composition_supervisor_persistent_volume_claim") from exc
        if type(self.child_identity_count) is not int or self.child_identity_count < _MIN_IDENTITY_COUNT:
            raise ValueError("composition_supervisor_identity_range")
        for start in (self.child_uid_start, self.child_gid_start):
            if (
                type(start) is not int
                or start < _MIN_CHILD_IDENTITY
                or start + self.child_identity_count >= _MAX_IDENTITY_STOP
            ):
                raise ValueError("composition_supervisor_identity_range")

    def to_dict(self) -> dict[str, object]:
        """Return the canonical JSON-compatible wire projection."""

        return {
            "schema": self.schema,
            "persistent_volume_claim": self.persistent_volume_claim,
            "child_uid_start": self.child_uid_start,
            "child_gid_start": self.child_gid_start,
            "child_identity_count": self.child_identity_count,
        }


def supervisor_projection_for_release(
    *,
    release_schema: str,
    value: object,
) -> CompositionSupervisorProjection | None:
    """Apply the v3-required and v1/v2-forbidden release policy."""

    if release_schema == _COMPOSITION_RELEASE_SCHEMA:
        if not isinstance(value, Mapping):
            raise ValueError("composition_supervisor_required")
        return CompositionSupervisorProjection.from_mapping(value)
    if release_schema in _LEGACY_RELEASE_SCHEMAS:
        if value is not None:
            raise ValueError("composition_supervisor_forbidden")
        return None
    raise ValueError("composition_supervisor_parent_schema")


__all__ = [
    "COMPOSITION_SUPERVISOR_SCHEMA",
    "CompositionSupervisorProjection",
    "supervisor_projection_for_release",
]
