"""Complete immutable source membership for parent physical admission.

Construction validates shape and closure, not artifact authenticity. Only the
producer-backed reader may supply this value to an activation composition root.
"""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.dbt_source_inventory_binding import DbtReleaseSources
from dpone.contracts.release_composition_ordinary import OrdinaryReleaseCapture


@dataclass(frozen=True, slots=True)
class CompositionSourceSnapshot:
    """Retain native identity, ordinary identity and every final workload pin.

    Transfer manifests are exact bounded bytes from verified source archives.
    They include native-generated transfers: no native-only subset can satisfy
    the complete parent workload partition. Bytes remain detached from the tree.
    """

    release_id: str
    native: DbtReleaseSources
    ordinary: OrdinaryReleaseCapture
    workload_pins: tuple[tuple[str, str], ...]
    transfer_manifests: tuple[tuple[str, bytes], ...]

    def __post_init__(self) -> None:
        if not is_canonical_sha256_digest(self.release_id):
            raise ValueError("composition source identity is invalid")
        for pairs in (self.workload_pins, self.transfer_manifests):
            if not isinstance(pairs, tuple) or any(type(pair) is not tuple or len(pair) != 2 for pair in pairs):
                raise ValueError("composition source entries must be immutable pairs")
        pins = dict(self.workload_pins)
        ordinary_ids = {row["id"] for row in self.ordinary.inventory["workload_packs"]}
        native_ids = {workload for workload, _ in self.native.required_workloads}
        if (
            not isinstance(self.workload_pins, tuple)
            or tuple(sorted(self.workload_pins)) != self.workload_pins
            or len(pins) != len(self.workload_pins)
            or native_ids & ordinary_ids
            or set(pins) != native_ids | ordinary_ids
            or any(not is_canonical_sha256_digest(digest) for digest in pins.values())
        ):
            raise ValueError("composition workloads do not cover the complete source closure")
        transfers = {write.resource_id for write in self.relation_writes if write.kind == "transfer"}
        if (
            not isinstance(self.transfer_manifests, tuple)
            or tuple(sorted(self.transfer_manifests)) != self.transfer_manifests
            or len(dict(self.transfer_manifests)) != len(self.transfer_manifests)
            or {key for key, _ in self.transfer_manifests} != transfers
            or any(
                not isinstance(body, bytes) or not 0 < len(body) <= 8 * 1024 * 1024
                for _, body in self.transfer_manifests
            )
        ):
            raise ValueError("composition transfer sources are incomplete")

    @property
    def relation_writes(self) -> tuple[DbtRelationWrite, ...]:
        """Full native helpers/models/generated transfers plus ordinary targets."""
        return (*self.native.relation_writes, *self.ordinary.relation_writes)

    @property
    def subject_sha256(self) -> str:
        """Bind immutable source IDs; mutable physical observations are separate."""
        return canonical_fingerprint(
            {
                "schema": "dpone.composition-source-subject.v1",
                "release_id": self.release_id,
                "native_release_id": self.native.release_id,
                "native_snapshot_sha256": self.native.inventory.snapshot_sha256,
                "ordinary_inventory_sha256": self.ordinary.inventory_sha256,
                "workloads": [list(pin) for pin in self.workload_pins],
            }
        )
