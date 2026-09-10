"""Pure byte binding for composition transport, without acquisition authority.

The immutable plan pins an ordered, metadata-validated inventory. Callers retain
filesystem confinement, read ordering and complete source admission. Descriptor,
embedded native and subject checks operate only on explicitly supplied bytes;
a passing transport check grants neither source trust nor signing authority.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.release_composition import MAX_COMPOSITION_TOTAL_BYTES, NATIVE_SIDECARS
from dpone.contracts.release_composition_policy import validate_composition_metadata
from dpone.contracts.release_composition_subject import composition_subject_bytes, composition_subject_sha256
from dpone.contracts.strict_json import strict_json_object


@dataclass(frozen=True, slots=True)
class CompositionTransportArtifact:
    """Detached byte and path pins for one admitted transport artifact."""

    path: str
    size: int
    sha256: str

    def verify_bytes(self, payload: bytes) -> None:
        """Require exact length and digest after the caller's bounded read."""
        if len(payload) != self.size or sha256_bytes(payload) != self.sha256:
            raise ValueError("composition artifact differs from its exact descriptor")


@dataclass(frozen=True, slots=True)
class CompositionTransportPlan:
    """Immutable inventory preserving the parent descriptor's traversal order.

    Construct with ``from_release`` at the metadata admission boundary. This
    value holds no release mappings or file reader and never acquires bytes.
    """

    descriptors: tuple[CompositionTransportArtifact, ...]

    @classmethod
    def from_release(cls, release: Mapping[str, Any]) -> CompositionTransportPlan:
        """Validate the complete parent before detaching its transport pins."""
        validate_composition_metadata(release)
        return cls(
            tuple(
                CompositionTransportArtifact(path, row["bytes"], row["sha256"])
                for path, row in project_composition_descriptors(release).items()
            )
        )

    def require_complete_byte_budget(self, descriptor_bytes: int, subject_bytes: int) -> None:
        """Charge descriptor and subject bytes before any artifact acquisition."""
        if sum(row.size for row in self.descriptors) + descriptor_bytes + subject_bytes > MAX_COMPOSITION_TOTAL_BYTES:
            raise ValueError("composition aggregate bytes including metadata exceed the bound")


def project_composition_descriptors(release: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    """Project paths without adding metadata admission to existing helper calls.

    Preserve section/row order and original row identity. Duplicate paths fail
    immediately, including on partial mappings accepted by the public adapter.
    """
    result = {}
    for rows in release["artifacts"].values():
        for row in rows:
            path = row["path"]
            if path in result:
                raise ValueError("composition contains duplicate artifact paths")
            result[path] = row
    return result


def verify_composition_descriptor(release: Mapping[str, Any], payload: bytes) -> None:
    """Bind a freshly acquired descriptor to the caller's validated metadata."""
    if strict_json_object(payload) != release:
        raise ValueError("composition descriptor changed after metadata validation")


def verify_embedded_native_descriptor(release: Mapping[str, Any], files: Mapping[str, bytes]) -> None:
    """Bind original native descriptor bytes to the embedded native authority."""
    native = next(item["release"] for item in release["constituents"] if item["id"] == "native")
    if strict_json_object(files[NATIVE_SIDECARS["release-set.json"]]) != native:
        raise ValueError("composition native source descriptor differs from embedded authority")


def verify_composition_subject(release: Mapping[str, Any], descriptor_payload: bytes, subject: bytes) -> None:
    """Require the exact redundant checksum subject after artifact verification."""
    if subject != composition_subject_bytes(release, descriptor_payload):
        raise ValueError("composition checksum subject differs from the parent-bound inventory")


def composition_auxiliary_subject_digest(release: Mapping[str, Any], descriptor_payload: bytes) -> str:
    """Derive a transport pin only after binding the reread descriptor bytes.

    This preserves the auxiliary helper's existing boundary: metadata admission
    belongs to its caller, so this operation also accepts partial inventories.
    """
    verify_composition_descriptor(release, descriptor_payload)
    return composition_subject_sha256(release, descriptor_payload)
