"""Map verified init-fetch artifacts to the narrow attestation port contract."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.runtime_artifact_attestation import (
    RuntimeArtifactAttestationError,
    RuntimeArtifactAttestationSubject,
)

if TYPE_CHECKING:
    from dpone.runtime.runtime_init_fetch_plan import (
        RuntimeArtifactDescriptor,
        RuntimeInitFetchPlan,
        RuntimePayloadDescriptor,
        RuntimeWorkloadPackRef,
    )


@dataclass(frozen=True, slots=True)
class StagedRuntimeArtifact:
    """One checksum-verified artifact in the private init-fetch staging tree."""

    descriptor: RuntimeArtifactDescriptor | RuntimeWorkloadPackRef | RuntimePayloadDescriptor
    path: Path


def release_attestation_subject(
    plan: RuntimeInitFetchPlan,
    staged: tuple[StagedRuntimeArtifact, ...],
) -> RuntimeArtifactAttestationSubject:
    """Select the exact staged release-set without exposing runtime DTOs to adapters."""

    releases = tuple(item for item in staged if item.descriptor.artifact_ref == plan.release.artifact_ref)
    if len(releases) != 1:
        raise RuntimeArtifactAttestationError(
            "DPONE_ARTIFACT_ATTESTATION_SUBJECT_INVALID",
            "runtime attestation requires exactly one staged release-set",
        )
    return RuntimeArtifactAttestationSubject(
        release_id=plan.release_id,
        subject_path=releases[0].path.absolute(),
        subject_sha256=plan.release.sha256,
    )


__all__ = ["StagedRuntimeArtifact", "release_attestation_subject"]
