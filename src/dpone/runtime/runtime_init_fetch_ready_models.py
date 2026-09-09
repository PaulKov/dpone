"""Value objects for runtime init-fetch readiness evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

RUNTIME_FETCH_READY_SCHEMA = "dpone.runtime-fetch-ready.v1"
RUNTIME_FETCH_READY_SCHEMA_V2 = "dpone.runtime-fetch-ready.v2"
RUNTIME_FETCH_READY_NAME = "runtime-fetch-ready.json"
ATTESTATION_REQUIREMENT_OPTIONAL = "optional"
ATTESTATION_REQUIREMENT_REQUIRED = "required"
ATTESTATION_REQUIREMENTS = frozenset(
    {
        ATTESTATION_REQUIREMENT_OPTIONAL,
        ATTESTATION_REQUIREMENT_REQUIRED,
    }
)


@dataclass(frozen=True, slots=True)
class ReadyArtifact:
    """One verified artifact exposed through a relative runtime locator."""

    artifact_ref: str
    locator: str
    sha256: str
    bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_ref": self.artifact_ref,
            "locator": self.locator,
            "sha256": self.sha256,
            "bytes": self.bytes,
        }


@dataclass(frozen=True, slots=True)
class RuntimeFetchReady:
    """Final init-container commit record consumed by the base launcher."""

    plan_sha256: str
    release_id: str
    deployment_id: str
    runtime_image_digest: str
    trust_tier: str
    artifact_registry_ref: str
    registry_config_sha256: str
    trust_policy_sha256: str | None
    workload_id: str
    pack_fingerprint: str
    release: ReadyArtifact
    deployment: ReadyArtifact
    workload_pack: ReadyArtifact
    checksum_status: str
    effective_attestation_requirement: str
    attestation_status: str
    attestation_decision_sha256: str
    runtime_payload_sha256: str
    artifact_attestation_subject_kind: str | None = None
    artifact_attestation_backend: str | None = None
    artifact_attestation_id: str | None = None
    artifact_attestation_verification_sha256: str | None = None
    artifact_attestation_observed_claims: tuple[str, ...] = ()
    artifact_attestation_unobserved_claims: tuple[str, ...] = ()
    schema: str = RUNTIME_FETCH_READY_SCHEMA

    def to_dict(self) -> dict[str, Any]:
        verification: dict[str, Any] = {
            "checksums": self.checksum_status,
            "effective_attestation_requirement": self.effective_attestation_requirement,
            "attestations": self.attestation_status,
            "attestation_decision_sha256": self.attestation_decision_sha256,
            "runtime_payload_sha256": self.runtime_payload_sha256,
        }
        if self.schema == RUNTIME_FETCH_READY_SCHEMA_V2:
            verification["artifact_attestation"] = {
                "subject_kind": self.artifact_attestation_subject_kind,
                "backend": self.artifact_attestation_backend,
                "attestation_id": self.artifact_attestation_id,
                "verification_sha256": self.artifact_attestation_verification_sha256,
                "observed_claims": list(self.artifact_attestation_observed_claims),
                "unobserved_claims": list(self.artifact_attestation_unobserved_claims),
            }
        return {
            "schema": self.schema,
            "plan_sha256": self.plan_sha256,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "runtime_image_digest": self.runtime_image_digest,
            "trust_tier": self.trust_tier,
            "artifact_registry_ref": self.artifact_registry_ref,
            "registry_config_sha256": self.registry_config_sha256,
            "trust_policy_sha256": self.trust_policy_sha256,
            "workload_id": self.workload_id,
            "pack_fingerprint": self.pack_fingerprint,
            "artifacts": {
                "release": self.release.to_dict(),
                "deployment": self.deployment.to_dict(),
                "workload_pack": self.workload_pack.to_dict(),
            },
            "verification": verification,
        }


__all__ = [
    "ATTESTATION_REQUIREMENTS",
    "ATTESTATION_REQUIREMENT_OPTIONAL",
    "ATTESTATION_REQUIREMENT_REQUIRED",
    "ReadyArtifact",
    "RUNTIME_FETCH_READY_NAME",
    "RUNTIME_FETCH_READY_SCHEMA",
    "RUNTIME_FETCH_READY_SCHEMA_V2",
    "RuntimeFetchReady",
]
