"""Application service for one exact Airflow artifact-attestation decision."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from dpone.contracts.airflow_artifact_attestation_errors import (
    AirflowArtifactAttestationRejected,
)

if TYPE_CHECKING:
    from dpone.contracts.airflow_artifact_attestation import (
        AirflowArtifactAttestationPackage,
        AirflowArtifactAttestationVerification,
        AirflowArtifactExpectedSubject,
        AirflowArtifactObservedSubject,
    )
    from dpone.contracts.airflow_deployment_trust_policy import AirflowDeploymentTrustPolicy


class AirflowArtifactAttestationPackageReader(Protocol):
    """Port used by consumers to fetch one immutable deployment package."""

    def fetch(self, *, environment: str, deployment_id: str) -> AirflowArtifactAttestationPackage: ...


class AirflowArtifactAttestationVerifier(Protocol):
    """Cryptographic verification port consumed by both activation boundaries."""

    def verify(
        self,
        *,
        attestation: bytes,
        sigstore_bundle: bytes,
        policy: AirflowDeploymentTrustPolicy,
        public_keys: Mapping[str, bytes],
        expected: AirflowArtifactExpectedSubject | AirflowArtifactObservedSubject,
    ) -> AirflowArtifactAttestationVerification: ...


@dataclass(frozen=True, slots=True)
class AirflowArtifactTrustMaterial:
    """One immutable policy snapshot and its exact public-key bytes."""

    policy: AirflowDeploymentTrustPolicy
    policy_sha256: str
    public_keys: Mapping[str, bytes]


class AirflowArtifactAttestationConsumer:
    """Fetch, verify, and return the typed decision for one deployment."""

    def __init__(
        self,
        *,
        registry: AirflowArtifactAttestationPackageReader,
        verification_service: AirflowArtifactAttestationVerifier,
        trust_material: AirflowArtifactTrustMaterial,
    ) -> None:
        self._registry = registry
        self._verification_service = verification_service
        self._trust_material = trust_material

    @property
    def policy_sha256(self) -> str:
        return self._trust_material.policy_sha256

    def verify(
        self,
        expected: AirflowArtifactExpectedSubject | AirflowArtifactObservedSubject,
    ) -> AirflowArtifactAttestationVerification:
        package = self._registry.fetch(
            environment=expected.environment,
            deployment_id=expected.deployment_id,
        )
        verification = self._verification_service.verify(
            attestation=package.attestation,
            sigstore_bundle=package.sigstore_bundle,
            policy=self._trust_material.policy,
            public_keys=self._trust_material.public_keys,
            expected=expected,
        )
        if not verification.is_verified:
            raise AirflowArtifactAttestationRejected(verification)
        return verification


__all__ = [
    "AirflowArtifactAttestationConsumer",
    "AirflowArtifactAttestationPackageReader",
    "AirflowArtifactAttestationRejected",
    "AirflowArtifactAttestationVerifier",
    "AirflowArtifactTrustMaterial",
]
