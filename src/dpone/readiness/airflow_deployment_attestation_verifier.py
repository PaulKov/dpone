"""Compose registry-backed Airflow deployment attestation verification."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dpone.adapters.cosign_public_key_signature import CosignPublicKeyBlobSignatureVerifier
from dpone.readiness.airflow_artifact_trust_material import (
    load_optional_airflow_deployment_trust_material_from_files,
)
from dpone.services.airflow_artifact_attestation import (
    AirflowArtifactAttestationVerificationService,
    AirflowArtifactSignatureVerifier,
)
from dpone.services.airflow_artifact_attestation_consumer import (
    AirflowArtifactAttestationConsumer,
    AirflowArtifactTrustMaterial,
)
from dpone.services.airflow_artifact_attestation_registry import (
    AirflowArtifactAttestationRegistryReader,
)

if TYPE_CHECKING:
    from dpone.contracts.airflow_artifact_attestation import (
        AirflowArtifactAttestationVerification,
        AirflowArtifactObservedSubject,
    )
    from dpone.ports.artifact_registry import ArtifactRegistryReader


class RegistryAirflowDeploymentAttestationVerifier:
    """Verify one observed subject through an immutable registry package."""

    def __init__(
        self,
        *,
        registry: ArtifactRegistryReader,
        trust_material: AirflowArtifactTrustMaterial,
        signature_verifier: AirflowArtifactSignatureVerifier | None = None,
    ) -> None:
        self._consumer = _consumer(registry, trust_material, signature_verifier)

    @property
    def policy_sha256(self) -> str:
        return self._consumer.policy_sha256

    def verify(
        self,
        subject: AirflowArtifactObservedSubject,
    ) -> AirflowArtifactAttestationVerification:
        return self._consumer.verify(subject)


def optional_airflow_deployment_attestation_verifier(
    *,
    registry: ArtifactRegistryReader,
    policy_path: Path,
    key_root: Path,
) -> RegistryAirflowDeploymentAttestationVerifier | None:
    """Compose verification only for a deployment-level Cosign policy."""

    if not policy_path.exists():
        return None
    material = load_optional_airflow_deployment_trust_material_from_files(
        policy_path=policy_path,
        key_root=key_root,
    )
    if material is None:
        return None
    return RegistryAirflowDeploymentAttestationVerifier(
        registry=registry,
        trust_material=material,
    )


def _consumer(
    registry: ArtifactRegistryReader,
    trust_material: AirflowArtifactTrustMaterial,
    signature_verifier: AirflowArtifactSignatureVerifier | None,
) -> AirflowArtifactAttestationConsumer:
    return AirflowArtifactAttestationConsumer(
        registry=AirflowArtifactAttestationRegistryReader(registry),
        verification_service=AirflowArtifactAttestationVerificationService(
            signature_verifier=signature_verifier or CosignPublicKeyBlobSignatureVerifier()
        ),
        trust_material=trust_material,
    )


__all__ = [
    "RegistryAirflowDeploymentAttestationVerifier",
    "optional_airflow_deployment_attestation_verifier",
]
