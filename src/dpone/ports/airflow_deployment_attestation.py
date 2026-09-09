"""Ports for deployment-scoped Airflow artifact attestation."""

from __future__ import annotations

from typing import Protocol

from dpone.contracts.airflow_artifact_attestation import (
    AirflowArtifactAttestationVerification,
    AirflowArtifactExpectedSubject,
    AirflowArtifactObservedSubject,
    sha256_bytes,
)
from dpone.contracts.airflow_artifact_attestation_errors import (
    AirflowArtifactAttestationRegistryError,
    AirflowArtifactAttestationRejected,
)


class AirflowDeploymentAttestationVerifier(Protocol):
    """Verify signed claims against values observed by one consumer."""

    @property
    def policy_sha256(self) -> str: ...

    def verify(
        self,
        subject: AirflowArtifactObservedSubject,
    ) -> AirflowArtifactAttestationVerification: ...


__all__ = [
    "AirflowArtifactAttestationRegistryError",
    "AirflowArtifactAttestationRejected",
    "AirflowArtifactAttestationVerification",
    "AirflowArtifactExpectedSubject",
    "AirflowArtifactObservedSubject",
    "AirflowDeploymentAttestationVerifier",
    "sha256_bytes",
]
