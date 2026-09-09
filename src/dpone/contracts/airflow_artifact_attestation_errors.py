"""Typed, redacted failures shared by Airflow artifact trust boundaries."""

from __future__ import annotations

from dpone.contracts.airflow_artifact_attestation import (
    AirflowArtifactAttestationVerification,
)


class AirflowArtifactAttestationRejected(RuntimeError):
    """Carry one stable verification decision across application boundaries."""

    def __init__(self, verification: AirflowArtifactAttestationVerification) -> None:
        super().__init__(verification.message)
        self.verification = verification


class AirflowArtifactAttestationRegistryError(RuntimeError):
    """Carry a stable package-read/write code without backend payloads."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


__all__ = [
    "AirflowArtifactAttestationRegistryError",
    "AirflowArtifactAttestationRejected",
]
