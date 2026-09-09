"""Credential readiness operational service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.managed_credentials import ManagedCredentialReadinessService


@dataclass(frozen=True, slots=True)
class ArtifactCredentialCatalog:
    """Factory catalog for managed credential readiness services."""

    @classmethod
    def default(cls) -> ArtifactCredentialCatalog:
        return cls()

    def managed_credentials_readiness(self) -> ManagedCredentialReadinessService:
        return ManagedCredentialReadinessService()
