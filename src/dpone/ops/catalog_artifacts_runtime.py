"""Runtime evidence and recovery artifact service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.observability_pack import ObservabilityPackService
from dpone.ops.reconciliation import ReconciliationService
from dpone.ops.recovery import RuntimeRecoveryPlanner
from dpone.ops.staging_evidence import StagingEvidenceService


@dataclass(frozen=True, slots=True)
class ArtifactRuntimeCatalog:
    """Factory catalog for runtime evidence, staging, reconciliation and recovery."""

    @classmethod
    def default(cls) -> ArtifactRuntimeCatalog:
        return cls()

    def observability_pack(self) -> ObservabilityPackService:
        return ObservabilityPackService()

    def reconciliation(self) -> ReconciliationService:
        return ReconciliationService()

    def runtime_recovery(self) -> RuntimeRecoveryPlanner:
        return RuntimeRecoveryPlanner()

    def staging_evidence(self) -> StagingEvidenceService:
        return StagingEvidenceService()


__all__ = ["ArtifactRuntimeCatalog"]
