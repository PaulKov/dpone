"""CDC evidence and certification service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.cdc.apply import CdcApplyCertificationService
from dpone.ops.cdc.handoff import SnapshotCdcHandoffService
from dpone.ops.cdc.observability import CdcObservabilityEvidenceService
from dpone.ops.cdc.promotion import CdcPromotionGateService
from dpone.ops.cdc.recovery import CdcRecoveryEvidenceService
from dpone.ops.cdc.schema_evolution import CdcSchemaEvolutionEvidenceService


@dataclass(frozen=True, slots=True)
class CdcEvidenceCatalog:
    """Factory catalog for CDC handoff, certification and evidence gates."""

    @classmethod
    def default(cls) -> CdcEvidenceCatalog:
        return cls()

    def apply_certification(self) -> CdcApplyCertificationService:
        return CdcApplyCertificationService()

    def handoff(self) -> SnapshotCdcHandoffService:
        return SnapshotCdcHandoffService()

    def observability_evidence(self) -> CdcObservabilityEvidenceService:
        return CdcObservabilityEvidenceService()

    def promotion_gate(self) -> CdcPromotionGateService:
        return CdcPromotionGateService()

    def recovery_evidence(self) -> CdcRecoveryEvidenceService:
        return CdcRecoveryEvidenceService()

    def schema_evolution_evidence(self) -> CdcSchemaEvolutionEvidenceService:
        return CdcSchemaEvolutionEvidenceService()


__all__ = ["CdcEvidenceCatalog"]
