"""CDC operational service factories."""

from __future__ import annotations

from dataclasses import dataclass, field

from dpone.ops.catalog_cdc_evidence import CdcEvidenceCatalog
from dpone.ops.catalog_cdc_materialization import CdcMaterializationCatalog
from dpone.ops.catalog_cdc_recovery import CdcRecoveryCatalog
from dpone.ops.catalog_cdc_runtime import CdcRuntimeCatalog
from dpone.ops.catalog_protocols import (
    ApplyService,
    CertifyService,
    CompareService,
    EvaluateService,
    ExecuteService,
    InspectService,
    MaterializeService,
    PlanService,
    RunService,
)


@dataclass(frozen=True, slots=True)
class CdcOpsCatalog:
    """Factory catalog for CDC control-plane evidence services."""

    evidence: CdcEvidenceCatalog = field(default_factory=CdcEvidenceCatalog.default)
    materializers: CdcMaterializationCatalog = field(default_factory=CdcMaterializationCatalog.default)
    recovery: CdcRecoveryCatalog = field(default_factory=CdcRecoveryCatalog.default)
    runtime: CdcRuntimeCatalog = field(default_factory=CdcRuntimeCatalog.default)

    @classmethod
    def default(cls) -> CdcOpsCatalog:
        return cls()

    def apply_certification(self) -> CertifyService:
        return self.evidence.apply_certification()

    def handoff(self) -> EvaluateService:
        return self.evidence.handoff()

    def materialization(self) -> MaterializeService:
        return self.materializers.materialization()

    def typed_materialization(self) -> MaterializeService:
        return self.materializers.typed_materialization()

    def observability_evidence(self) -> EvaluateService:
        return self.evidence.observability_evidence()

    def promotion_gate(self) -> EvaluateService:
        return self.evidence.promotion_gate()

    def recovery_evidence(self) -> EvaluateService:
        return self.evidence.recovery_evidence()

    def quarantine_inspection(self) -> InspectService:
        return self.recovery.quarantine_inspection()

    def compare_repair(self) -> CompareService:
        return self.recovery.compare_repair()

    def repair_execution(self) -> ExecuteService:
        return self.recovery.repair_execution()

    def retention_check(self) -> EvaluateService:
        return self.recovery.retention_check()

    def resync_plan(self) -> PlanService:
        return self.recovery.resync_plan()

    def resync_execution(self) -> ExecuteService:
        return self.recovery.resync_execution()

    def replay_execution(self) -> ExecuteService:
        return self.recovery.replay_execution()

    def runtime_run(self) -> RunService:
        return self.runtime.runtime_run()

    def schema_evolution_evidence(self) -> EvaluateService:
        return self.evidence.schema_evolution_evidence()

    def schema_apply(self) -> ApplyService:
        return self.recovery.schema_apply()
