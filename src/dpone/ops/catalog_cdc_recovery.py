"""CDC quarantine, replay and schema-apply service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.cdc.compare_repair import CdcCompareRepairOpsService, CdcRepairExecutionOpsService
from dpone.ops.cdc.quarantine import CdcQuarantineInspectionService
from dpone.ops.cdc.replay_execute import CdcReplayExecutionOpsService
from dpone.ops.cdc.retention_resync import (
    CdcResyncExecutionOpsService,
    CdcResyncPlanOpsService,
    CdcRetentionCheckOpsService,
)
from dpone.ops.cdc.schema_apply import CdcSchemaEvolutionApplyService


@dataclass(frozen=True, slots=True)
class CdcRecoveryCatalog:
    """Factory catalog for CDC recovery, replay and schema-apply services."""

    @classmethod
    def default(cls) -> CdcRecoveryCatalog:
        return cls()

    def quarantine_inspection(self) -> CdcQuarantineInspectionService:
        return CdcQuarantineInspectionService()

    def compare_repair(self) -> CdcCompareRepairOpsService:
        return CdcCompareRepairOpsService()

    def repair_execution(self) -> CdcRepairExecutionOpsService:
        return CdcRepairExecutionOpsService()

    def retention_check(self) -> CdcRetentionCheckOpsService:
        return CdcRetentionCheckOpsService()

    def resync_plan(self) -> CdcResyncPlanOpsService:
        return CdcResyncPlanOpsService()

    def resync_execution(self) -> CdcResyncExecutionOpsService:
        return CdcResyncExecutionOpsService()

    def replay_execution(self) -> CdcReplayExecutionOpsService:
        return CdcReplayExecutionOpsService()

    def schema_apply(self) -> CdcSchemaEvolutionApplyService:
        return CdcSchemaEvolutionApplyService()


__all__ = ["CdcRecoveryCatalog"]
