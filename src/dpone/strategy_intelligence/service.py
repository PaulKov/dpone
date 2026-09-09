from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.strategy_intelligence.advisor import StrategyAdvisor
from dpone.strategy_intelligence.manifest_reader import StrategyManifestReader
from dpone.strategy_intelligence.repair import RepairPlanService, RepairRequest


class StrategyIntelligenceService:
    """Facade for CLI/docs UX around load strategy intelligence."""

    def __init__(
        self,
        reader: StrategyManifestReader | None = None,
        advisor: StrategyAdvisor | None = None,
        repair_plans: RepairPlanService | None = None,
    ) -> None:
        self._reader = reader or StrategyManifestReader()
        self._advisor = advisor or StrategyAdvisor()
        self._repair_plans = repair_plans or RepairPlanService()

    def advise_manifest(
        self,
        path: str | Path,
        *,
        estimated_rows: int | None = None,
        changed_percent: float | None = None,
        delete_percent: float | None = None,
        cdc_available: bool | None = None,
    ) -> dict[str, Any]:
        context = self._reader.read_context(
            path,
            estimated_rows=estimated_rows,
            changed_percent=changed_percent,
            delete_percent=delete_percent,
            cdc_available=cdc_available,
        )
        decision = self._advisor.advise(context)
        repair = self._repair_plans.plan(
            RepairRequest(
                run_id="RUN_ID",
                source_type=context.source_type,
                sink_type=context.sink_type,
                strategy_mode=decision.strategy_mode,
                failed_stage="finalize",
            )
        )
        return {
            "decision": decision.to_dict(),
            "repair_commands": list(repair.commands),
            "repair_plan": repair.to_dict(),
        }
