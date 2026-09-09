"""Core rollback service factories."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.ops.rollback import RollbackPlan, RollbackPlanService
from dpone.ops.rollback_execute import RollbackExecutionService


@dataclass(frozen=True, slots=True)
class CoreRollbackCatalog:
    """Factory catalog for rollback planning, apply and execution services."""

    @classmethod
    def default(cls) -> CoreRollbackCatalog:
        return cls()

    def rollback_plan(self, **kwargs: Any) -> RollbackPlan:
        return RollbackPlan(**kwargs)

    def rollback_plan_from_payload(self, payload: Mapping[str, Any]) -> RollbackPlan:
        return RollbackPlan(
            sink=str(payload["sink"]),
            target=str(payload["target"]),
            load_id=str(payload["load_id"]),
            strategy=str(payload.get("strategy", "restore_backup")),
            actions=tuple(payload.get("actions", ())),
        )

    def rollback_plan_service(self) -> RollbackPlanService:
        return RollbackPlanService()

    def rollback_execution(self) -> RollbackExecutionService:
        return RollbackExecutionService()


__all__ = ["CoreRollbackCatalog"]
