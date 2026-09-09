"""Physical DDL apply decisions with dependency-injected execution."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol

from dpone.readiness.physical_design_models import PhysicalDesignPlan


@dataclass(frozen=True, slots=True)
class DdlExecutionRequest:
    sink_type: str
    table: str
    sql: str
    apply_mode: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


class DdlExecutor(Protocol):
    def execute(self, request: DdlExecutionRequest) -> None: ...


@dataclass(frozen=True, slots=True)
class DdlApplyReport:
    applied: bool
    table: str
    sink_type: str
    apply_mode: str
    executed: tuple[DdlExecutionRequest, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "applied": self.applied,
            "table": self.table,
            "sink_type": self.sink_type,
            "apply_mode": self.apply_mode,
            "executed": [item.to_dict() for item in self.executed],
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
        }


class PhysicalDdlApplyService:
    """Apply only physical DDL that policy says is safe."""

    def __init__(self, executor: DdlExecutor | None = None) -> None:
        self._executor = executor

    def apply(self, plan: PhysicalDesignPlan, *, table_exists: bool) -> DdlApplyReport:
        mode = plan.options.apply
        blockers = self._blockers(plan, table_exists=table_exists)
        warnings = tuple(plan.risks)
        if blockers or mode in {"plan_only", "manual_approval"}:
            return DdlApplyReport(
                applied=False,
                table=plan.table,
                sink_type=plan.sink_type,
                apply_mode=mode,
                executed=(),
                blockers=blockers,
                warnings=warnings,
            )
        requests = tuple(
            DdlExecutionRequest(sink_type=plan.sink_type, table=plan.table, sql=sql, apply_mode=mode)
            for sql in plan.ddl
        )
        if self._executor is not None:
            for request in requests:
                self._executor.execute(request)
        return DdlApplyReport(
            applied=True,
            table=plan.table,
            sink_type=plan.sink_type,
            apply_mode=mode,
            executed=requests,
            blockers=(),
            warnings=warnings,
        )

    def _blockers(self, plan: PhysicalDesignPlan, *, table_exists: bool) -> tuple[str, ...]:
        if not plan.options.enabled:
            return ("physical_design.disabled",)
        if plan.options.mode == "off":
            return ("physical_design.mode_off",)
        if plan.options.apply == "plan_only":
            return ("physical_design.plan_only",)
        if plan.options.apply == "manual_approval":
            return ("physical_design.manual_approval_required",)
        if not table_exists:
            return ()
        if plan.options.apply == "online":
            return tuple(plan.risks)
        return ()


__all__ = ["DdlApplyReport", "DdlExecutionRequest", "DdlExecutor", "PhysicalDdlApplyService"]
