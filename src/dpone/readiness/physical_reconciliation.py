"""Physical design drift detection and reconciliation planning."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any, Literal

from dpone.readiness.physical_apply import DdlExecutionRequest, DdlExecutor
from dpone.readiness.physical_design_models import PhysicalReconciliationOptions, approval_blockers
from dpone.readiness.physical_reconciliation_diff import (
    PhysicalDriftChange,
    column_changes,
    extra_table_setting_warnings,
    key_changes,
    scalar_changes,
    table_setting_changes,
)
from dpone.readiness.physical_state import PhysicalTableState, TargetPhysicalMigrationDialect

ChangeDecision = Literal["online_safe", "blocking", "shadow_required", "schema_evolution_owned", "warning"]


@dataclass(frozen=True, slots=True)
class PhysicalReconciliationAction:
    change: PhysicalDriftChange
    decision: ChangeDecision
    ddl: tuple[str, ...] = ()
    blocker: str | None = None
    warning: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "change": self.change.to_dict(),
            "decision": self.decision,
            "ddl": list(self.ddl),
        }
        if self.blocker:
            payload["blocker"] = self.blocker
        if self.warning:
            payload["warning"] = self.warning
        return payload


@dataclass(frozen=True, slots=True)
class PhysicalReconciliationPlan:
    sink_type: str
    table: str
    mode: str
    changes: tuple[PhysicalDriftChange, ...]
    actions: tuple[PhysicalReconciliationAction, ...]
    ddl: tuple[str, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    recommendations: tuple[str, ...]
    execution_apply_mode: str = "online"
    applied: bool = False
    executed: tuple[Any, ...] = ()

    @property
    def has_drift(self) -> bool:
        return bool(self.changes)

    def with_execution(self, *, applied: bool, executed: Sequence[DdlExecutionRequest]) -> PhysicalReconciliationPlan:
        return PhysicalReconciliationPlan(
            sink_type=self.sink_type,
            table=self.table,
            mode=self.mode,
            changes=self.changes,
            actions=self.actions,
            ddl=self.ddl,
            blockers=self.blockers,
            warnings=self.warnings,
            recommendations=self.recommendations,
            execution_apply_mode=self.execution_apply_mode,
            applied=applied,
            executed=tuple(executed),
        )

    def with_blocker(self, blocker: str) -> PhysicalReconciliationPlan:
        return PhysicalReconciliationPlan(
            sink_type=self.sink_type,
            table=self.table,
            mode=self.mode,
            changes=self.changes,
            actions=self.actions,
            ddl=self.ddl,
            blockers=(*self.blockers, blocker),
            warnings=self.warnings,
            recommendations=self.recommendations,
            execution_apply_mode=self.execution_apply_mode,
            applied=False,
            executed=self.executed,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "sink_type": self.sink_type,
            "table": self.table,
            "mode": self.mode,
            "has_drift": self.has_drift,
            "applied": self.applied,
            "changes": [change.to_dict() for change in self.changes],
            "actions": [action.to_dict() for action in self.actions],
            "ddl": list(self.ddl),
            "executed": [request.to_dict() for request in self.executed],
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "recommendations": list(self.recommendations),
        }


class PhysicalDesignDriftDetector:
    """Pure desired-vs-actual physical target diff."""

    def detect(self, desired: PhysicalTableState, actual: PhysicalTableState) -> tuple[PhysicalDriftChange, ...]:
        changes: list[PhysicalDriftChange] = []
        changes.extend(scalar_changes("engine", desired.engine, actual.engine))
        changes.extend(scalar_changes("partition_by", desired.partition_by, actual.partition_by))
        changes.extend(key_changes("order_by", desired.order_by, actual.order_by))
        expected_primary_key = desired.primary_key
        compare_primary_key = bool(expected_primary_key or actual.primary_key)
        if desired.sink_type == "clickhouse":
            compare_primary_key = bool(actual.primary_key)
            if actual.primary_key and not expected_primary_key:
                expected_primary_key = desired.order_by
        if compare_primary_key:
            changes.extend(key_changes("primary_key", expected_primary_key, actual.primary_key))
        changes.extend(scalar_changes("ttl", desired.ttl, actual.ttl))
        changes.extend(column_changes(desired, actual))
        changes.extend(table_setting_changes(desired, actual))
        return tuple(changes)


class PhysicalDesignChangeClassifier:
    """Classify drift without rendering SQL."""

    def classify(self, change: PhysicalDriftChange) -> ChangeDecision:
        if change.change_type == "table_setting":
            return "online_safe"
        if change.change_type == "extra_table_setting":
            return "warning"
        if change.change_type.startswith("column_"):
            return "schema_evolution_owned"
        if change.change_type in {"engine", "partition_by", "order_by", "primary_key", "ttl"}:
            return "shadow_required"
        return "blocking"


class PhysicalDesignReconciler:
    """Build an explainable physical reconciliation plan."""

    def __init__(
        self,
        *,
        detector: PhysicalDesignDriftDetector | None = None,
        classifier: PhysicalDesignChangeClassifier | None = None,
    ) -> None:
        self._detector = detector or PhysicalDesignDriftDetector()
        self._classifier = classifier or PhysicalDesignChangeClassifier()

    def reconcile(
        self,
        *,
        desired: PhysicalTableState,
        actual: PhysicalTableState,
        options: PhysicalReconciliationOptions,
        dialect: TargetPhysicalMigrationDialect,
        execution_apply_mode: str = "online",
    ) -> PhysicalReconciliationPlan:
        changes = self._detector.detect(desired, actual)
        actions = tuple(
            self._action(change, options=options, table=desired.table, dialect=dialect) for change in changes
        )
        suppressor = getattr(dialect, "suppress_table_setting_ddl", None)
        changed_settings = tuple(_setting_name(change) for change in changes if change.change_type == "table_setting")
        if callable(suppressor) and suppressor(changed_settings):
            actions = tuple(replace(action, ddl=()) if action.ddl else action for action in actions)
        warnings = (
            *(action.warning for action in actions if action.warning),
            *extra_table_setting_warnings(desired, actual),
        )
        return PhysicalReconciliationPlan(
            sink_type=desired.sink_type,
            table=desired.table,
            mode=options.mode,
            changes=changes,
            actions=actions,
            ddl=tuple(sql for action in actions for sql in action.ddl),
            blockers=tuple(action.blocker for action in actions if action.blocker),
            warnings=warnings,
            recommendations=tuple(dict.fromkeys(change.recommendation for change in changes if change.recommendation)),
            execution_apply_mode=execution_apply_mode,
        )

    def _action(
        self,
        change: PhysicalDriftChange,
        *,
        options: PhysicalReconciliationOptions,
        table: str,
        dialect: TargetPhysicalMigrationDialect,
    ) -> PhysicalReconciliationAction:
        decision = self._classifier.classify(change)
        if decision == "online_safe" and not dialect.is_online_safe_table_setting(_setting_name(change)):
            decision = "blocking"
        if decision == "warning":
            return PhysicalReconciliationAction(change=change, decision=decision, warning=_warning(change))
        if decision == "schema_evolution_owned":
            return PhysicalReconciliationAction(change=change, decision=decision)
        if options.mode == "block":
            return PhysicalReconciliationAction(change=change, decision=decision, blocker=_blocker(change, decision))
        if options.mode == "plan_only":
            return PhysicalReconciliationAction(
                change=change,
                decision=decision,
                ddl=_ddl(change, table=table, dialect=dialect) if decision == "online_safe" else (),
                blocker=_blocker(change, decision),
            )
        if options.mode == "auto_safe" and decision == "online_safe":
            return PhysicalReconciliationAction(
                change=change,
                decision=decision,
                ddl=_ddl(change, table=table, dialect=dialect),
            )
        if options.mode == "safe_window" and decision == "blocking":
            setting = _setting_name(change)
            if dialect.is_safe_window_table_setting(setting):
                risk = f"table_settings.{setting}"
                blockers = approval_blockers(options.approval, risk=risk, table=table)
                ddl = _ddl(change, table=table, dialect=dialect)
                if blockers:
                    return PhysicalReconciliationAction(
                        change=change,
                        decision=decision,
                        ddl=ddl,
                        blocker=blockers[0],
                    )
                return PhysicalReconciliationAction(change=change, decision=decision, ddl=ddl)
        return PhysicalReconciliationAction(change=change, decision=decision, blocker=_blocker(change, decision))


class PhysicalReconciliationApplyService:
    """Execute only safe reconciliation DDL already selected by the plan."""

    def __init__(self, executor: DdlExecutor | None = None) -> None:
        self._executor = executor

    def apply(self, plan: PhysicalReconciliationPlan) -> PhysicalReconciliationPlan:
        if plan.blockers or not plan.ddl:
            return plan
        if plan.execution_apply_mode == "plan_only":
            return plan.with_blocker("physical_design.plan_only")
        if plan.execution_apply_mode == "manual_approval":
            return plan.with_blocker("physical_design.manual_approval_required")
        if self._executor is None:
            return plan.with_blocker("physical_reconciliation.executor_missing")
        requests = tuple(
            DdlExecutionRequest(
                sink_type=plan.sink_type,
                table=plan.table,
                sql=sql,
                apply_mode=plan.execution_apply_mode,
            )
            for sql in plan.ddl
        )
        for request in requests:
            self._executor.execute(request)
        return plan.with_execution(applied=True, executed=requests)


def _ddl(
    change: PhysicalDriftChange,
    *,
    table: str,
    dialect: TargetPhysicalMigrationDialect,
) -> tuple[str, ...]:
    setting = change.path.removeprefix("table_settings.")
    return (dialect.render_table_setting_update(table=table, setting=setting, value=change.desired),)


def _setting_name(change: PhysicalDriftChange) -> str:
    return change.path.removeprefix("table_settings.")


def _blocker(change: PhysicalDriftChange, decision: ChangeDecision) -> str:
    return f"physical_design.{decision}:{change.path}"


def _warning(change: PhysicalDriftChange) -> str:
    return f"physical_design.warning:{change.path}"


__all__ = [
    "PhysicalDesignChangeClassifier",
    "PhysicalDesignDriftDetector",
    "PhysicalDesignReconciler",
    "PhysicalDriftChange",
    "PhysicalReconciliationAction",
    "PhysicalReconciliationApplyService",
    "PhysicalReconciliationPlan",
]
