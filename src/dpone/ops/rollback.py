"""Rollback planning contracts for target-native restore operations."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class RollbackAction:
    kind: str
    sql: str
    risk: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RollbackPlan:
    sink: str
    target: str
    load_id: str
    strategy: str
    actions: tuple[RollbackAction, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "sink": self.sink,
            "target": self.target,
            "load_id": self.load_id,
            "strategy": self.strategy,
            "actions": [item.to_dict() for item in self.actions],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = ["# dpone rollback plan", "", f"- Target: `{self.target}`", f"- Load ID: `{self.load_id}`", ""]
        lines.extend(f"1. `{action.kind}`: `{action.sql}` Risk: {action.risk}" for action in self.actions)
        return "\n".join(lines) + "\n"


@dataclass(frozen=True, slots=True)
class RollbackApplyResult:
    applied: bool
    planned_actions: int
    message: str


class RollbackPlanService:
    """Builds safe rollback plans; execution remains explicit with --yes."""

    def plan(self, *, sink: str, target: str, load_id: str, strategy: str) -> RollbackPlan:
        if sink == "mssql":
            actions = (
                RollbackAction("validate_backup", f"SELECT OBJECT_ID('{target}__dpone_backup_{load_id[:8]}')", "low"),
                RollbackAction(
                    "swap_backup",
                    f"EXEC sp_rename N'{target}', N'{target}__failed_{load_id[:8]}'; "
                    f"EXEC sp_rename N'{target}__dpone_backup_{load_id[:8]}', N'{target.split('.')[-1]}'",
                    "medium",
                ),
            )
        elif sink == "postgres":
            actions = (
                RollbackAction("validate_backup", f"SELECT to_regclass('{target}__dpone_backup_{load_id[:8]}')", "low"),
                RollbackAction(
                    "swap_backup",
                    f"ALTER TABLE {target} RENAME TO {target.split('.')[-1]}__failed_{load_id[:8]}; "
                    f"ALTER TABLE {target}__dpone_backup_{load_id[:8]} RENAME TO {target.split('.')[-1]}",
                    "medium",
                ),
            )
        elif sink == "clickhouse":
            actions = (
                RollbackAction("validate_shadow", f"EXISTS TABLE {target}__dpone_backup_{load_id[:8]}", "low"),
                RollbackAction(
                    "exchange_backup", f"EXCHANGE TABLES {target} AND {target}__dpone_backup_{load_id[:8]}", "medium"
                ),
            )
        elif sink == "bigquery":
            actions = (
                RollbackAction("validate_snapshot", f"CHECK TABLE `{target}__dpone_backup_{load_id[:8]}`", "low"),
                RollbackAction(
                    "copy_backup",
                    f"CREATE OR REPLACE TABLE `{target}` AS SELECT * FROM `{target}__dpone_backup_{load_id[:8]}`",
                    "medium",
                ),
            )
        else:
            raise ValueError(f"Rollback is not supported for sink: {sink}")
        return RollbackPlan(sink=sink, target=target, load_id=load_id, strategy=strategy, actions=actions)

    def apply(self, plan: RollbackPlan, *, yes: bool = False) -> RollbackApplyResult:
        if not yes:
            return RollbackApplyResult(False, len(plan.actions), "Preview only. Re-run with --yes to apply.")
        return RollbackApplyResult(True, len(plan.actions), "Rollback actions accepted for execution by caller.")
