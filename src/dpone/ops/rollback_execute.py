"""Safe rollback execution orchestration."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from dpone.ops.rollback import RollbackPlanService


@dataclass(frozen=True, slots=True)
class RollbackExecutionReport:
    sink: str
    target: str
    load_id: str
    strategy: str
    lock_id: str
    dry_run: bool
    applied: bool
    backup_validated: bool
    planned_actions: int
    message: str
    post_actions: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        mode = "apply" if self.applied else "dry-run"
        lines = [
            "# dpone ops rollback execute",
            "",
            f"- Mode: `{mode}`",
            f"- Sink: `{self.sink}`",
            f"- Target: `{self.target}`",
            f"- Load ID: `{self.load_id}`",
            f"- Strategy: `{self.strategy}`",
            f"- Lock ID: `{self.lock_id}`",
            f"- Backup validated: `{self.backup_validated}`",
            f"- Planned actions: `{self.planned_actions}`",
            f"- Message: {self.message}",
            "",
            "## Post actions",
            "",
        ]
        lines.extend(f"- {action}" for action in self.post_actions)
        return "\n".join(lines) + "\n"


class RollbackExecutionService:
    """Coordinates guarded rollback preview/apply flows."""

    def __init__(self, planner: RollbackPlanService | None = None) -> None:
        self._planner = planner or RollbackPlanService()

    def execute(
        self,
        *,
        sink: str,
        target: str,
        load_id: str,
        strategy: str,
        yes: bool = False,
        require_backup: bool = False,
    ) -> RollbackExecutionReport:
        plan = self._planner.plan(sink=sink, target=target, load_id=load_id, strategy=strategy)
        backup_validated = self._has_backup_validation(plan.actions)
        if require_backup and not backup_validated:
            return self._report(
                sink=sink,
                target=target,
                load_id=load_id,
                strategy=strategy,
                dry_run=not yes,
                applied=False,
                backup_validated=False,
                planned_actions=len(plan.actions),
                message="Backup validation step is required but missing from the rollback plan.",
            )
        result = self._planner.apply(plan, yes=yes)
        return self._report(
            sink=sink,
            target=target,
            load_id=load_id,
            strategy=strategy,
            dry_run=not yes,
            applied=result.applied,
            backup_validated=backup_validated,
            planned_actions=result.planned_actions,
            message=result.message,
        )

    def _report(
        self,
        *,
        sink: str,
        target: str,
        load_id: str,
        strategy: str,
        dry_run: bool,
        applied: bool,
        backup_validated: bool,
        planned_actions: int,
        message: str,
    ) -> RollbackExecutionReport:
        return RollbackExecutionReport(
            sink=sink,
            target=target,
            load_id=load_id,
            strategy=strategy,
            lock_id=self._lock_id(sink=sink, target=target, load_id=load_id),
            dry_run=dry_run,
            applied=applied,
            backup_validated=backup_validated,
            planned_actions=planned_actions,
            message=message,
            post_actions=self._post_actions(target),
        )

    @staticmethod
    def _has_backup_validation(actions: object) -> bool:
        return any(getattr(action, "kind", "") == "validate_backup" for action in actions)

    @staticmethod
    def _lock_id(*, sink: str, target: str, load_id: str) -> str:
        return f"rollback:{sink}:{target}:{load_id[:8]}"

    @staticmethod
    def _post_actions(target: str) -> tuple[str, ...]:
        return (
            "Run `dpone ops diff`",
            "Run `dpone ops contract-check` for critical data contracts.",
            f"Inspect downstream consumers of `{target}` before replaying state.",
            "Attach rollback execution output to the incident pack.",
        )
