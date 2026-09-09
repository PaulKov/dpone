"""CDC schema apply value objects."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from dpone.ops.cdc.schema_evolution_models import CdcSchemaChangeEvent

SCHEMA_APPLY_VERSION = "dpone.cdc_schema_apply.v1"
SchemaApplyMode = Literal["dry_run", "apply"]


@dataclass(frozen=True, slots=True)
class CdcSchemaApplyPolicy:
    """Safety policy for target-side CDC schema apply."""

    mode: SchemaApplyMode = "dry_run"
    require_approval: bool = True
    apply_backfill: bool = True
    force_nullable_additions: bool = True

    def __post_init__(self) -> None:
        if self.mode not in {"dry_run", "apply"}:
            raise ValueError("mode must be dry_run or apply")

    def to_dict(self) -> dict[str, object]:
        return {
            "apply_backfill": self.apply_backfill,
            "force_nullable_additions": self.force_nullable_additions,
            "mode": self.mode,
            "require_approval": self.require_approval,
        }


@dataclass(frozen=True, slots=True)
class CdcSchemaApplyPlan:
    """Target DDL plan for one CDC schema change."""

    change: CdcSchemaChangeEvent
    target_dataset: str
    operation: str
    ddl: str
    backfill_sql: str
    source_type: str
    target_type: str
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...] = tuple()

    def to_dict(self) -> dict[str, object]:
        return {
            "backfill_sql": self.backfill_sql,
            "blockers": list(self.blockers),
            "change": self.change.to_dict(),
            "ddl": self.ddl,
            "operation": self.operation,
            "passed": self.passed,
            "source_type": self.source_type,
            "target_dataset": self.target_dataset,
            "target_type": self.target_type,
            "warnings": list(self.warnings),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


@dataclass(frozen=True, slots=True)
class CdcSchemaApplyResult:
    """Execution result for one CDC schema apply attempt."""

    mode: SchemaApplyMode
    applied: bool
    ddl_executed: bool
    backfill_executed: bool
    typed_refresh: Mapping[str, object]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "applied": self.applied,
            "backfill_executed": self.backfill_executed,
            "blockers": list(self.blockers),
            "ddl_executed": self.ddl_executed,
            "mode": self.mode,
            "typed_refresh": dict(self.typed_refresh),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class CdcSchemaApplyReport:
    """Stable JSON/Markdown contract for CDC schema apply."""

    plan: CdcSchemaApplyPlan
    result: CdcSchemaApplyResult
    policy: CdcSchemaApplyPolicy
    output_dir: str
    plan_json_path: str
    result_json_path: str
    markdown_path: str

    @property
    def mode(self) -> SchemaApplyMode:
        return self.policy.mode

    @property
    def passed(self) -> bool:
        return self.plan.passed and not self.result.blockers

    @property
    def blockers(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys((*self.plan.blockers, *self.result.blockers)))

    @property
    def warnings(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys((*self.plan.warnings, *self.result.warnings)))

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_APPLY_VERSION,
            "applied": self.result.applied,
            "blockers": list(self.blockers),
            "change": self.plan.change.to_dict(),
            "mode": self.mode,
            "passed": self.passed,
            "plan": self.plan.to_dict(),
            "policy": self.policy.to_dict(),
            "result": self.result.to_dict(),
            "target_dataset": self.plan.target_dataset,
            "typed_refresh": dict(self.result.typed_refresh),
            "warnings": list(self.warnings),
            "output_dir": self.output_dir,
            "plan_json_path": self.plan_json_path,
            "result_json_path": self.result_json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# CDC schema apply",
            "",
            f"- Target: `{self.plan.target_dataset}`",
            f"- Mode: `{self.mode}`",
            f"- Operation: `{self.plan.operation}`",
            f"- Passed: `{self.passed}`",
            f"- Applied: `{self.result.applied}`",
            "",
            "## DDL",
            "",
            "```sql",
            self.plan.ddl or "-- no DDL",
            "```",
            "",
            "## Blockers",
            "",
        ]
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings) if self.warnings else lines.append("- none")
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.plan_json_path).write_text(self.plan.to_json(), encoding="utf-8")
        Path(self.result_json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


def load_schema_apply_change(path: str | Path) -> CdcSchemaChangeEvent:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("CDC schema apply input must be a JSON object")
    change = payload.get("change")
    if not isinstance(change, Mapping):
        raise ValueError("CDC schema apply input must include a `change` object")
    return CdcSchemaChangeEvent.from_dict(change)


def mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise ValueError("CDC schema apply value must be a JSON object")


__all__ = [
    "CdcSchemaApplyPlan",
    "CdcSchemaApplyPolicy",
    "CdcSchemaApplyReport",
    "CdcSchemaApplyResult",
    "SCHEMA_APPLY_VERSION",
    "SchemaApplyMode",
    "load_schema_apply_change",
]
