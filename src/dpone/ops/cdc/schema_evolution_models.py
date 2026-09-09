"""CDC schema evolution evidence value objects."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.ops.cdc.models import CdcStreamKey

SCHEMA_VERSION = "dpone.cdc_schema_evolution_evidence.v1"

SCHEMA_EVOLUTION_EVIDENCE_DOMAINS: tuple[str, ...] = (
    "cdc_schema_change_capture",
    "cdc_schema_compatibility",
    "cdc_type_widening_safety",
    "cdc_target_ddl_dry_run",
    "cdc_backfill_requirement",
    "cdc_breaking_change_gate",
    "cdc_offset_schema_ordering",
)

CDC_SCHEMA_CHANGE_KINDS: tuple[str, ...] = (
    "add_column",
    "drop_column",
    "rename_column",
    "alter_type",
    "nullable_changed",
    "default_changed",
)


@dataclass(frozen=True, slots=True)
class CdcSchemaChangeEvent:
    """Normalized CDC schema-change event captured from the source stream."""

    change_id: str
    kind: str
    captured_at: str
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    old_type: str
    new_type: str
    old_nullable: bool
    new_nullable: bool
    source_offset: str
    breaking: bool

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> CdcSchemaChangeEvent:
        return cls(
            change_id=str(payload.get("change_id", "")).strip(),
            kind=str(payload.get("kind", "")).strip().lower().replace("-", "_"),
            captured_at=str(payload.get("captured_at", "")).strip(),
            source_table=str(payload.get("source_table", "")).strip(),
            source_column=str(payload.get("source_column", "")).strip(),
            target_table=str(payload.get("target_table", "")).strip(),
            target_column=str(payload.get("target_column", payload.get("source_column", ""))).strip(),
            old_type=str(payload.get("old_type", "")).strip(),
            new_type=str(payload.get("new_type", "")).strip(),
            old_nullable=_bool(payload.get("old_nullable", True)),
            new_nullable=_bool(payload.get("new_nullable", True)),
            source_offset=str(payload.get("source_offset", "")).strip(),
            breaking=_bool(payload.get("breaking")),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "change_id": self.change_id,
            "kind": self.kind,
            "captured_at": self.captured_at,
            "source_table": self.source_table,
            "source_column": self.source_column,
            "target_table": self.target_table,
            "target_column": self.target_column,
            "old_type": self.old_type,
            "new_type": self.new_type,
            "old_nullable": self.old_nullable,
            "new_nullable": self.new_nullable,
            "source_offset": self.source_offset,
            "breaking": self.breaking,
        }


@dataclass(frozen=True, slots=True)
class CdcSchemaEvolutionPlan:
    """Planned sink-side handling for one captured CDC schema change."""

    sink_impact: str
    compatibility_level: str
    target_ddl_preview: str
    ddl_dry_run_passed: bool
    backfill_required: bool
    backfill_plan: str
    type_widening_safe: bool
    offset_schema_ordering_safe: bool
    approved_by: tuple[str, ...]

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> CdcSchemaEvolutionPlan:
        return cls(
            sink_impact=str(payload.get("sink_impact", "")).strip(),
            compatibility_level=str(payload.get("compatibility_level", "")).strip().lower(),
            target_ddl_preview=str(payload.get("target_ddl_preview", "")).strip(),
            ddl_dry_run_passed=_bool(payload.get("ddl_dry_run_passed")),
            backfill_required=_bool(payload.get("backfill_required")),
            backfill_plan=str(payload.get("backfill_plan", "")).strip(),
            type_widening_safe=_bool(payload.get("type_widening_safe", True)),
            offset_schema_ordering_safe=_bool(payload.get("offset_schema_ordering_safe", True)),
            approved_by=tuple(str(item) for item in _sequence(payload.get("approved_by"))),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "sink_impact": self.sink_impact,
            "compatibility_level": self.compatibility_level,
            "target_ddl_preview": self.target_ddl_preview,
            "ddl_dry_run_passed": self.ddl_dry_run_passed,
            "backfill_required": self.backfill_required,
            "backfill_plan": self.backfill_plan,
            "type_widening_safe": self.type_widening_safe,
            "offset_schema_ordering_safe": self.offset_schema_ordering_safe,
            "approved_by": list(self.approved_by),
        }


@dataclass(frozen=True, slots=True)
class CdcSchemaEvolutionPolicy:
    """Safety policy for CDC schema evolution evidence."""

    allowed_compatibility_levels: tuple[str, ...] = ("compatible", "backward_compatible")
    require_ddl_dry_run: bool = True
    require_breaking_approval: bool = True
    require_offset_schema_ordering: bool = True

    @classmethod
    def default(cls) -> CdcSchemaEvolutionPolicy:
        return cls()

    @classmethod
    def from_path(cls, path: str | Path) -> CdcSchemaEvolutionPolicy:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("CDC schema evolution policy must be a JSON object")
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> CdcSchemaEvolutionPolicy:
        default = cls.default()
        levels = tuple(str(item) for item in _sequence(payload.get("allowed_compatibility_levels")))
        return cls(
            allowed_compatibility_levels=levels or default.allowed_compatibility_levels,
            require_ddl_dry_run=_bool(payload.get("require_ddl_dry_run", default.require_ddl_dry_run)),
            require_breaking_approval=_bool(
                payload.get("require_breaking_approval", default.require_breaking_approval)
            ),
            require_offset_schema_ordering=_bool(
                payload.get("require_offset_schema_ordering", default.require_offset_schema_ordering)
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "allowed_compatibility_levels": list(self.allowed_compatibility_levels),
            "require_ddl_dry_run": self.require_ddl_dry_run,
            "require_breaking_approval": self.require_breaking_approval,
            "require_offset_schema_ordering": self.require_offset_schema_ordering,
        }


@dataclass(frozen=True, slots=True)
class CdcSchemaEvolutionEvidenceItem:
    """Normalized evidence item emitted by one CDC schema evolution check."""

    name: str
    passed: bool
    summary: str
    blockers: tuple[str, ...]
    details: Mapping[str, object]
    policy: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "kind": "cdc_schema_evolution",
            "passed": self.passed,
            "summary": self.summary,
            "blockers": list(self.blockers),
            "details": dict(self.details),
            "policy": dict(self.policy),
        }


@dataclass(frozen=True, slots=True)
class CdcSchemaEvolutionDecision:
    """Pure go/no-go decision for CDC schema evolution evidence."""

    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class CdcSchemaEvolutionReport:
    """Stable JSON/Markdown contract for CDC schema evolution evidence."""

    stream: CdcStreamKey
    change: CdcSchemaChangeEvent
    plan: CdcSchemaEvolutionPlan
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    policy: CdcSchemaEvolutionPolicy
    evidence: tuple[CdcSchemaEvolutionEvidenceItem, ...]
    evidence_artifacts: Mapping[str, str]
    upstream_artifacts: Mapping[str, str]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "route": self.stream.route.to_dict(),
            "change": self.change.to_dict(),
            "plan": self.plan.to_dict(),
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "policy": self.policy.to_dict(),
            "evidence": [item.to_dict() for item in self.evidence],
            "evidence_artifacts": dict(self.evidence_artifacts),
            "upstream_artifacts": dict(self.upstream_artifacts),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# CDC schema evolution evidence",
            "",
            f"- Route: `{self.stream.route.case_id}`",
            f"- Stream: `{self.stream.stream_id}`",
            f"- Change: `{self.change.kind}`",
            f"- Compatibility: `{self.plan.compatibility_level}`",
            f"- Passed: `{self.passed}`",
            "",
            "## Evidence",
            "",
            "| evidence | status | summary | blockers | path |",
            "|---|---|---|---|---|",
        ]
        for item in self.evidence:
            status = "pass" if item.passed else "fail"
            blockers = ", ".join(item.blockers) if item.blockers else "none"
            lines.append(
                f"| `{item.name}` | {status} | {item.summary} | `{blockers}` | "
                f"`{self.evidence_artifacts.get(item.name, '')}` |"
            )
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings) if self.warnings else lines.append("- none")
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


def load_schema_evolution_input(path: str | Path) -> tuple[CdcSchemaChangeEvent, CdcSchemaEvolutionPlan]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("CDC schema change input must be a JSON object")
    change = payload.get("change")
    plan = payload.get("plan")
    if not isinstance(change, Mapping) or not isinstance(plan, Mapping):
        raise ValueError("CDC schema change input must include `change` and `plan` objects")
    return CdcSchemaChangeEvent.from_dict(change), CdcSchemaEvolutionPlan.from_dict(plan)


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "pass", "passed", "safe", "approved"}
    return bool(value)


def _sequence(value: object) -> tuple[object, ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(value)
    return (value,)
