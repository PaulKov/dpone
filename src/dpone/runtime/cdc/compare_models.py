"""CDC compare and repair value objects."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.runtime.cdc.runtime_models import CdcRuntimeStream

COMPARE_REPAIR_SCHEMA_VERSION = "dpone.cdc_compare_repair.v1"
REPAIR_PLAN_SCHEMA_VERSION = "dpone.cdc_repair_plan.v1"


@dataclass(frozen=True, slots=True)
class CdcCompareRow:
    """Canonical row snapshot used by CDC compare readers."""

    key: Mapping[str, object]
    payload: Mapping[str, object]
    deleted: bool = False
    position: str | None = None

    @classmethod
    def from_payload(
        cls,
        *,
        unique_key: Sequence[str],
        payload: Mapping[str, object],
        deleted: bool = False,
        position: str | None = None,
    ) -> CdcCompareRow:
        key = {column: payload.get(column) for column in unique_key}
        return cls(key=key, payload=dict(payload), deleted=deleted, position=position)

    @property
    def key_json(self) -> str:
        return canonical_json(self.key)

    @property
    def payload_hash(self) -> str:
        return sha256(canonical_json(self.payload))

    def to_dict(self) -> dict[str, object]:
        return {
            "key": json_safe_mapping(self.key),
            "payload": json_safe_mapping(self.payload),
            "deleted": self.deleted,
            "position": self.position,
            "payload_hash": self.payload_hash,
        }


@dataclass(frozen=True, slots=True)
class CdcCompareDiff:
    """One source/target current-state difference."""

    kind: str
    key: Mapping[str, object]
    source_payload: Mapping[str, object] | None
    target_payload: Mapping[str, object] | None
    source_hash: str | None
    target_hash: str | None
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "key": json_safe_mapping(self.key),
            "source_payload": json_safe_mapping(self.source_payload) if self.source_payload is not None else None,
            "target_payload": json_safe_mapping(self.target_payload) if self.target_payload is not None else None,
            "source_hash": self.source_hash,
            "target_hash": self.target_hash,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class CdcRepairAction:
    """One replayable action that can repair a compare diff."""

    action_id: str
    kind: str
    operation: str
    key: Mapping[str, object]
    payload: Mapping[str, object]
    target_payload: Mapping[str, object] | None
    reason: str
    replayable: bool = True

    def to_dict(self) -> dict[str, object]:
        return {
            "action_id": self.action_id,
            "kind": self.kind,
            "operation": self.operation,
            "key": json_safe_mapping(self.key),
            "payload": json_safe_mapping(self.payload),
            "target_payload": json_safe_mapping(self.target_payload) if self.target_payload is not None else None,
            "reason": self.reason,
            "replayable": self.replayable,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> CdcRepairAction:
        return cls(
            action_id=str(payload["action_id"]),
            kind=str(payload["kind"]),
            operation=str(payload["operation"]),
            key=_mapping(payload.get("key")),
            payload=_mapping(payload.get("payload")),
            target_payload=_optional_mapping(payload.get("target_payload")),
            reason=str(payload.get("reason") or ""),
            replayable=bool(payload.get("replayable", True)),
        )


@dataclass(frozen=True, slots=True)
class CdcRepairPlan:
    """Stable repair-plan contract embedded in compare reports."""

    stream: CdcRuntimeStream
    actions: tuple[CdcRepairAction, ...]
    output_path: str | None = None

    @property
    def action_count(self) -> int:
        return len(self.actions)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": REPAIR_PLAN_SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "action_count": self.action_count,
            "actions": [action.to_dict() for action in self.actions],
            "output_path": self.output_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def write(self, path: str | Path) -> CdcRepairPlan:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        updated = CdcRepairPlan(stream=self.stream, actions=self.actions, output_path=str(target))
        target.write_text(updated.to_json(), encoding="utf-8")
        return updated

    @classmethod
    def from_dict(cls, payload: Mapping[str, object], *, stream: CdcRuntimeStream) -> CdcRepairPlan:
        actions = tuple(CdcRepairAction.from_dict(item) for item in _sequence(payload.get("actions")))
        return cls(stream=stream, actions=actions, output_path=str(payload.get("output_path") or "") or None)


@dataclass(frozen=True, slots=True)
class CdcCompareRepairReport:
    """Stable JSON/Markdown contract for one CDC compare and repair plan."""

    stream: CdcRuntimeStream
    rows_source: int
    rows_target: int
    matched_rows: int
    diff_count: int
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    metrics: Mapping[str, object]
    diffs: tuple[CdcCompareDiff, ...]
    repair_plan: CdcRepairPlan
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": COMPARE_REPAIR_SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "rows_source": self.rows_source,
            "rows_target": self.rows_target,
            "matched_rows": self.matched_rows,
            "diff_count": self.diff_count,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
            "diffs": [diff.to_dict() for diff in self.diffs],
            "repair_plan": self.repair_plan.to_dict(),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# CDC compare and repair",
            "",
            f"- Pipeline: `{self.stream.pipeline_name}`",
            f"- Route: `{self.stream.route_id}`",
            f"- Stream: `{self.stream.stream_id}`",
            f"- Passed: `{self.passed}`",
            f"- Source rows: `{self.rows_source}`",
            f"- Target rows: `{self.rows_target}`",
            f"- Matched rows: `{self.matched_rows}`",
            f"- Diff count: `{self.diff_count}`",
            f"- Repair actions: `{self.repair_plan.action_count}`",
            "",
            "## Blockers",
            "",
        ]
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings) if self.warnings else lines.append("- none")
        lines.extend(["", "## Diff kinds", ""])
        if self.diffs:
            for diff in self.diffs:
                lines.append(f"- `{diff.kind}` `{canonical_json(diff.key)}`")
        else:
            lines.append("- none")
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


def load_repair_plan(path: str | Path, *, stream: CdcRuntimeStream) -> CdcRepairPlan:
    payload = _mapping(json.loads(Path(path).read_text(encoding="utf-8")))
    return CdcRepairPlan.from_dict(payload, stream=stream)


def canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def json_safe_mapping(value: Mapping[str, object]) -> dict[str, object]:
    return dict(json.loads(canonical_json(value)))


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise ValueError("CDC compare JSON fields must be objects")


def _optional_mapping(value: object) -> Mapping[str, Any] | None:
    if value is None:
        return None
    return _mapping(value)


def _sequence(value: object) -> tuple[Mapping[str, object], ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(_mapping(item) for item in value)
    raise ValueError("CDC repair actions must be an array")


__all__ = [
    "COMPARE_REPAIR_SCHEMA_VERSION",
    "REPAIR_PLAN_SCHEMA_VERSION",
    "CdcCompareDiff",
    "CdcCompareRepairReport",
    "CdcCompareRow",
    "CdcRepairAction",
    "CdcRepairPlan",
    "canonical_json",
    "json_safe_mapping",
    "load_repair_plan",
    "sha256",
]
