"""CDC retention-gap and resync value objects."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.compare_models import json_safe_mapping
from dpone.runtime.cdc.retention_reports import (
    resync_execution_markdown,
    resync_plan_markdown,
    retention_markdown,
    write_report_files,
)
from dpone.runtime.cdc.runtime_models import CdcRuntimeStream

RETENTION_CHECK_SCHEMA_VERSION = "dpone.cdc_retention_check.v1"
RESYNC_PLAN_SCHEMA_VERSION = "dpone.cdc_resync_plan.v1"
RESYNC_PLAN_REPORT_SCHEMA_VERSION = "dpone.cdc_resync_plan_report.v1"
RESYNC_EXECUTION_SCHEMA_VERSION = "dpone.cdc_resync_execution.v1"


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


@dataclass(frozen=True, slots=True)
class CdcRetentionBounds:
    """Source-side retained CDC interval for one stream backend."""

    backend: CDCBackend
    min_available_offset: str
    high_watermark: str
    current_offset: str
    retention_seconds: int | None = None
    captured_at: str = field(default_factory=_utc_now)

    def to_dict(self) -> dict[str, object]:
        return {
            "backend": self.backend.value,
            "min_available_offset": self.min_available_offset,
            "high_watermark": self.high_watermark,
            "current_offset": self.current_offset,
            "retention_seconds": self.retention_seconds,
            "captured_at": self.captured_at,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> CdcRetentionBounds:
        return cls(
            backend=CDCBackend(str(payload["backend"])),
            min_available_offset=str(payload["min_available_offset"]),
            high_watermark=str(payload["high_watermark"]),
            current_offset=str(payload.get("current_offset") or payload["high_watermark"]),
            retention_seconds=_optional_int(payload.get("retention_seconds")),
            captured_at=str(payload.get("captured_at") or _utc_now()),
        )


@dataclass(frozen=True, slots=True)
class CdcRetentionDecision:
    """Policy decision derived from committed offset and source retention bounds."""

    level: str
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    metrics: Mapping[str, object]
    next_actions: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "level": self.level,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
            "next_actions": list(self.next_actions),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> CdcRetentionDecision:
        return cls(
            level=str(payload["level"]),
            passed=bool(payload["passed"]),
            blockers=_strings(payload.get("blockers")),
            warnings=_strings(payload.get("warnings")),
            metrics=_mapping(payload.get("metrics", {})),
            next_actions=_strings(payload.get("next_actions")),
        )


@dataclass(frozen=True, slots=True)
class CdcRetentionReport:
    """Stable retention-check report for CLI, CI, and release gates."""

    stream: CdcRuntimeStream
    committed_offset: CDCOffset | None
    bounds: CdcRetentionBounds
    decision: CdcRetentionDecision
    output_dir: str
    json_path: str
    markdown_path: str

    @property
    def passed(self) -> bool:
        return self.decision.passed

    @property
    def blockers(self) -> tuple[str, ...]:
        return self.decision.blockers

    @property
    def warnings(self) -> tuple[str, ...]:
        return self.decision.warnings

    @property
    def metrics(self) -> Mapping[str, object]:
        return self.decision.metrics

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": RETENTION_CHECK_SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "committed_offset": _offset_dict(self.committed_offset),
            "bounds": self.bounds.to_dict(),
            "decision": self.decision.to_dict(),
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        return retention_markdown(self)

    def write(self) -> CdcRetentionReport:
        return write_report_files(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, object], *, stream: CdcRuntimeStream) -> CdcRetentionReport:
        committed = payload.get("committed_offset")
        return cls(
            stream=stream,
            committed_offset=CDCOffset.from_state(dict(_mapping(committed))) if committed else None,
            bounds=CdcRetentionBounds.from_dict(_mapping(payload["bounds"])),
            decision=CdcRetentionDecision.from_dict(_mapping(payload["decision"])),
            output_dir=str(payload.get("output_dir") or ""),
            json_path=str(payload.get("json_path") or ""),
            markdown_path=str(payload.get("markdown_path") or ""),
        )


@dataclass(frozen=True, slots=True)
class CdcResyncAction:
    """One bounded snapshot action used to rebuild target current state."""

    action_id: str
    operation: str
    key: Mapping[str, object]
    payload: Mapping[str, object]
    reason: str
    replayable: bool = True

    def to_dict(self) -> dict[str, object]:
        return {
            "action_id": self.action_id,
            "operation": self.operation,
            "key": json_safe_mapping(self.key),
            "payload": json_safe_mapping(self.payload),
            "reason": self.reason,
            "replayable": self.replayable,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> CdcResyncAction:
        return cls(
            action_id=str(payload["action_id"]),
            operation=str(payload["operation"]),
            key=_mapping(payload.get("key")),
            payload=_mapping(payload.get("payload")),
            reason=str(payload.get("reason") or ""),
            replayable=bool(payload.get("replayable", True)),
        )


@dataclass(frozen=True, slots=True)
class CdcResyncPlan:
    """Stable resync action plan consumed by the execution service."""

    stream: CdcRuntimeStream
    actions: tuple[CdcResyncAction, ...]
    reason: str
    retention_report_json: str | None = None
    output_path: str | None = None

    @property
    def action_count(self) -> int:
        return len(self.actions)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": RESYNC_PLAN_SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "reason": self.reason,
            "retention_report_json": self.retention_report_json,
            "action_count": self.action_count,
            "actions": [action.to_dict() for action in self.actions],
            "output_path": self.output_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def write(self, path: str | Path) -> CdcResyncPlan:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        updated = CdcResyncPlan(
            stream=self.stream,
            actions=self.actions,
            reason=self.reason,
            retention_report_json=self.retention_report_json,
            output_path=str(target),
        )
        target.write_text(updated.to_json(), encoding="utf-8")
        return updated

    @classmethod
    def from_dict(cls, payload: Mapping[str, object], *, stream: CdcRuntimeStream) -> CdcResyncPlan:
        return cls(
            stream=stream,
            actions=tuple(CdcResyncAction.from_dict(item) for item in _sequence(payload.get("actions"))),
            reason=str(payload.get("reason") or ""),
            retention_report_json=str(payload.get("retention_report_json") or "") or None,
            output_path=str(payload.get("output_path") or "") or None,
        )


@dataclass(frozen=True, slots=True)
class CdcResyncPlanReport:
    """Stable report for one resync planning run."""

    stream: CdcRuntimeStream
    retention_level: str
    plan: CdcResyncPlan
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    metrics: Mapping[str, object]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": RESYNC_PLAN_REPORT_SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "retention_level": self.retention_level,
            "plan": self.plan.to_dict(),
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        return resync_plan_markdown(self)

    def write(self) -> CdcResyncPlanReport:
        return write_report_files(self)


@dataclass(frozen=True, slots=True)
class CdcResyncExecutionReport:
    """Stable report for one resync execution run."""

    stream: CdcRuntimeStream
    resync_actions: int
    committed: bool
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    sink_receipt: object | None
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": RESYNC_EXECUTION_SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "resync_actions": self.resync_actions,
            "committed": self.committed,
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "sink_receipt": self.sink_receipt.to_dict() if hasattr(self.sink_receipt, "to_dict") else None,
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        return resync_execution_markdown(self)

    def write(self) -> CdcResyncExecutionReport:
        return write_report_files(self)


def load_retention_report(path: str | Path, *, stream: CdcRuntimeStream) -> CdcRetentionReport:
    payload = _mapping(json.loads(Path(path).read_text(encoding="utf-8")))
    return CdcRetentionReport.from_dict(payload, stream=stream)


def load_resync_plan(path: str | Path, *, stream: CdcRuntimeStream) -> CdcResyncPlan:
    payload = _mapping(json.loads(Path(path).read_text(encoding="utf-8")))
    return CdcResyncPlan.from_dict(payload, stream=stream)


def resync_row_key(*, unique_key: tuple[str, ...], payload: Mapping[str, object]) -> dict[str, object]:
    return {column: payload.get(column) for column in unique_key}


def _offset_dict(offset: CDCOffset | None) -> dict[str, object] | None:
    return offset.to_state() if offset else None


def _optional_int(value: object) -> int | None:
    return None if value is None else int(value)


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    raise ValueError("CDC retention JSON fields must be objects")


def _sequence(value: object) -> tuple[Mapping[str, object], ...]:
    if value is None:
        return tuple()
    if not isinstance(value, list | tuple):
        raise ValueError("CDC retention JSON sequence fields must be arrays")
    return tuple(_mapping(item) for item in value)


def _strings(value: object) -> tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(str(item) for item in value)
    raise ValueError("CDC retention JSON string lists must be arrays")


__all__ = [
    "RETENTION_CHECK_SCHEMA_VERSION",
    "RESYNC_EXECUTION_SCHEMA_VERSION",
    "RESYNC_PLAN_REPORT_SCHEMA_VERSION",
    "RESYNC_PLAN_SCHEMA_VERSION",
    "CdcResyncAction",
    "CdcResyncExecutionReport",
    "CdcResyncPlan",
    "CdcResyncPlanReport",
    "CdcRetentionBounds",
    "CdcRetentionDecision",
    "CdcRetentionReport",
    "load_resync_plan",
    "load_retention_report",
    "resync_row_key",
]
