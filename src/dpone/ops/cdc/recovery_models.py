"""CDC recovery evidence value objects."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.ops.cdc.models import CdcStreamKey

SCHEMA_VERSION = "dpone.cdc_recovery_evidence.v1"

RECOVERY_EVIDENCE_DOMAINS: tuple[str, ...] = (
    "cdc_restart_resume",
    "cdc_offset_commit_ordering",
    "cdc_idempotent_replay_window",
    "cdc_partial_commit_repair",
    "cdc_poison_event_quarantine",
    "cdc_retention_recovery_margin",
)

CDC_FAILURE_SCENARIO_KINDS: tuple[str, ...] = (
    "consumer_restart",
    "partial_sink_commit",
    "offset_commit_failed",
    "duplicate_replay",
    "poison_event",
    "retention_window_pressure",
)


@dataclass(frozen=True, slots=True)
class CdcFailureScenario:
    """Normalized fault-injection result for one CDC stream."""

    scenario_id: str
    kind: str
    injected_at: str
    failed_after_offset: str
    recovered_offset: str
    resume_completed: bool
    sink_commit_state: str
    offset_commit_state: str
    offset_committed_after_sink_commit: bool
    partial_sink_commit_detected: bool
    partial_sink_commit_repaired: bool
    repair_actions: tuple[str, ...]
    replayed_events: int
    duplicate_events: int
    idempotent_replay_passed: bool
    poison_events: int
    quarantined_events: int
    retention_remaining_seconds: float
    recovery_margin_seconds: float

    @classmethod
    def from_path(cls, path: str | Path) -> CdcFailureScenario:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("CDC failure scenario must be a JSON object")
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> CdcFailureScenario:
        return cls(
            scenario_id=str(payload.get("scenario_id", "")).strip(),
            kind=str(payload.get("kind", "")).strip().lower().replace("-", "_"),
            injected_at=str(payload.get("injected_at", "")).strip(),
            failed_after_offset=str(payload.get("failed_after_offset", "")).strip(),
            recovered_offset=str(payload.get("recovered_offset", "")).strip(),
            resume_completed=_bool(payload.get("resume_completed")),
            sink_commit_state=str(payload.get("sink_commit_state", "")).strip().lower(),
            offset_commit_state=str(payload.get("offset_commit_state", "")).strip().lower(),
            offset_committed_after_sink_commit=_bool(payload.get("offset_committed_after_sink_commit")),
            partial_sink_commit_detected=_bool(payload.get("partial_sink_commit_detected")),
            partial_sink_commit_repaired=_bool(payload.get("partial_sink_commit_repaired")),
            repair_actions=tuple(str(item) for item in _sequence(payload.get("repair_actions"))),
            replayed_events=_int(payload.get("replayed_events")),
            duplicate_events=_int(payload.get("duplicate_events")),
            idempotent_replay_passed=_bool(payload.get("idempotent_replay_passed")),
            poison_events=_int(payload.get("poison_events")),
            quarantined_events=_int(payload.get("quarantined_events")),
            retention_remaining_seconds=_float(payload.get("retention_remaining_seconds")),
            recovery_margin_seconds=_float(payload.get("recovery_margin_seconds")),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "scenario_id": self.scenario_id,
            "kind": self.kind,
            "injected_at": self.injected_at,
            "failed_after_offset": self.failed_after_offset,
            "recovered_offset": self.recovered_offset,
            "resume_completed": self.resume_completed,
            "sink_commit_state": self.sink_commit_state,
            "offset_commit_state": self.offset_commit_state,
            "offset_committed_after_sink_commit": self.offset_committed_after_sink_commit,
            "partial_sink_commit_detected": self.partial_sink_commit_detected,
            "partial_sink_commit_repaired": self.partial_sink_commit_repaired,
            "repair_actions": list(self.repair_actions),
            "replayed_events": self.replayed_events,
            "duplicate_events": self.duplicate_events,
            "idempotent_replay_passed": self.idempotent_replay_passed,
            "poison_events": self.poison_events,
            "quarantined_events": self.quarantined_events,
            "retention_remaining_seconds": self.retention_remaining_seconds,
            "recovery_margin_seconds": self.recovery_margin_seconds,
        }


@dataclass(frozen=True, slots=True)
class CdcRecoveryPolicy:
    """Fault-recovery thresholds for CDC evidence evaluation."""

    min_recovery_margin_seconds: float = 900.0
    max_duplicate_events: int = 0
    max_replayed_events: int | None = None
    require_poison_quarantine: bool = True
    require_offset_commit_ordering: bool = True

    @classmethod
    def default(cls) -> CdcRecoveryPolicy:
        return cls()

    @classmethod
    def from_path(cls, path: str | Path) -> CdcRecoveryPolicy:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("CDC recovery policy must be a JSON object")
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> CdcRecoveryPolicy:
        default = cls.default()
        return cls(
            min_recovery_margin_seconds=_float(
                payload.get("min_recovery_margin_seconds", default.min_recovery_margin_seconds)
            ),
            max_duplicate_events=_int(payload.get("max_duplicate_events", default.max_duplicate_events)),
            max_replayed_events=_optional_int(payload.get("max_replayed_events", default.max_replayed_events)),
            require_poison_quarantine=_bool(
                payload.get("require_poison_quarantine", default.require_poison_quarantine)
            ),
            require_offset_commit_ordering=_bool(
                payload.get("require_offset_commit_ordering", default.require_offset_commit_ordering)
            ),
        )

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "min_recovery_margin_seconds": self.min_recovery_margin_seconds,
            "max_duplicate_events": self.max_duplicate_events,
            "require_poison_quarantine": self.require_poison_quarantine,
            "require_offset_commit_ordering": self.require_offset_commit_ordering,
        }
        if self.max_replayed_events is not None:
            payload["max_replayed_events"] = self.max_replayed_events
        return payload


@dataclass(frozen=True, slots=True)
class CdcRecoveryEvidenceItem:
    """Normalized evidence item emitted by one CDC recovery check."""

    name: str
    passed: bool
    summary: str
    blockers: tuple[str, ...]
    metrics: Mapping[str, object]
    policy: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "kind": "cdc_recovery",
            "passed": self.passed,
            "summary": self.summary,
            "blockers": list(self.blockers),
            "metrics": dict(self.metrics),
            "policy": dict(self.policy),
        }


@dataclass(frozen=True, slots=True)
class CdcRecoveryDecision:
    """Pure go/no-go decision for CDC recovery evidence."""

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
class CdcRecoveryReport:
    """Stable JSON/Markdown contract for CDC recovery evidence."""

    stream: CdcStreamKey
    scenario: CdcFailureScenario
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    policy: CdcRecoveryPolicy
    evidence: tuple[CdcRecoveryEvidenceItem, ...]
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
            "scenario": self.scenario.to_dict(),
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
            "# CDC recovery evidence",
            "",
            f"- Route: `{self.stream.route.case_id}`",
            f"- Stream: `{self.stream.stream_id}`",
            f"- Scenario: `{self.scenario.kind}`",
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


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "pass", "passed", "committed"}
    return bool(value)


def _float(value: object) -> float:
    if value is None or value == "":
        return 0.0
    return float(value)


def _int(value: object) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _sequence(value: object) -> tuple[object, ...]:
    if value is None:
        return tuple()
    if isinstance(value, list | tuple):
        return tuple(value)
    return (value,)
