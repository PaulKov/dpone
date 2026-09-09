"""CDC observability evidence value objects."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.ops.cdc.models import CdcStreamKey

SCHEMA_VERSION = "dpone.cdc_observability.v1"

OBSERVABILITY_EVIDENCE_DOMAINS: tuple[str, ...] = (
    "cdc_lag_slo",
    "cdc_freshness_slo",
    "cdc_retention_risk",
    "cdc_offset_commit_health",
    "cdc_duplicate_replay_rate",
    "cdc_throughput_slo",
)


@dataclass(frozen=True, slots=True)
class CdcTelemetrySnapshot:
    """Single normalized CDC telemetry sample for one stream."""

    captured_at: str
    lag_seconds: float
    freshness_seconds: float
    retention_remaining_seconds: float
    events_per_second: float
    duplicate_events: int
    replayed_events: int
    offset_commit_status: str
    last_committed_offset: str

    @classmethod
    def from_path(cls, path: str | Path) -> CdcTelemetrySnapshot:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("CDC telemetry metrics must be a JSON object")
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> CdcTelemetrySnapshot:
        return cls(
            captured_at=str(payload.get("captured_at", "")),
            lag_seconds=_float(payload.get("lag_seconds")),
            freshness_seconds=_float(payload.get("freshness_seconds")),
            retention_remaining_seconds=_float(payload.get("retention_remaining_seconds")),
            events_per_second=_float(payload.get("events_per_second")),
            duplicate_events=_int(payload.get("duplicate_events")),
            replayed_events=_int(payload.get("replayed_events")),
            offset_commit_status=str(payload.get("offset_commit_status", "")).strip().lower(),
            last_committed_offset=str(payload.get("last_committed_offset", "")).strip(),
        )

    @property
    def offset_committed(self) -> bool:
        return self.offset_commit_status in {"committed", "healthy", "ok"} and bool(self.last_committed_offset)

    def to_dict(self) -> dict[str, object]:
        return {
            "captured_at": self.captured_at,
            "lag_seconds": self.lag_seconds,
            "freshness_seconds": self.freshness_seconds,
            "retention_remaining_seconds": self.retention_remaining_seconds,
            "events_per_second": self.events_per_second,
            "duplicate_events": self.duplicate_events,
            "replayed_events": self.replayed_events,
            "offset_commit_status": self.offset_commit_status,
            "last_committed_offset": self.last_committed_offset,
            "offset_committed": self.offset_committed,
        }


@dataclass(frozen=True, slots=True)
class CdcSloProfile:
    """Pluggable SLO thresholds for CDC observability evidence."""

    max_lag_seconds: float = 300.0
    max_freshness_seconds: float = 600.0
    min_retention_remaining_seconds: float = 1800.0
    min_events_per_second: float = 1.0
    max_duplicate_events: int = 0
    max_replayed_events: int | None = None
    require_committed_offset: bool = True

    @classmethod
    def default(cls) -> CdcSloProfile:
        return cls()

    @classmethod
    def from_path(cls, path: str | Path) -> CdcSloProfile:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("CDC SLO profile must be a JSON object")
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> CdcSloProfile:
        default = cls.default()
        return cls(
            max_lag_seconds=_float(payload.get("max_lag_seconds", default.max_lag_seconds)),
            max_freshness_seconds=_float(payload.get("max_freshness_seconds", default.max_freshness_seconds)),
            min_retention_remaining_seconds=_float(
                payload.get("min_retention_remaining_seconds", default.min_retention_remaining_seconds)
            ),
            min_events_per_second=_float(payload.get("min_events_per_second", default.min_events_per_second)),
            max_duplicate_events=_int(payload.get("max_duplicate_events", default.max_duplicate_events)),
            max_replayed_events=_optional_int(payload.get("max_replayed_events", default.max_replayed_events)),
            require_committed_offset=bool(payload.get("require_committed_offset", default.require_committed_offset)),
        )

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "max_lag_seconds": self.max_lag_seconds,
            "max_freshness_seconds": self.max_freshness_seconds,
            "min_retention_remaining_seconds": self.min_retention_remaining_seconds,
            "min_events_per_second": self.min_events_per_second,
            "max_duplicate_events": self.max_duplicate_events,
            "require_committed_offset": self.require_committed_offset,
        }
        if self.max_replayed_events is not None:
            payload["max_replayed_events"] = self.max_replayed_events
        return payload


@dataclass(frozen=True, slots=True)
class CdcObservabilityEvidenceItem:
    """Normalized evidence item emitted by one CDC observability check."""

    name: str
    passed: bool
    summary: str
    blockers: tuple[str, ...]
    metrics: Mapping[str, object]
    slo: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "kind": "cdc_observability",
            "passed": self.passed,
            "summary": self.summary,
            "blockers": list(self.blockers),
            "metrics": dict(self.metrics),
            "slo": dict(self.slo),
        }


@dataclass(frozen=True, slots=True)
class CdcObservabilityDecision:
    """Pure go/no-go decision for CDC observability evidence."""

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
class CdcObservabilityReport:
    """Stable public JSON/Markdown contract for CDC observability evidence."""

    stream: CdcStreamKey
    passed: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    metrics: Mapping[str, object]
    slo_profile: CdcSloProfile
    evidence: tuple[CdcObservabilityEvidenceItem, ...]
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
            "passed": self.passed,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
            "slo_profile": self.slo_profile.to_dict(),
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
            "# CDC observability evidence",
            "",
            f"- Route: `{self.stream.route.case_id}`",
            f"- Stream: `{self.stream.stream_id}`",
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
        lines.extend(["", "## Metrics", ""])
        lines.extend(f"- `{name}`: `{value}`" for name, value in self.metrics.items())
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
