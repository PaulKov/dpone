"""Route data quality scorecard public contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal, Protocol

SCHEMA_VERSION = "dpone.route_data_quality.v1"

RouteDataQualityStatus = Literal["passed", "warning", "blocked", "waiver_required", "quarantine_sla_breached"]


@dataclass(frozen=True, slots=True)
class RouteDataQualityThresholds:
    """Policy thresholds for one route data quality scorecard."""

    min_score: float = 95.0
    warning_score: float = 98.0
    max_exception_ratio: float = 0.0
    max_quarantine_rows: int = 0
    max_exception_age_hours: float = 24.0

    def to_dict(self) -> dict[str, float | int]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteDataQualityDimension:
    """One normalized scorecard dimension."""

    name: str
    score: float
    passed: bool
    weight: float
    summary: str
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["blockers"] = list(self.blockers)
        payload["warnings"] = list(self.warnings)
        return payload


@dataclass(frozen=True, slots=True)
class RouteDataQualityExceptionSummary:
    """Aggregated exception backlog for one route scorecard."""

    total_count: int
    max_ratio: float
    max_age_hours: float
    by_evidence: dict[str, dict[str, float | int]]

    def to_dict(self) -> dict[str, object]:
        return {
            "total_count": self.total_count,
            "max_ratio": self.max_ratio,
            "max_age_hours": self.max_age_hours,
            "by_evidence": self.by_evidence,
        }


@dataclass(frozen=True, slots=True)
class RouteDataQualityEvidence:
    """Normalized data quality evidence artifact."""

    name: str
    kind: str
    path: str
    required: bool
    missing: bool
    passed: bool
    sha256: str
    summary: str
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    route_case_id: str
    route_matched: bool
    score: float
    dimensions: tuple[RouteDataQualityDimension, ...]
    exception_count: int
    exception_ratio: float
    max_exception_age_hours: float
    waiver_required: bool
    waiver_approved: bool

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["blockers"] = list(self.blockers)
        payload["warnings"] = list(self.warnings)
        payload["dimensions"] = [item.to_dict() for item in self.dimensions]
        return payload


@dataclass(frozen=True, slots=True)
class RouteDataQualityReport:
    """Stable JSON/Markdown route data quality scorecard."""

    route: _RouteIdentity
    profile: _RouteProfile | None
    passed: bool
    status: RouteDataQualityStatus
    score: float
    thresholds: RouteDataQualityThresholds
    dimensions: tuple[RouteDataQualityDimension, ...]
    exceptions: RouteDataQualityExceptionSummary
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    required_evidence: tuple[str, ...]
    evidence: tuple[RouteDataQualityEvidence, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "passed": self.passed,
            "status": self.status,
            "score": self.score,
            "thresholds": self.thresholds.to_dict(),
            "dimensions": {item.name: item.to_dict() for item in self.dimensions},
            "exceptions": self.exceptions.to_dict(),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "required_evidence": list(self.required_evidence),
            "evidence": [item.to_dict() for item in self.evidence],
            "evidence_index": {item.name: item.to_dict() for item in self.evidence},
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Route data quality scorecard",
            "",
            f"- Route: `{self.route.case_id}`",
            f"- Passed: `{self.passed}`",
            f"- Status: `{self.status}`",
            f"- Score: `{self.score}`",
            f"- Required evidence: `{', '.join(self.required_evidence)}`",
        ]
        if self.profile:
            lines.extend(
                [
                    f"- Docs: `{self.profile.docs_link}`",
                    f"- Native fast path: `{self.profile.native_fast_path}`",
                ]
            )
        lines.extend(
            [
                "",
                "## Dimensions",
                "",
                "| dimension | score | weight | status | summary |",
                "|---|---:|---:|---|---|",
            ]
        )
        for item in self.dimensions:
            status = "pass" if item.passed else "fail"
            lines.append(f"| `{item.name}` | `{item.score}` | `{item.weight}` | {status} | {item.summary} |")
        lines.extend(
            [
                "",
                "## Exceptions",
                "",
                f"- Total count: `{self.exceptions.total_count}`",
                f"- Max ratio: `{self.exceptions.max_ratio}`",
                f"- Max age hours: `{self.exceptions.max_age_hours}`",
                "",
                "## Evidence",
                "",
                "| evidence | kind | required | status | route | score | exceptions | sha256 | summary | path |",
                "|---|---|---|---|---|---:|---:|---|---|---|",
            ]
        )
        for evidence_item in self.evidence:
            status = "missing" if evidence_item.missing else ("pass" if evidence_item.passed else "fail")
            route_status = "match" if evidence_item.route_matched else "mismatch"
            lines.append(
                f"| `{evidence_item.name}` | `{evidence_item.kind}` | `{evidence_item.required}` | "
                f"{status} | {route_status} | `{evidence_item.score}` | `{evidence_item.exception_count}` | "
                f"`{evidence_item.sha256}` | {evidence_item.summary} | `{evidence_item.path}` |"
            )
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings) if self.warnings else lines.append("- none")
        lines.extend(["", "## Runbook", ""])
        if self.next_actions:
            lines.extend(f"- {item}" for item in self.next_actions)
        else:
            lines.extend(
                [
                    "- Attach `route_data_quality.json` and `route_data_quality.md` to route release evidence.",
                    "- Keep upstream quality, quarantine, and reconciliation evidence immutable for this release.",
                ]
            )
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


class _RouteIdentity(Protocol):
    @property
    def case_id(self) -> str: ...

    def to_dict(self) -> dict[str, str]: ...


class _RouteProfile(Protocol):
    @property
    def docs_link(self) -> str: ...

    @property
    def native_fast_path(self) -> str: ...

    def to_dict(self) -> dict[str, object]: ...


__all__ = [
    "RouteDataQualityDimension",
    "RouteDataQualityEvidence",
    "RouteDataQualityExceptionSummary",
    "RouteDataQualityReport",
    "RouteDataQualityStatus",
    "RouteDataQualityThresholds",
    "SCHEMA_VERSION",
]
