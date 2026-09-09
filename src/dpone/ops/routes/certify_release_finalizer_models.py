"""Final release route certification contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "dpone.route_certification_release_finalizer.v1"


@dataclass(frozen=True, slots=True)
class RouteCertificationReleaseFinalizerCheck:
    """One finalizer policy check for one route or release domain."""

    name: str
    route_case_id: str
    passed: bool
    required: bool
    summary: str
    blocker: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteCertificationReleaseHistoryEntry:
    """One immutable route certification release history entry."""

    release: str
    profile: str
    passed: bool
    level: str
    score: float
    route_scores: dict[str, float]
    route_levels: dict[str, str]
    finalizer_path: str

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["route_scores"] = dict(self.route_scores)
        payload["route_levels"] = dict(self.route_levels)
        return payload


@dataclass(frozen=True, slots=True)
class RouteCertificationReleaseFinalizerReport:
    """Stable release-final route certification artifact."""

    release: str
    profile: str
    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    discovered_routes: tuple[str, ...]
    required_routes: tuple[str, ...]
    route_scores: dict[str, float]
    route_levels: dict[str, str]
    release_report_path: str
    release_report: dict[str, Any]
    checks: tuple[RouteCertificationReleaseFinalizerCheck, ...]
    artifact_index: dict[str, str]
    output_dir: str
    json_path: str
    markdown_path: str
    history_index_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "release": self.release,
            "profile": self.profile,
            "passed": self.passed,
            "level": self.level,
            "score": self.score,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "discovered_routes": list(self.discovered_routes),
            "required_routes": list(self.required_routes),
            "route_scores": dict(self.route_scores),
            "route_levels": dict(self.route_levels),
            "release_report_path": self.release_report_path,
            "release_report": dict(self.release_report),
            "checks": [check.to_dict() for check in self.checks],
            "artifact_index": dict(self.artifact_index),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
            "history_index_path": self.history_index_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Route release finalizer",
            "",
            f"- Release: `{self.release}`",
            f"- Profile: `{self.profile}`",
            f"- Passed: `{self.passed}`",
            f"- Level: `{self.level}`",
            f"- Score: `{self.score}`",
            "",
            "## Checks",
            "",
            "| check | route | required | status | summary |",
            "|---|---|---:|---|---|",
        ]
        for check in self.checks:
            status = "pass" if check.passed else "fail"
            route = check.route_case_id or "release"
            lines.append(f"| `{check.name}` | `{route}` | `{check.required}` | {status} | {check.summary} |")
        lines.extend(["", "## Blockers", ""])
        if self.blockers:
            lines.extend(f"- `{blocker}`" for blocker in self.blockers)
        else:
            lines.append("- none")
        lines.extend(["", "## Operator runbook", ""])
        if self.next_actions:
            lines.extend(f"- {action}" for action in self.next_actions)
        else:
            lines.extend(
                [
                    "- Attach `route_release_finalizer.json` to release review.",
                    "- Attach `route_certification_history_index.json` to release evidence.",
                    "- Tag only while this report remains `final_ready`.",
                ]
            )
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


__all__ = [
    "RouteCertificationReleaseFinalizerCheck",
    "RouteCertificationReleaseFinalizerReport",
    "RouteCertificationReleaseHistoryEntry",
]
