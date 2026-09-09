"""Route release candidate orchestration report contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from dpone.ops.routes.models import RouteKey, RouteProfile

SCHEMA_VERSION = "dpone.route_rc_orchestrator.v1"


@dataclass(frozen=True, slots=True)
class RouteRcOrchestrationStep:
    """One ordered release candidate train step."""

    name: str
    command: str
    path: str
    sha256: str
    passed: bool
    required: bool
    summary: str
    blockers: tuple[str, ...] = tuple()

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["blockers"] = list(self.blockers)
        return payload


@dataclass(frozen=True, slots=True)
class RouteRcOrchestrationDecision:
    """Pure policy decision for the ordered route RC train."""

    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RouteRcOrchestrationReport:
    """Stable JSON/Markdown release candidate orchestration receipt."""

    release: str
    profile: str
    row_count: int
    route: RouteKey
    route_profile: RouteProfile | None
    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    steps: tuple[RouteRcOrchestrationStep, ...]
    artifact_index: dict[str, str]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "release": self.release,
            "profile": self.profile,
            "row_count": self.row_count,
            "route": self.route.to_dict(),
            "route_profile": self.route_profile.to_dict() if self.route_profile else None,
            "passed": self.passed,
            "level": self.level,
            "score": self.score,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "steps": [step.to_dict() for step in self.steps],
            "artifact_index": dict(self.artifact_index),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Route release candidate orchestration",
            "",
            f"- Release: `{self.release}`",
            f"- Profile: `{self.profile}`",
            f"- Route: `{self.route.case_id}`",
            f"- Passed: `{self.passed}`",
            f"- Level: `{self.level}`",
            f"- Score: `{self.score}`",
            "",
            "## Release train",
            "",
            "| step | required | status | sha256 | summary | path |",
            "|---|---:|---|---|---|---|",
        ]
        for step in self.steps:
            status = "pass" if step.passed else "fail"
            lines.append(
                f"| `{step.name}` | `{step.required}` | {status} | `{step.sha256}` | {step.summary} | `{step.path}` |"
            )
        lines.extend(["", "## Artifact index", ""])
        for name, path in self.artifact_index.items():
            lines.append(f"- `{name}`: `{path}`")
        lines.extend(["", "## Blockers", ""])
        if self.blockers:
            lines.extend(f"- `{item}`" for item in self.blockers)
        else:
            lines.append("- none")
        lines.extend(["", "## Operator runbook", ""])
        if self.next_actions:
            lines.extend(f"- {item}" for item in self.next_actions)
        else:
            lines.extend(
                [
                    "- Attach `route_rc_orchestration.json` to release review.",
                    "- Attach `release_evidence_pack.json` to release notes or the approval record.",
                    "- Keep all upstream evidence immutable for this release candidate.",
                ]
            )
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")
