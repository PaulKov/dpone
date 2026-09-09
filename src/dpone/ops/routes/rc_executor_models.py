"""Route release-candidate execution report contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from dpone.ops.routes.models import RouteKey

SCHEMA_VERSION = "dpone.route_rc_executor.v1"


@dataclass(frozen=True, slots=True)
class RouteRcExecutionArtifact:
    """Expected artifact produced by a route release-candidate execution."""

    name: str
    path: str
    required: bool
    exists: bool
    sha256: str
    status: str
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteRcExecutionStep:
    """One executable or planned route release-candidate step."""

    name: str
    command: str
    path: str
    required: bool
    status: str
    passed: bool
    attempts: int
    timeout_seconds: int
    exit_code: int | None
    timed_out: bool
    duration_seconds: float
    stdout_tail: str
    stderr_tail: str
    sha256: str
    artifact_exists: bool
    blockers: tuple[str, ...] = tuple()

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["blockers"] = list(self.blockers)
        return payload


@dataclass(frozen=True, slots=True)
class RouteRcExecutionDecision:
    """Pure policy decision for route RC command execution."""

    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RouteRcExecutionReport:
    """Stable JSON/Markdown execution receipt for route release candidates."""

    release: str
    profile: str
    route: RouteKey
    orchestration_json: str
    mode: str
    executed: bool
    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    steps: tuple[RouteRcExecutionStep, ...]
    artifacts: tuple[RouteRcExecutionArtifact, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "release": self.release,
            "profile": self.profile,
            "route": self.route.to_dict(),
            "orchestration_json": self.orchestration_json,
            "mode": self.mode,
            "executed": self.executed,
            "passed": self.passed,
            "level": self.level,
            "score": self.score,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "steps": [step.to_dict() for step in self.steps],
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Route release candidate execution",
            "",
            f"- Release: `{self.release}`",
            f"- Profile: `{self.profile}`",
            f"- Route: `{self.route.case_id}`",
            f"- Mode: `{self.mode}`",
            f"- Executed: `{self.executed}`",
            f"- Passed: `{self.passed}`",
            f"- Level: `{self.level}`",
            f"- Score: `{self.score}`",
            "",
            "## Steps",
            "",
            "| step | required | status | attempts | exit code | artifact | sha256 |",
            "|---|---:|---|---:|---:|---|---|",
        ]
        for step in self.steps:
            exit_code = "" if step.exit_code is None else str(step.exit_code)
            lines.append(
                f"| `{step.name}` | `{step.required}` | {step.status} | `{step.attempts}` | "
                f"`{exit_code}` | `{step.path}` | `{step.sha256}` |"
            )
        lines.extend(
            ["", "## Artifacts", "", "| artifact | required | status | sha256 | path |", "|---|---:|---|---|---|"]
        )
        for artifact in self.artifacts:
            lines.append(
                f"| `{artifact.name}` | `{artifact.required}` | {artifact.status} | "
                f"`{artifact.sha256}` | `{artifact.path}` |"
            )
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
                    "- Attach `route_rc_execution.json` to release review.",
                    "- Attach generated route evidence receipts from the execution output.",
                    "- Keep the input `route_rc_orchestration.json` immutable for this release candidate.",
                ]
            )
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


__all__ = [
    "SCHEMA_VERSION",
    "RouteRcExecutionArtifact",
    "RouteRcExecutionDecision",
    "RouteRcExecutionReport",
    "RouteRcExecutionStep",
]
