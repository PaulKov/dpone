"""Route run supervisor public report contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from dpone.ops.routes.models import RouteKey, RouteProfile

SCHEMA_VERSION = "dpone.route_run_supervisor.v1"

RouteRunMode = Literal["evidence_only", "route_refresh"]
RouteRunStatus = Literal["ready", "blocked", "retryable", "unsafe_to_retry", "manual_approval_required"]
RouteRunContractStageStatus = Literal[
    "complete",
    "missing",
    "blocked",
    "retryable",
    "unsafe_to_retry",
    "manual_approval_required",
    "optional",
]


@dataclass(frozen=True, slots=True)
class RouteRunIdentity:
    """Operator-facing identity for one route run."""

    run_id: str
    dataset: str
    manifest: str
    mode: str = "evidence_only"

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteRunEvidence:
    """Normalized evidence artifact attached to a route run receipt."""

    name: str
    phase: str
    kind: str
    path: str
    required: bool
    missing: bool
    passed: bool
    sha256: str
    summary: str
    blockers: tuple[str, ...]
    route_case_id: str
    route_matched: bool
    safe_to_retry: bool | None
    requires_manual_approval: bool

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["blockers"] = list(self.blockers)
        return payload


@dataclass(frozen=True, slots=True)
class RouteRunPhase:
    """Aggregated status for one route run lifecycle phase."""

    name: str
    required: bool
    passed: bool
    evidence: tuple[str, ...]
    blockers: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "required": self.required,
            "passed": self.passed,
            "evidence": list(self.evidence),
            "blockers": list(self.blockers),
        }


@dataclass(frozen=True, slots=True)
class RouteRunContractStage:
    """Expected execution-plane stage for one route run evidence domain."""

    name: str
    phase: str
    evidence: str
    required: bool
    status: RouteRunContractStageStatus
    command: str
    path: str
    blockers: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "phase": self.phase,
            "evidence": self.evidence,
            "required": self.required,
            "status": self.status,
            "command": self.command,
            "path": self.path,
            "blockers": list(self.blockers),
        }


@dataclass(frozen=True, slots=True)
class RouteRunExecutionContract:
    """Control-plane execution contract for expected route run stages."""

    mode: str
    ready: bool
    stages: tuple[RouteRunContractStage, ...]
    next_commands: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "ready": self.ready,
            "stages": [stage.to_dict() for stage in self.stages],
            "next_commands": list(self.next_commands),
        }


@dataclass(frozen=True, slots=True)
class RouteRunDecision:
    """Pure policy decision for one route run evidence bundle."""

    status: RouteRunStatus
    passed: bool
    retry_safe: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "passed": self.passed,
            "retry_safe": self.retry_safe,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
        }


@dataclass(frozen=True, slots=True)
class RouteRunEvidenceBundle:
    """Stable JSON/Markdown contract for route run lifecycle supervision."""

    route: RouteKey
    run: RouteRunIdentity
    profile: RouteProfile | None
    passed: bool
    decision: RouteRunDecision
    required_evidence: tuple[str, ...]
    execution_contract: RouteRunExecutionContract
    phases: tuple[RouteRunPhase, ...]
    evidence: tuple[RouteRunEvidence, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    @property
    def blockers(self) -> tuple[str, ...]:
        return self.decision.blockers

    @property
    def warnings(self) -> tuple[str, ...]:
        return self.decision.warnings

    @property
    def next_actions(self) -> tuple[str, ...]:
        return self.decision.next_actions

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "run": self.run.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "passed": self.passed,
            "decision": self.decision.to_dict(),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "required_evidence": list(self.required_evidence),
            "execution_contract": self.execution_contract.to_dict(),
            "phases": {phase.name: phase.to_dict() for phase in self.phases},
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
            "# Route run receipt",
            "",
            f"- Route: `{self.route.case_id}`",
            f"- Run ID: `{self.run.run_id}`",
            f"- Dataset: `{self.run.dataset}`",
            f"- Manifest: `{self.run.manifest or 'not provided'}`",
            f"- Mode: `{self.run.mode}`",
            f"- Passed: `{self.passed}`",
            f"- Status: `{self.decision.status}`",
            f"- Retry safe: `{self.decision.retry_safe}`",
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
                "## Phases",
                "",
                "| phase | required | status | evidence | blockers |",
                "|---|---|---|---|---|",
            ]
        )
        for phase in self.phases:
            status = "pass" if phase.passed else "fail"
            blockers = ", ".join(f"`{item}`" for item in phase.blockers) if phase.blockers else "none"
            lines.append(
                f"| `{phase.name}` | `{phase.required}` | {status} | `{', '.join(phase.evidence)}` | {blockers} |"
            )
        lines.extend(
            [
                "",
                "## Execution Contract",
                "",
                "| stage | phase | evidence | required | status | command | blockers |",
                "|---|---|---|---:|---|---|---|",
            ]
        )
        for stage in self.execution_contract.stages:
            blockers = ", ".join(f"`{item}`" for item in stage.blockers) if stage.blockers else "none"
            lines.append(
                f"| `{stage.name}` | `{stage.phase}` | `{stage.evidence}` | `{stage.required}` | "
                f"{stage.status} | `{stage.command}` | {blockers} |"
            )
        lines.extend(
            [
                "",
                "## Evidence",
                "",
                "| evidence | phase | required | status | route | retry | sha256 | summary | path |",
                "|---|---|---|---|---|---|---|---|---|",
            ]
        )
        for item in self.evidence:
            status = "missing" if item.missing else ("pass" if item.passed else "fail")
            route = "match" if item.route_matched else "mismatch"
            retry = "unknown" if item.safe_to_retry is None else str(item.safe_to_retry).lower()
            lines.append(
                f"| `{item.name}` | `{item.phase}` | `{item.required}` | {status} | {route} | {retry} | "
                f"`{item.sha256}` | {item.summary} | `{item.path}` |"
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
                    "- Attach `route_run_receipt.json` and `route_run_receipt.md` to route release evidence.",
                    "- Keep upstream evidence immutable for this route run.",
                ]
            )
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


__all__ = [
    "RouteRunDecision",
    "RouteRunContractStage",
    "RouteRunExecutionContract",
    "RouteRunEvidence",
    "RouteRunEvidenceBundle",
    "RouteRunIdentity",
    "RouteRunMode",
    "RouteRunPhase",
    "RouteRunStatus",
    "SCHEMA_VERSION",
]
