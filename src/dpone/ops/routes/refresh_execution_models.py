"""Route refresh execution public contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal, Protocol

SCHEMA_VERSION = "dpone.route_refresh_execution.v1"

RouteRefreshExecutionStatus = Literal["dry_run", "succeeded", "partial_failure", "blocked", "approval_required"]


@dataclass(frozen=True, slots=True)
class RouteRefreshChunkExecutionRequest:
    """One route refresh chunk request passed to a concrete executor."""

    route: _RouteIdentity
    dataset: str
    ordinal: int
    start: str
    end: str
    partition: str
    source_boundary: str
    sink_boundary: str
    idempotency_key: str
    runner_id: str
    execute: bool
    output_dir: str
    plan_path: str


@dataclass(frozen=True, slots=True)
class RouteRefreshChunkExecutionResult:
    """One route refresh chunk execution or dry-run result."""

    ordinal: int
    idempotency_key: str
    status: str
    passed: bool
    rows_read: int
    rows_written: int
    artifact_path: str
    summary: str
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    duration_seconds: float = 0.0
    start: str = ""
    end: str = ""
    partition: str = ""
    source_boundary: str = ""
    sink_boundary: str = ""

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["blockers"] = list(self.blockers)
        payload["warnings"] = list(self.warnings)
        return payload


@dataclass(frozen=True, slots=True)
class RouteRefreshExecutionArtifact:
    """Artifact referenced by a route refresh execution receipt."""

    name: str
    path: str
    required: bool
    exists: bool
    sha256: str
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteRefreshExecutionReport:
    """Stable JSON/Markdown receipt for route refresh execution."""

    route: _RouteIdentity
    profile: _RouteProfile | None
    dataset: str
    runner_id: str
    route_refresh_plan_json: str
    plan_sha256: str
    mode: str
    executed: bool
    status: RouteRefreshExecutionStatus
    passed: bool
    ready_for_state_promotion: bool
    summary: dict[str, int]
    chunks: tuple[RouteRefreshChunkExecutionResult, ...]
    artifacts: tuple[RouteRefreshExecutionArtifact, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "dataset": self.dataset,
            "runner_id": self.runner_id,
            "route_refresh_plan_json": self.route_refresh_plan_json,
            "plan_sha256": self.plan_sha256,
            "mode": self.mode,
            "executed": self.executed,
            "status": self.status,
            "passed": self.passed,
            "ready_for_state_promotion": self.ready_for_state_promotion,
            "summary": self.summary,
            "chunks": [item.to_dict() for item in self.chunks],
            "artifacts": [item.to_dict() for item in self.artifacts],
            "artifact_index": {item.name: item.to_dict() for item in self.artifacts},
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Route refresh execution",
            "",
            f"- Route: `{self.route.case_id}`",
            f"- Dataset: `{self.dataset}`",
            f"- Runner: `{self.runner_id}`",
            f"- Mode: `{self.mode}`",
            f"- Executed: `{self.executed}`",
            f"- Status: `{self.status}`",
            f"- Passed: `{self.passed}`",
            f"- Ready for state promotion: `{self.ready_for_state_promotion}`",
            "",
            "## Summary",
            "",
        ]
        lines.extend(f"- {key}: `{value}`" for key, value in self.summary.items())
        lines.extend(
            [
                "",
                "## Chunks",
                "",
                "| # | status | rows read | rows written | idempotency key | artifact | summary |",
                "|---:|---|---:|---:|---|---|---|",
            ]
        )
        for chunk in self.chunks:
            lines.append(
                f"| `{chunk.ordinal}` | {chunk.status} | `{chunk.rows_read}` | `{chunk.rows_written}` | "
                f"`{chunk.idempotency_key}` | `{chunk.artifact_path}` | {chunk.summary} |"
            )
        lines.extend(
            ["", "## Artifacts", "", "| artifact | required | exists | sha256 | path |", "|---|---:|---:|---|---|"]
        )
        for artifact in self.artifacts:
            lines.append(
                f"| `{artifact.name}` | `{artifact.required}` | `{artifact.exists}` | "
                f"`{artifact.sha256}` | `{artifact.path}` |"
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
                    "- Attach `route_refresh_execution.json` and `route_refresh_execution.md` to release evidence.",
                    "- Record route execution ledger and route state promotion evidence before advancing source state.",
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
    def to_dict(self) -> dict[str, object]: ...


__all__ = [
    "RouteRefreshChunkExecutionRequest",
    "RouteRefreshChunkExecutionResult",
    "RouteRefreshExecutionArtifact",
    "RouteRefreshExecutionReport",
    "RouteRefreshExecutionStatus",
    "SCHEMA_VERSION",
]
