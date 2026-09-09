"""Route refresh/backfill/resync plan public contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal, Protocol

SCHEMA_VERSION = "dpone.route_refresh_plan.v1"

RouteRefreshReason = Literal[
    "initial_backfill",
    "manual_resync",
    "dq_repair",
    "schema_backfill",
    "retention_gap",
    "range_replay",
]
RouteRefreshStatus = Literal["ready", "approval_required", "blocked"]
RouteRefreshWindowKind = Literal["integer", "timestamp", "partition", "state"]


@dataclass(frozen=True, slots=True)
class RouteRefreshWindow:
    """Source-side replay window requested for one route refresh plan."""

    kind: str
    start: str
    end: str
    chunk_size: int | None = None
    partition: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "start": self.start,
            "end": self.end,
            "chunk_size": self.chunk_size,
            "partition": self.partition,
        }


@dataclass(frozen=True, slots=True)
class RouteRefreshChunk:
    """One idempotent unit of refresh work."""

    ordinal: int
    start: str
    end: str
    partition: str
    source_boundary: str
    sink_boundary: str
    idempotency_key: str
    estimated_rows: int | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteRefreshStateRewind:
    """State rewind assessment for route replay requests."""

    current_state: str
    target_state: str
    safe_to_rewind: bool
    requires_approval: bool
    reason: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteRefreshApproval:
    """Manual approval decision carried by the refresh plan."""

    required: bool
    approved: bool
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "required": self.required,
            "approved": self.approved,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class RouteRefreshEvidence:
    """Normalized evidence artifact used by the refresh policy."""

    name: str
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

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["blockers"] = list(self.blockers)
        return payload


@dataclass(frozen=True, slots=True)
class RouteRefreshPlanReport:
    """Stable JSON/Markdown contract for route refresh planning."""

    route: _RouteIdentity
    profile: _RouteProfile | None
    dataset: str
    reason: str
    status: RouteRefreshStatus
    passed: bool
    window: RouteRefreshWindow
    chunks: tuple[RouteRefreshChunk, ...]
    state_rewind: RouteRefreshStateRewind
    approval: RouteRefreshApproval
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    required_evidence: tuple[str, ...]
    evidence: tuple[RouteRefreshEvidence, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "dataset": self.dataset,
            "reason": self.reason,
            "status": self.status,
            "passed": self.passed,
            "window": self.window.to_dict(),
            "chunks": [chunk.to_dict() for chunk in self.chunks],
            "state_rewind": self.state_rewind.to_dict(),
            "approval": self.approval.to_dict(),
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
            "# Route refresh plan",
            "",
            f"- Route: `{self.route.case_id}`",
            f"- Dataset: `{self.dataset}`",
            f"- Reason: `{self.reason}`",
            f"- Status: `{self.status}`",
            f"- Passed: `{self.passed}`",
            f"- Approval required: `{self.approval.required}`",
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
                "## Window",
                "",
                f"- Kind: `{self.window.kind}`",
                f"- Start: `{self.window.start}`",
                f"- End: `{self.window.end}`",
                f"- Chunk size: `{self.window.chunk_size}`",
                f"- Partition: `{self.window.partition}`",
                "",
                "## Chunks",
                "",
                "| # | start | end | partition | idempotency key | source boundary | sink boundary |",
                "|---:|---|---|---|---|---|---|",
            ]
        )
        for chunk in self.chunks:
            lines.append(
                f"| `{chunk.ordinal}` | `{chunk.start}` | `{chunk.end}` | `{chunk.partition}` | "
                f"`{chunk.idempotency_key}` | `{chunk.source_boundary}` | `{chunk.sink_boundary}` |"
            )
        lines.extend(
            [
                "",
                "## Evidence",
                "",
                "| evidence | kind | required | status | route | sha256 | summary | path |",
                "|---|---|---|---|---|---|---|---|",
            ]
        )
        for item in self.evidence:
            status = "missing" if item.missing else ("pass" if item.passed else "fail")
            route_status = "match" if item.route_matched else "mismatch"
            lines.append(
                f"| `{item.name}` | `{item.kind}` | `{item.required}` | {status} | {route_status} | "
                f"`{item.sha256}` | {item.summary} | `{item.path}` |"
            )
        lines.extend(
            [
                "",
                "## State rewind",
                "",
                f"- Current state: `{self.state_rewind.current_state}`",
                f"- Target state: `{self.state_rewind.target_state}`",
                f"- Safe to rewind: `{self.state_rewind.safe_to_rewind}`",
                f"- Requires approval: `{self.state_rewind.requires_approval}`",
                f"- Reason: `{self.state_rewind.reason}`",
                "",
                "## Approval",
                "",
                f"- Required: `{self.approval.required}`",
                f"- Approved: `{self.approval.approved}`",
                f"- Reasons: `{', '.join(self.approval.reasons)}`",
                "",
                "## Blockers",
                "",
            ]
        )
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings) if self.warnings else lines.append("- none")
        lines.extend(["", "## Runbook", ""])
        if self.next_actions:
            lines.extend(f"- {item}" for item in self.next_actions)
        else:
            lines.extend(
                [
                    "- Attach `route_refresh_plan.json` and `route_refresh_plan.md` to the route release evidence.",
                    "- Execute chunks idempotently and promote route state only after downstream quality checks pass.",
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

    @property
    def colon_id(self) -> str: ...

    def to_dict(self) -> dict[str, str]: ...


class _RouteProfile(Protocol):
    @property
    def docs_link(self) -> str: ...

    @property
    def native_fast_path(self) -> str: ...

    def to_dict(self) -> dict[str, object]: ...


__all__ = [
    "RouteRefreshApproval",
    "RouteRefreshChunk",
    "RouteRefreshEvidence",
    "RouteRefreshPlanReport",
    "RouteRefreshReason",
    "RouteRefreshStateRewind",
    "RouteRefreshStatus",
    "RouteRefreshWindow",
    "RouteRefreshWindowKind",
    "SCHEMA_VERSION",
]
