"""Public contracts for route refresh snapshot capture evidence."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal, Protocol

SCHEMA_VERSION = "dpone.route_refresh_snapshot_capture.v1"
SNAPSHOT_SCHEMA_VERSION = "dpone.route_refresh_snapshot.v1"

RouteRefreshSnapshotCaptureStatus = Literal["captured", "failed", "blocked"]
RouteRefreshSnapshotSide = Literal["source", "sink"]


@dataclass(frozen=True, slots=True)
class RouteRefreshSnapshotCaptureRequest:
    """One route refresh chunk request passed to row readers."""

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
    execution_path: str
    chunk_artifact_path: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    boundary_column: str
    type_hints: dict[str, str]


@dataclass(frozen=True, slots=True)
class RouteRefreshSnapshotChunk:
    """One captured side snapshot inside a route refresh snapshot document."""

    ordinal: int
    idempotency_key: str
    start: str
    end: str
    snapshot: _RouteRefreshSideSnapshot

    def to_dict(self) -> dict[str, object]:
        return {
            "ordinal": self.ordinal,
            "idempotency_key": self.idempotency_key,
            "start": self.start,
            "end": self.end,
            **self.snapshot.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class RouteRefreshSnapshotDocument:
    """Stable JSON document consumed by route refresh verification."""

    route: _RouteIdentity
    side: RouteRefreshSnapshotSide
    dataset: str
    runner_id: str
    route_refresh_execution_json: str
    execution_sha256: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    boundary_column: str
    type_hints: dict[str, str]
    chunks: tuple[RouteRefreshSnapshotChunk, ...]
    json_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "side": self.side,
            "dataset": self.dataset,
            "runner_id": self.runner_id,
            "route_refresh_execution_json": self.route_refresh_execution_json,
            "execution_sha256": self.execution_sha256,
            "columns": list(self.columns),
            "key_columns": list(self.key_columns),
            "boundary_column": self.boundary_column,
            "type_hints": dict(self.type_hints),
            "chunks": [chunk.to_dict() for chunk in self.chunks],
            "json_path": self.json_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def write(self) -> None:
        path = Path(self.json_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json(), encoding="utf-8")


@dataclass(frozen=True, slots=True)
class RouteRefreshChunkSnapshotCapture:
    """Capture result for one source/sink chunk pair."""

    ordinal: int
    idempotency_key: str
    start: str
    end: str
    status: str
    passed: bool
    source: _RouteRefreshSideSnapshot
    sink: _RouteRefreshSideSnapshot
    summary: str
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "ordinal": self.ordinal,
            "idempotency_key": self.idempotency_key,
            "start": self.start,
            "end": self.end,
            "status": self.status,
            "passed": self.passed,
            "source": self.source.to_dict(),
            "sink": self.sink.to_dict(),
            "summary": self.summary,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class RouteRefreshSnapshotCaptureArtifact:
    """Artifact referenced by a route refresh snapshot capture receipt."""

    name: str
    path: str
    required: bool
    exists: bool
    sha256: str
    summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteRefreshSnapshotCaptureReport:
    """Stable JSON/Markdown receipt for snapshot capture."""

    route: _RouteIdentity
    profile: _RouteProfile | None
    dataset: str
    runner_id: str
    route_refresh_execution_json: str
    execution_sha256: str
    status: RouteRefreshSnapshotCaptureStatus
    passed: bool
    ready_for_verification: bool
    summary: dict[str, int]
    chunks: tuple[RouteRefreshChunkSnapshotCapture, ...]
    artifacts: tuple[RouteRefreshSnapshotCaptureArtifact, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    output_dir: str
    source_snapshot_json: str
    sink_snapshot_json: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "dataset": self.dataset,
            "runner_id": self.runner_id,
            "route_refresh_execution_json": self.route_refresh_execution_json,
            "execution_sha256": self.execution_sha256,
            "status": self.status,
            "passed": self.passed,
            "ready_for_verification": self.ready_for_verification,
            "summary": self.summary,
            "chunks": [chunk.to_dict() for chunk in self.chunks],
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "artifact_index": {artifact.name: artifact.to_dict() for artifact in self.artifacts},
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "output_dir": self.output_dir,
            "source_snapshot_json": self.source_snapshot_json,
            "sink_snapshot_json": self.sink_snapshot_json,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Route refresh snapshot capture",
            "",
            f"- Route: `{self.route.case_id}`",
            f"- Dataset: `{self.dataset}`",
            f"- Runner: `{self.runner_id}`",
            f"- Status: `{self.status}`",
            f"- Passed: `{self.passed}`",
            f"- Ready for verification: `{self.ready_for_verification}`",
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
                "| # | status | source rows | sink rows | summary |",
                "|---:|---|---:|---:|---|",
            ]
        )
        for chunk in self.chunks:
            lines.append(
                f"| `{chunk.ordinal}` | {chunk.status} | `{chunk.source.row_count}` | "
                f"`{chunk.sink.row_count}` | {chunk.summary} |"
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
            lines.append("- Pass captured snapshot JSON files to `dpone ops route-refresh-verify`.")
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


class _RouteRefreshSideSnapshot(Protocol):
    def to_dict(self) -> dict[str, object]: ...


__all__ = [
    "RouteRefreshChunkSnapshotCapture",
    "RouteRefreshSnapshotCaptureArtifact",
    "RouteRefreshSnapshotCaptureReport",
    "RouteRefreshSnapshotCaptureRequest",
    "RouteRefreshSnapshotCaptureStatus",
    "RouteRefreshSnapshotChunk",
    "RouteRefreshSnapshotDocument",
    "SCHEMA_VERSION",
]
