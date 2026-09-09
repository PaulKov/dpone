"""Release-level route certification report contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

SCHEMA_VERSION = "dpone.route_certification_release.v1"
DEFAULT_REQUIRED_ROUTE_CERTIFICATION_ROUTES: tuple[str, ...] = (
    "postgres_to_mssql__incremental_merge",
    "mssql_to_clickhouse__incremental_merge",
)


@dataclass(frozen=True, slots=True)
class RouteCertificationReleaseItem:
    """One normalized route certification bundle in a release gate."""

    route_case_id: str
    path: str
    required: bool
    missing: bool
    passed: bool
    level: str
    profile: str
    bundle_release: str
    score: float | None
    modified_at: str
    route_matched: bool
    profile_matched: bool
    sha256: str
    summary: str
    blockers: tuple[str, ...]
    artifact_index: dict[str, str]

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["blockers"] = list(self.blockers)
        payload["artifact_index"] = dict(self.artifact_index)
        return payload


@dataclass(frozen=True, slots=True)
class RouteCertificationReleaseReport:
    """Stable JSON/Markdown release-level route certification gate."""

    release: str
    profile: str
    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    required_routes: tuple[str, ...]
    routes: tuple[RouteCertificationReleaseItem, ...]
    artifact_index: dict[str, str]
    output_dir: str
    json_path: str
    markdown_path: str
    release_notes_path: str

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
            "required_routes": list(self.required_routes),
            "routes": [item.to_dict() for item in self.routes],
            "route_index": {item.route_case_id: item.to_dict() for item in self.routes},
            "artifact_index": dict(self.artifact_index),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
            "release_notes_path": self.release_notes_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Route certification release",
            "",
            f"- Release: `{self.release}`",
            f"- Profile: `{self.profile}`",
            f"- Passed: `{self.passed}`",
            f"- Level: `{self.level}`",
            f"- Score: `{self.score}`",
            "",
            "## Routes",
            "",
            "| route | required | status | profile | sha256 | summary | path |",
            "|---|---:|---|---|---|---|---|",
        ]
        for item in self.routes:
            status = "missing" if item.missing else ("certified" if item.passed else "blocked")
            lines.append(
                f"| `{item.route_case_id}` | `{item.required}` | {status} | `{item.profile}` | "
                f"`{item.sha256}` | {item.summary} | `{item.path}` |"
            )
        lines.extend(["", "## Artifact index", ""])
        for name, path in self.artifact_index.items():
            lines.append(f"- `{name}`: `{path}`")
        lines.extend(["", "## Blockers", ""])
        if self.blockers:
            lines.extend(f"- `{item}`" for item in self.blockers)
        else:
            lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        if self.warnings:
            lines.extend(f"- `{item}`" for item in self.warnings)
        else:
            lines.append("- none")
        lines.extend(["", "## Operator runbook", ""])
        if self.next_actions:
            lines.extend(f"- {item}" for item in self.next_actions)
        else:
            lines.extend(
                [
                    "- Attach `route_certification_release.json` to the release candidate review.",
                    "- Attach `route_certification_release_notes.md` to GitHub release notes.",
                    "- Tag the release only while this report remains `release_ready`.",
                ]
            )
        lines.append("")
        return "\n".join(lines)

    def to_release_notes(self) -> str:
        lines = [
            f"# Route certification for {self.release}",
            "",
            f"- Profile: `{self.profile}`",
            f"- Level: `{self.level}`",
            f"- Score: `{self.score}`",
            "",
            "| route | level | profile | bundle |",
            "|---|---|---|---|",
        ]
        for item in self.routes:
            lines.append(f"| `{item.route_case_id}` | `{item.level}` | `{item.profile}` | `{item.path}` |")
        if self.blockers:
            lines.extend(["", "## Blockers", ""])
            lines.extend(f"- `{item}`" for item in self.blockers)
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")
        Path(self.release_notes_path).write_text(self.to_release_notes(), encoding="utf-8")
