"""Route release gate report contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from dpone.ops.routes.models import RouteKey, RouteProfile

SCHEMA_VERSION = "dpone.route_release_gate.v1"


@dataclass(frozen=True, slots=True)
class RouteReleaseGateEvidence:
    """One normalized route release evidence artifact."""

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
class RouteReleaseGateReport:
    """Stable JSON/Markdown route release go/no-go receipt."""

    release: str
    route: RouteKey
    profile: RouteProfile | None
    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    required_evidence: tuple[str, ...]
    evidence: tuple[RouteReleaseGateEvidence, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    @property
    def evidence_status(self) -> str:
        """Expose the result of the strict route evidence gate."""

        return "PASS" if self.passed else "FAIL"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "release": self.release,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "passed": self.passed,
            "evidence_status": self.evidence_status,
            "level": self.level,
            "score": self.score,
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
            "# Route release gate",
            "",
            f"- Release: `{self.release}`",
            f"- Route: `{self.route.case_id}`",
            f"- Passed: `{self.passed}`",
            f"- Evidence status: `{self.evidence_status}`",
            f"- Level: `{self.level}`",
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
                "## Evidence",
                "",
                "| evidence | required | status | route | sha256 | summary | path |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for item in self.evidence:
            status = "missing" if item.missing else ("pass" if item.passed else "fail")
            route_status = "match" if item.route_matched else "mismatch"
            lines.append(
                f"| `{item.name}` | `{item.required}` | {status} | {route_status} | "
                f"`{item.sha256}` | {item.summary} | `{item.path}` |"
            )
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
                    "- Attach `route_release_gate.json` and `route_release_gate.md` to release review.",
                    "- Keep upstream evidence immutable for the release candidate.",
                ]
            )
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")
