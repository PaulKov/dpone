"""Route live certification report contracts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from dpone.ops.routes.models import RouteKey, RouteProfile

SCHEMA_VERSION = "dpone.route_live_certification.v1"


@dataclass(frozen=True, slots=True)
class RouteLiveCertificationStep:
    """One operator-executed live certification step."""

    name: str
    command: str
    required: bool
    artifacts: tuple[str, ...]
    environment: tuple[str, ...] = tuple()
    services: tuple[str, ...] = tuple()
    summary: str = ""

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["artifacts"] = list(self.artifacts)
        payload["environment"] = list(self.environment)
        payload["services"] = list(self.services)
        return payload


@dataclass(frozen=True, slots=True)
class RouteLiveCertificationEvidence:
    """One normalized live route evidence artifact."""

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
class RouteLiveCertificationReport:
    """Stable JSON/Markdown route live certification bundle."""

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
    required_evidence: tuple[str, ...]
    evidence: tuple[RouteLiveCertificationEvidence, ...]
    harness_steps: tuple[RouteLiveCertificationStep, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    @property
    def evidence_status(self) -> str:
        """Trust only an executed live profile, never a mock-local projection."""

        if self.passed and self.profile in {"vendor_live", "real_local", "native_transfer"}:
            return "PASS"
        return "UNVERIFIED"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "release": self.release,
            "profile": self.profile,
            "row_count": self.row_count,
            "route": self.route.to_dict(),
            "route_profile": self.route_profile.to_dict() if self.route_profile else None,
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
            "harness_steps": [step.to_dict() for step in self.harness_steps],
            "evidence_bundle": {
                "name": "route_live_evidence_bundle",
                "path": self.json_path,
                "markdown_path": self.markdown_path,
            },
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Route live certification",
            "",
            f"- Release: `{self.release}`",
            f"- Profile: `{self.profile}`",
            f"- Route: `{self.route.case_id}`",
            f"- Passed: `{self.passed}`",
            f"- Evidence status: `{self.evidence_status}`",
            f"- Level: `{self.level}`",
            f"- Score: `{self.score}`",
            "- Evidence bundle: `route_live_evidence_bundle`",
            "",
            "## Harness steps",
            "",
            "| step | required | artifacts | command |",
            "|---|---:|---|---|",
        ]
        for step in self.harness_steps:
            artifacts = ", ".join(f"`{item}`" for item in step.artifacts) or "-"
            lines.append(f"| `{step.name}` | `{step.required}` | {artifacts} | `{step.command}` |")
        lines.extend(
            [
                "",
                "## Evidence",
                "",
                "| evidence | required | status | route | sha256 | summary | path |",
                "|---|---:|---|---|---|---|---|",
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
                    "- Run the emitted Docker-live or vendor-live commands in an opt-in environment.",
                    "- Feed `route_live_certification.json` to `dpone ops route-release-gate` as `route_live_evidence_bundle`.",
                    "- Keep upstream evidence immutable for the release candidate.",
                ]
            )
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")
