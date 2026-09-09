"""Route release certification bundle contracts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol

SCHEMA_VERSION = "dpone.route_certification_bundle.v1"


@dataclass(frozen=True, slots=True)
class RouteCertificationStage:
    """One stage in the route certification bundle."""

    name: str
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
class RouteCertificationBundleReport:
    """Stable JSON/Markdown release certification receipt for one route."""

    release: str
    profile: str
    route: _RouteKey
    route_profile: _RouteProfile | None
    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    required_evidence: tuple[str, ...]
    stages: tuple[RouteCertificationStage, ...]
    artifact_index: dict[str, str]
    output_dir: str
    json_path: str
    markdown_path: str
    matrix_claim: Mapping[str, object] | None = None

    @property
    def evidence_status(self) -> str:
        """Promote only a bound vendor-live bundle to trusted evidence."""

        if self.passed and self.profile == "vendor_live" and self.matrix_claim is not None:
            return "PASS"
        return "UNVERIFIED"

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "release": self.release,
            "profile": self.profile,
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
            "stages": [stage.to_dict() for stage in self.stages],
            "stage_index": {stage.name: stage.to_dict() for stage in self.stages},
            "artifact_index": dict(self.artifact_index),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }
        if self.matrix_claim is not None:
            payload["matrix_claim"] = dict(self.matrix_claim)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# Route certification bundle",
            "",
            f"- Release: `{self.release}`",
            f"- Profile: `{self.profile}`",
            f"- Route: `{self.route.case_id}`",
            f"- Passed: `{self.passed}`",
            f"- Evidence status: `{self.evidence_status}`",
            f"- Level: `{self.level}`",
            f"- Score: `{self.score}`",
            f"- Matrix claim: `{'present' if self.matrix_claim is not None else 'absent'}`",
            "",
            "## Stages",
            "",
            "| stage | required | status | sha256 | summary | path |",
            "|---|---:|---|---|---|---|",
        ]
        for stage in self.stages:
            status = "pass" if stage.passed else "fail"
            lines.append(
                f"| `{stage.name}` | `{stage.required}` | {status} | `{stage.sha256}` | "
                f"{stage.summary} | `{stage.path}` |"
            )
        lines.extend(["", "## Artifact index", ""])
        for name, path in self.artifact_index.items():
            lines.append(f"- `{name}`: `{path}`")
        lines.extend(["", "## Required evidence", ""])
        lines.extend(f"- `{item}`" for item in self.required_evidence)
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
                    "- Attach `route_certification_bundle.json` to release review.",
                    "- Keep every upstream evidence artifact immutable for this release candidate.",
                    "- Publish only after the bundle level is `certified`.",
                ]
            )
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


class _RouteKey(Protocol):
    @property
    def case_id(self) -> str: ...

    def to_dict(self) -> dict[str, str]: ...


class _RouteProfile(Protocol):
    def to_dict(self) -> dict[str, object]: ...
