"""CDC promotion gate value objects."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dpone.ops.cdc.models import CdcStreamKey

SCHEMA_VERSION = "dpone.cdc_promotion_gate.v1"

PROMOTION_EVIDENCE_DOMAINS: tuple[str, ...] = (
    "cdc_apply_gate",
    "cdc_handoff_gate",
    "cdc_observability_gate",
    "cdc_recovery_gate",
    "cdc_schema_evolution_gate",
    "cdc_stream_identity_consistency",
    "cdc_offset_promotion_decision",
)


@dataclass(frozen=True, slots=True)
class CdcPromotionEvidenceItem:
    """Normalized evidence item emitted by one CDC promotion gate check."""

    name: str
    passed: bool
    summary: str
    blockers: tuple[str, ...]
    details: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "kind": "cdc_promotion_gate",
            "passed": self.passed,
            "summary": self.summary,
            "blockers": list(self.blockers),
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class CdcPromotionDecision:
    """Pure go/no-go decision for CDC stream promotion."""

    passed: bool
    production_ready: bool
    promote_offsets: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "production_ready": self.production_ready,
            "promote_offsets": self.promote_offsets,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
        }


@dataclass(frozen=True, slots=True)
class CdcPromotionReport:
    """Stable JSON/Markdown contract for CDC production promotion review."""

    stream: CdcStreamKey
    passed: bool
    production_ready: bool
    promote_offsets: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    evidence: tuple[CdcPromotionEvidenceItem, ...]
    evidence_artifacts: Mapping[str, str]
    upstream_artifacts: Mapping[str, str]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "route": self.stream.route.to_dict(),
            "passed": self.passed,
            "production_ready": self.production_ready,
            "promote_offsets": self.promote_offsets,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "evidence": [item.to_dict() for item in self.evidence],
            "evidence_artifacts": dict(self.evidence_artifacts),
            "upstream_artifacts": dict(self.upstream_artifacts),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# CDC promotion gate",
            "",
            f"- Route: `{self.stream.route.case_id}`",
            f"- Stream: `{self.stream.stream_id}`",
            f"- Production ready: `{self.production_ready}`",
            f"- Promote offsets: `{self.promote_offsets}`",
            f"- Passed: `{self.passed}`",
            "",
            "## Evidence",
            "",
            "| evidence | status | summary | blockers | path |",
            "|---|---|---|---|---|",
        ]
        for item in self.evidence:
            status = "pass" if item.passed else "fail"
            blockers = ", ".join(item.blockers) if item.blockers else "none"
            lines.append(
                f"| `{item.name}` | {status} | {item.summary} | `{blockers}` | "
                f"`{self.evidence_artifacts.get(item.name, '')}` |"
            )
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings) if self.warnings else lines.append("- none")
        lines.extend(["", "## Next actions", ""])
        lines.extend(f"- {item}" for item in self.next_actions) if self.next_actions else lines.append("- none")
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")
