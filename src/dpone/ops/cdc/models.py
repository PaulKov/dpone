"""CDC snapshot handoff value objects and report contracts."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dpone.ops.routes.models import RouteEvidenceItem, RouteKey

SCHEMA_VERSION = "dpone.cdc_handoff.v1"


@dataclass(frozen=True, slots=True)
class CdcStreamKey:
    """Canonical identity for one CDC source stream and target dataset."""

    route: RouteKey
    source_dataset: str
    target_dataset: str

    @classmethod
    def of(
        cls,
        *,
        source: str,
        sink: str,
        strategy: str,
        source_dataset: str,
        target_dataset: str,
    ) -> CdcStreamKey:
        return cls(
            route=RouteKey.of(source, sink, _cdc_strategy(strategy)),
            source_dataset=str(source_dataset).strip(),
            target_dataset=str(target_dataset).strip(),
        )

    @property
    def stream_id(self) -> str:
        return f"{self.route.case_id}__{_dataset_id(self.source_dataset)}__{_dataset_id(self.target_dataset)}"

    @property
    def colon_id(self) -> str:
        return f"{self.route.colon_id}:{self.source_dataset}:{self.target_dataset}"

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = dict(self.route.to_dict())
        payload.update(
            {
                "source_dataset": self.source_dataset,
                "target_dataset": self.target_dataset,
                "stream_id": self.stream_id,
                "colon_id": self.colon_id,
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class CdcHandoffProfile:
    """Static CDC handoff metadata for one route profile."""

    route: RouteKey
    docs_link: str
    source_backend: str
    sink_apply_mode: str
    snapshot_boundary_kind: str
    offset_kind: str
    native_apply_path: str
    required_evidence: tuple[str, ...]
    default_slo_hints: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "route": self.route.to_dict(),
            "docs_link": self.docs_link,
            "source_backend": self.source_backend,
            "sink_apply_mode": self.sink_apply_mode,
            "snapshot_boundary_kind": self.snapshot_boundary_kind,
            "offset_kind": self.offset_kind,
            "native_apply_path": self.native_apply_path,
            "required_evidence": list(self.required_evidence),
            "default_slo_hints": dict(self.default_slo_hints),
        }


@dataclass(frozen=True, slots=True)
class CdcHandoffDecision:
    """Pure policy decision before report paths are attached."""

    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "level": self.level,
            "score": self.score,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
        }


@dataclass(frozen=True, slots=True)
class CdcHandoffReport:
    """Stable JSON/Markdown contract for CDC handoff CLI and CI gates."""

    stream: CdcStreamKey
    profile: CdcHandoffProfile | None
    passed: bool
    level: str
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    evidence: tuple[RouteEvidenceItem, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "stream": self.stream.to_dict(),
            "route": self.stream.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "passed": self.passed,
            "level": self.level,
            "score": self.score,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "evidence": [item.to_dict() for item in self.evidence],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone CDC snapshot handoff",
            "",
            f"- Route: `{self.stream.route.case_id}`",
            f"- Stream: `{self.stream.stream_id}`",
            f"- Passed: `{self.passed}`",
            f"- Level: `{self.level}`",
            f"- Score: `{self.score}`",
        ]
        if self.profile:
            lines.extend(
                [
                    f"- Source backend: `{self.profile.source_backend}`",
                    f"- Sink apply mode: `{self.profile.sink_apply_mode}`",
                    f"- Snapshot boundary: `{self.profile.snapshot_boundary_kind}`",
                    f"- Native apply path: `{self.profile.native_apply_path}`",
                    f"- Docs: `{self.profile.docs_link}`",
                ]
            )
        lines.extend(
            [
                "",
                "## Evidence",
                "",
                "| evidence | kind | required | status | sha256 | summary | path |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for item in self.evidence:
            status = "missing" if item.missing else ("pass" if item.passed else "fail")
            lines.append(
                f"| `{item.name}` | `{item.kind}` | `{item.required}` | {status} | "
                f"`{item.sha256}` | {item.summary} | `{item.path}` |"
            )
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{item}`" for item in self.blockers) if self.blockers else lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings) if self.warnings else lines.append("- none")
        lines.extend(["", "## Next actions", ""])
        if self.next_actions:
            lines.extend(f"- {item}" for item in self.next_actions)
        else:
            lines.append("- CDC handoff evidence is complete for the configured policy.")
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


def _cdc_strategy(value: str) -> str:
    normalized = str(value).strip().lower().replace("-", "_")
    return "cdc" if normalized == "cdc_apply" else normalized


def _dataset_id(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
    return normalized or "dataset"
