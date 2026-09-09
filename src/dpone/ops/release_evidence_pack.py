"""Final release evidence pack for manual publishing decisions."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from dpone.ops.artifact_validation import EvidenceArtifactReader, EvidenceArtifactStatus
from dpone.ops.ids import utc_now_iso

DEFAULT_REQUIRED_RELEASE_EVIDENCE = (
    "service_markers",
    "certification_pack",
    "performance_certification",
    "live_state_reconciliation",
    "pre_release_checklist",
    "evidence_chain",
)

NATIVE_TRANSFER_RELEASE_PROFILES = (
    "native_transfer",
    "critical_native_transfer",
    "postgres_mssql_native",
    "mssql_clickhouse_native",
)

NATIVE_TRANSFER_REQUIRED_RELEASE_EVIDENCE = (
    *DEFAULT_REQUIRED_RELEASE_EVIDENCE,
    "strategy_certification_bundle",
)


@dataclass(frozen=True, slots=True)
class ReleaseEvidencePackReport:
    release: str
    profile: str
    passed: bool
    generated_at: str
    blockers: tuple[str, ...]
    artifacts: tuple[EvidenceArtifactStatus, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "release": self.release,
            "profile": self.profile,
            "passed": self.passed,
            "generated_at": self.generated_at,
            "blockers": list(self.blockers),
            "artifacts": [item.to_dict() for item in self.artifacts],
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        blockers = "\n".join(f"- `{item}`" for item in self.blockers) if self.blockers else "- none"
        lines = [
            "# dpone release evidence pack",
            "",
            f"- Release: `{self.release}`",
            f"- Profile: `{self.profile}`",
            f"- Passed: `{self.passed}`",
            f"- Generated at: `{self.generated_at}`",
            "",
            "| artifact | required | exists | passed | sha256 | summary | path |",
            "|---|---:|---:|---:|---|---|---|",
        ]
        for item in self.artifacts:
            lines.append(
                f"| `{item.name}` | `{item.required}` | `{item.exists}` | `{item.passed}` | "
                f"`{item.sha256}` | {item.summary} | `{item.path}` |"
            )
        lines.extend(
            [
                "",
                "## Blockers",
                "",
                blockers,
                "",
                "## Runbook",
                "",
                "1. Missing required artifacts block release publication.",
                "2. Red required artifacts must be fixed at the originating gate.",
                "3. Keep this pack immutable after tag creation; rebuild it only as a new audit version.",
                "4. Attach the JSON file to GitHub release notes, CI summaries, or change approvals.",
                "",
            ]
        )
        return "\n".join(lines)


class ReleaseEvidencePackService:
    """Builds the final evidence bundle used to decide whether a release can ship."""

    def __init__(self, artifact_reader: EvidenceArtifactReader | None = None) -> None:
        self._artifact_reader = artifact_reader or EvidenceArtifactReader()

    def build(
        self,
        *,
        output_dir: str | Path,
        release: str,
        profile: str,
        artifacts: Mapping[str, str | Path],
        required: Sequence[str] | None = None,
    ) -> ReleaseEvidencePackReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        required_artifacts = tuple(required) if required is not None else required_release_evidence_for_profile(profile)
        statuses = self._artifact_reader.read_many(artifacts=artifacts, required=required_artifacts)
        blockers = tuple(status.blocker for status in statuses if status.blocker)
        json_path = directory / "release_evidence_pack.json"
        markdown_path = directory / "release_evidence_pack.md"
        report = ReleaseEvidencePackReport(
            release=release,
            profile=profile,
            passed=not blockers,
            generated_at=utc_now_iso(),
            blockers=blockers,
            artifacts=statuses,
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report


def required_release_evidence_for_profile(profile: str) -> tuple[str, ...]:
    normalized = profile.lower().replace("-", "_")
    if normalized in NATIVE_TRANSFER_RELEASE_PROFILES:
        return tuple(NATIVE_TRANSFER_REQUIRED_RELEASE_EVIDENCE)
    return tuple(DEFAULT_REQUIRED_RELEASE_EVIDENCE)


__all__ = [
    "DEFAULT_REQUIRED_RELEASE_EVIDENCE",
    "NATIVE_TRANSFER_RELEASE_PROFILES",
    "NATIVE_TRANSFER_REQUIRED_RELEASE_EVIDENCE",
    "ReleaseEvidencePackReport",
    "ReleaseEvidencePackService",
    "required_release_evidence_for_profile",
]
