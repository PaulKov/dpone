"""Live state and reconciliation certification evidence."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.ops.artifact_validation import EvidenceArtifactReader, EvidenceArtifactStatus
from dpone.ops.ids import utc_now_iso


@dataclass(frozen=True, slots=True)
class LiveStateReconciliationCertificationReport:
    profile: str
    passed: bool
    generated_at: str
    blockers: tuple[str, ...]
    state_backends: tuple[str, ...]
    state_checks: tuple[str, ...]
    delete_reconciliation_checked: bool
    artifacts: tuple[EvidenceArtifactStatus, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile,
            "passed": self.passed,
            "evidence_status": self.evidence_status,
            "generated_at": self.generated_at,
            "blockers": list(self.blockers),
            "state_backends": list(self.state_backends),
            "state_checks": list(self.state_checks),
            "delete_reconciliation_checked": self.delete_reconciliation_checked,
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
            "# dpone live state and reconciliation certification",
            "",
            f"- Profile: `{self.profile}`",
            f"- Passed: `{self.passed}`",
            f"- Evidence status: `{self.evidence_status}`",
            f"- Generated at: `{self.generated_at}`",
            f"- State backends: `{', '.join(self.state_backends) or '-'}`",
            f"- State checks: `{', '.join(self.state_checks) or '-'}`",
            f"- Delete reconciliation checked: `{self.delete_reconciliation_checked}`",
            "",
            "| artifact | required | exists | passed | sha256 | summary |",
            "|---|---:|---:|---:|---|---|",
        ]
        for item in self.artifacts:
            lines.append(
                f"| `{item.name}` | `{item.required}` | `{item.exists}` | `{item.passed}` | "
                f"`{item.sha256}` | {item.summary} |"
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
                "1. If state evidence is missing, run Postgres/MSSQL state integration markers before promotion.",
                "2. If delete reconciliation is missing, run snapshot reconciliation for physical deletes.",
                "3. If an artifact is red, fix the originating gate instead of editing this summary.",
                "4. Attach this report to connector certification and release evidence packs.",
                "",
            ]
        )
        return "\n".join(lines)

    @property
    def evidence_status(self) -> str:
        """Expose the observed reconciliation result without production promotion."""

        return "PASS" if self.passed else "FAIL"


class LiveStateReconciliationCertificationService:
    """Aggregates state backend and delete reconciliation artifacts for release gating."""

    def __init__(self, artifact_reader: EvidenceArtifactReader | None = None) -> None:
        self._artifact_reader = artifact_reader or EvidenceArtifactReader()

    def certify(
        self,
        *,
        output_dir: str | Path,
        profile: str,
        artifacts: Mapping[str, str | Path],
        required: Sequence[str] = ("state", "reconciliation"),
    ) -> LiveStateReconciliationCertificationReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        statuses = self._artifact_reader.read_many(artifacts=artifacts, required=required)
        state_backends = tuple(sorted(_state_backends(statuses)))
        state_checks = tuple(sorted(_state_checks(statuses)))
        delete_checked = _delete_reconciliation_checked(statuses)
        blockers = tuple(
            item
            for item in (
                *(status.blocker for status in statuses if status.blocker),
                None if delete_checked else "delete_reconciliation.missing",
            )
            if item
        )
        json_path = directory / "live_state_reconciliation.json"
        markdown_path = directory / "live_state_reconciliation.md"
        report = LiveStateReconciliationCertificationReport(
            profile=profile,
            passed=not blockers,
            generated_at=utc_now_iso(),
            blockers=blockers,
            state_backends=state_backends,
            state_checks=state_checks,
            delete_reconciliation_checked=delete_checked,
            artifacts=statuses,
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
        )
        json_path.write_text(report.to_json(), encoding="utf-8")
        markdown_path.write_text(report.to_markdown(), encoding="utf-8")
        return report


def _state_backends(statuses: tuple[EvidenceArtifactStatus, ...]) -> set[str]:
    values: set[str] = set()
    for status in statuses:
        payload = status.payload
        values.update(_string_list(payload.get("state_backends")))
        value = payload.get("state_backend")
        if isinstance(value, str) and value:
            values.add(value)
    return values


def _state_checks(statuses: tuple[EvidenceArtifactStatus, ...]) -> set[str]:
    values: set[str] = set()
    for status in statuses:
        values.update(_string_list(status.payload.get("checks")))
        values.update(_string_list(status.payload.get("state_checks")))
    return values


def _delete_reconciliation_checked(statuses: tuple[EvidenceArtifactStatus, ...]) -> bool:
    for status in statuses:
        payload = status.payload
        delete_payload = payload.get("delete_reconciliation")
        if isinstance(delete_payload, Mapping) and delete_payload.get("passed") is True:
            return True
        if _int_value(payload.get("physical_deletes_checked")) > 0:
            return True
    return False


def _string_list(value: Any) -> set[str]:
    if not isinstance(value, list):
        return set()
    return {str(item) for item in value if str(item)}


def _int_value(value: object) -> int:
    if not isinstance(value, str | int | float | bool):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


__all__ = [
    "LiveStateReconciliationCertificationReport",
    "LiveStateReconciliationCertificationService",
]
