"""CDC poison quarantine inspection service."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from dpone.runtime.cdc.poison_models import load_poison_quarantine

INSPECTION_SCHEMA_VERSION = "dpone.cdc_quarantine_inspection.v1"


@dataclass(frozen=True, slots=True)
class CdcQuarantineInspectionReport:
    """Stable report for one CDC quarantine inspection."""

    record_count: int
    reason_counts: dict[str, int]
    action_counts: dict[str, int]
    passed: bool
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": INSPECTION_SCHEMA_VERSION,
            "record_count": self.record_count,
            "reason_counts": dict(self.reason_counts),
            "action_counts": dict(self.action_counts),
            "passed": self.passed,
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = ["# CDC quarantine inspection", "", f"- Records: `{self.record_count}`", "", "## Reasons", ""]
        lines.extend(f"- `{reason}`: `{count}`" for reason, count in sorted(self.reason_counts.items()))
        lines.extend(["", "## Actions", ""])
        lines.extend(f"- `{action}`: `{count}`" for action, count in sorted(self.action_counts.items()))
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


class CdcQuarantineInspectionService:
    """Read a CDC poison quarantine artifact and summarize operator actions."""

    def inspect(self, *, output_dir: str | Path, quarantine_json: str | Path) -> CdcQuarantineInspectionReport:
        directory = Path(output_dir)
        records = load_poison_quarantine(quarantine_json).get("records", [])
        if not isinstance(records, list):
            raise ValueError("CDC poison quarantine records must be an array")
        reason_counts: Counter[str] = Counter()
        action_counts: Counter[str] = Counter()
        for record in records:
            if isinstance(record, dict):
                reason_counts[str(record.get("reason", "unknown"))] += 1
                action_counts[str(record.get("action", "unknown"))] += 1
        report = CdcQuarantineInspectionReport(
            record_count=len(records),
            reason_counts=dict(reason_counts),
            action_counts=dict(action_counts),
            passed=True,
            output_dir=str(directory),
            json_path=str(directory / "cdc_quarantine_inspection.json"),
            markdown_path=str(directory / "cdc_quarantine_inspection.md"),
        )
        report.write()
        return report


__all__ = ["CdcQuarantineInspectionReport", "CdcQuarantineInspectionService"]
