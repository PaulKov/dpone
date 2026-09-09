"""Native transfer runtime report evidence."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.runtime.lineage.partition_resume import PartitionResumePlan
from dpone.strategy_intelligence.partition_correctness import PartitionCorrectnessResult

SCHEMA_VERSION = "dpone.native_transfer.runtime_report.v1"


@dataclass(frozen=True)
class NativeTransferRuntimeReport:
    """Runtime evidence for one native transfer attempt."""

    run_id: str
    source_type: str
    sink_type: str
    strategy: str
    checkpoint_summary: dict[str, int]
    resume_plan: PartitionResumePlan
    partition_correctness: PartitionCorrectnessResult

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "created_at": datetime.now(UTC).isoformat(),
            "run_id": self.run_id,
            "source_type": self.source_type,
            "sink_type": self.sink_type,
            "strategy": self.strategy,
            "checkpoint_summary": self.checkpoint_summary,
            "resume_plan": self.resume_plan.to_dict(),
            "partition_correctness": self.partition_correctness.to_dict(),
            "passed": self.partition_correctness.passed,
        }


class NativeTransferRuntimeReportWriter:
    """Write native transfer runtime reports as JSON and Markdown."""

    def __init__(self, artifact_dir: str | Path) -> None:
        self._artifact_dir = Path(artifact_dir)

    def write(self, report: NativeTransferRuntimeReport) -> tuple[Path, Path]:
        self._artifact_dir.mkdir(parents=True, exist_ok=True)
        base = f"native_transfer_runtime_{_safe_name(report.run_id)}"
        json_path = self._artifact_dir / f"{base}.json"
        md_path = self._artifact_dir / f"{base}.md"
        payload = report.to_dict()
        json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        md_path.write_text(_render_markdown(payload), encoding="utf-8")
        return json_path, md_path


def _render_markdown(payload: dict[str, Any]) -> str:
    correctness = payload["partition_correctness"]
    resume = payload["resume_plan"]["summary"]
    lines = [
        f"# Native transfer runtime report: {payload['run_id']}",
        "",
        f"- schema_version: `{payload['schema_version']}`",
        f"- source_type: `{payload['source_type']}`",
        f"- sink_type: `{payload['sink_type']}`",
        f"- strategy: `{payload['strategy']}`",
        f"- passed: `{payload['passed']}`",
        f"- resume skip: `{resume.get('skip', 0)}`",
        f"- resume retry: `{resume.get('retry', 0)}`",
        f"- partition_correctness_passed: `{correctness['passed']}`",
        f"- total_source_rows: `{correctness['total_source_rows']}`",
        f"- total_target_rows: `{correctness['total_target_rows']}`",
        "",
    ]
    return "\n".join(lines)


def _safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)
