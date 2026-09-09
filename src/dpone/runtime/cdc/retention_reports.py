"""Rendering and persistence helpers for CDC retention and resync reports."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def retention_markdown(report: Any) -> str:
    lines = [
        "# CDC retention check",
        "",
        f"- Pipeline: `{report.stream.pipeline_name}`",
        f"- Route: `{report.stream.route_id}`",
        f"- Stream: `{report.stream.stream_id}`",
        f"- Passed: `{report.passed}`",
        f"- Level: `{report.decision.level}`",
        f"- Committed offset: `{report.committed_offset.token if report.committed_offset else ''}`",
        f"- Min available offset: `{report.bounds.min_available_offset}`",
        f"- High watermark: `{report.bounds.high_watermark}`",
        "",
        "## Blockers",
        "",
    ]
    _append_list(lines, report.blockers)
    lines.extend(["", "## Warnings", ""])
    _append_list(lines, report.warnings)
    lines.extend(["", "## Next Actions", ""])
    _append_list(lines, report.decision.next_actions)
    lines.append("")
    return "\n".join(lines)


def resync_plan_markdown(report: Any) -> str:
    lines = [
        "# CDC resync plan",
        "",
        f"- Pipeline: `{report.stream.pipeline_name}`",
        f"- Route: `{report.stream.route_id}`",
        f"- Stream: `{report.stream.stream_id}`",
        f"- Passed: `{report.passed}`",
        f"- Retention level: `{report.retention_level}`",
        f"- Resync actions: `{report.plan.action_count}`",
        "",
        "## Blockers",
        "",
    ]
    _append_list(lines, report.blockers)
    lines.extend(["", "## Warnings", ""])
    _append_list(lines, report.warnings)
    lines.append("")
    return "\n".join(lines)


def resync_execution_markdown(report: Any) -> str:
    lines = [
        "# CDC resync execution",
        "",
        f"- Pipeline: `{report.stream.pipeline_name}`",
        f"- Route: `{report.stream.route_id}`",
        f"- Stream: `{report.stream.stream_id}`",
        f"- Passed: `{report.passed}`",
        f"- Committed: `{report.committed}`",
        f"- Resync actions: `{report.resync_actions}`",
        "",
        "## Blockers",
        "",
    ]
    _append_list(lines, report.blockers)
    lines.extend(["", "## Warnings", ""])
    _append_list(lines, report.warnings)
    lines.append("")
    return "\n".join(lines)


def write_report_files(report: Any) -> Any:
    Path(report.output_dir).mkdir(parents=True, exist_ok=True)
    Path(report.json_path).write_text(report.to_json(), encoding="utf-8")
    Path(report.markdown_path).write_text(report.to_markdown(), encoding="utf-8")
    return report


def _append_list(lines: list[str], values: tuple[str, ...]) -> None:
    lines.extend(f"- `{item}`" for item in values) if values else lines.append("- none")


__all__ = [
    "resync_execution_markdown",
    "resync_plan_markdown",
    "retention_markdown",
    "write_report_files",
]
