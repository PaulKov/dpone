"""Report rendering for ClickHouse CDC materialization."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def materialization_report_json(report: Any) -> str:
    return json.dumps(report.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def materialization_report_markdown(report: Any) -> str:
    lines = [
        "# ClickHouse CDC materialization",
        "",
        f"- CDC log: `{report.plan.cdc_dataset}`",
        f"- Target: `{report.plan.target_dataset}`",
        f"- Unique key: `{', '.join(report.plan.unique_key)}`",
        f"- Delete mode: `{report.delete_mode}`",
        f"- Passed: `{report.passed}`",
        f"- Source events: `{report.rows_source_events}`",
        f"- Rows materialized: `{report.rows_materialized}`",
        f"- Deleted latest keys: `{report.rows_deleted}`",
        "",
        "## Blockers",
        "",
    ]
    _append_list(lines, report.blockers)
    lines.extend(["", "## Warnings", ""])
    _append_list(lines, report.warnings)
    lines.extend(["", "## Metrics", ""])
    lines.extend(f"- `{name}`: `{value}`" for name, value in report.metrics.items())
    lines.append("")
    return "\n".join(lines)


def write_materialization_report(report: Any) -> None:
    Path(report.output_dir).mkdir(parents=True, exist_ok=True)
    Path(report.json_path).write_text(report.to_json(), encoding="utf-8")
    Path(report.markdown_path).write_text(report.to_markdown(), encoding="utf-8")


def _append_list(lines: list[str], values: tuple[str, ...]) -> None:
    lines.extend(f"- `{item}`" for item in values) if values else lines.append("- none")


__all__ = [
    "materialization_report_json",
    "materialization_report_markdown",
    "write_materialization_report",
]
