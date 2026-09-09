"""Trend and architecture-delta markdown rendering."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import (
    find_project as _find_project,
)
from tools.oss_benchmark.payload_utils import (
    format_delta as _format_delta,
)
from tools.oss_benchmark.payload_utils import (
    format_float as _format_float,
)
from tools.oss_benchmark.payload_utils import (
    project_name as _project_name,
)


def render_trend_history_section(payload: dict[str, Any]) -> list[str]:
    trends = payload.get("trend_summary", {})
    lines = [
        "",
        "## Trend history",
        "",
        "![Industrial maintainability trend](assets/oss-quality-trend.svg)",
        "",
        "Trend data is stored in `docs/benchmarks/data/oss-code-quality-benchmark-history.json` ([open history JSON](data/oss-code-quality-benchmark-history.json)).",
        "",
    ]
    if not trends:
        lines.extend(
            [
                "This is the first tracked run for the current history file, so deltas will appear after the next refresh.",
                "",
            ]
        )
        return lines
    lines.extend(
        [
            "| Project | Index delta | Max Ce delta | P90 Ce delta | Largest LOC delta | Test footprint delta | Band movement |",
            "|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for slug, delta in sorted(trends.items()):
        project = _find_project(payload.get("projects", []), slug)
        lines.append(
            "| "
            f"{_project_name(project)} | "
            f"{_format_delta(delta.get('score'))} | "
            f"{_format_delta(delta.get('max_ce'))} | "
            f"{_format_delta(delta.get('p90_ce'))} | "
            f"{_format_delta(delta.get('largest_module_loc'))} | "
            f"{_format_delta(delta.get('test_footprint_ratio'))} | "
            f"{delta.get('previous_band', 'n/a')} -> {delta.get('current_band', 'n/a')} |"
        )
    lines.append("")
    return lines


def render_architecture_delta_section(payload: dict[str, Any]) -> list[str]:
    delta = payload.get("architecture_delta") or {}
    lines = [
        "",
        "## Architecture delta",
        "",
        "![Architecture delta](assets/oss-architecture-delta.svg)",
        "",
        "This view compares dpone architecture governance metrics against the previous benchmark evidence. It is optimized for PR review: lower fan-out and smaller hotspots are improvements, while test footprint is expected to move upward or stay stable.",
        "",
    ]
    if not delta.get("items"):
        lines.extend(["No comparable previous benchmark evidence is available for architecture deltas yet.", ""])
        return lines
    lines.extend(["| Metric | Previous | Current | Delta | Status |", "|---|---:|---:|---:|---|"])
    for item in delta.get("items", []):
        lines.append(
            "| "
            f"{item.get('label', 'metric')} | "
            f"{_format_delta_cell(item.get('previous'), item.get('previous_label'))} | "
            f"{_format_delta_cell(item.get('current'), item.get('current_label'))} | "
            f"{_format_delta(item.get('delta'))} | "
            f"{item.get('status', 'n/a')} |"
        )
    lines.append("")
    return lines


def _format_delta_cell(value: Any, label: Any = None) -> str:
    formatted = _format_float(value, digits=3)
    if label:
        return f"{formatted}<br>`{label}`"
    return formatted
