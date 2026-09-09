"""Markdown renderer for the Refactor ROI roadmap."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_int as _format_int


def render_refactor_roi_section(payload: dict[str, Any]) -> str:
    roadmap = payload.get("refactor_roi") or {}
    summary = roadmap.get("summary") or {}
    items = roadmap.get("items") or []
    if not summary and not items:
        return ""
    lines = [
        "",
        "## Refactor ROI Roadmap",
        "",
        "![Refactor ROI roadmap](assets/oss-refactor-roi-roadmap.svg)",
        "",
        "Quality is managed here as an investment portfolio: every generated refactor candidate is ranked by expected debt reduction, actionability, effort feasibility, and target architecture fit.",
        "",
        "### Quality debt estimate",
        "",
        "| Project | Debt points | Status | Top driver | Quick wins | Strategic refactors |",
        "|---|---:|---|---|---:|---:|",
    ]
    for slug, item in sorted(summary.items()):
        lines.append(
            "| "
            f"{item.get('name', slug)} | "
            f"{_format_int(item.get('debt_points'))} | "
            f"{item.get('status', 'n/a')} | "
            f"{item.get('top_debt_driver', 'n/a')} | "
            f"{_format_int(item.get('quick_win_count'))} | "
            f"{_format_int(item.get('strategic_refactor_count'))} |"
        )
    lines.extend(_render_items(items))
    lines.extend(_render_targets(items))
    return "\n".join(lines)


def _render_items(items: list[dict[str, Any]]) -> list[str]:
    lines = [
        "",
        "### ROI-ranked refactor backlog",
        "",
        "| Rank | Project | Module | ROI | Impact | Effort | Debt | Quadrant |",
        "|---:|---|---|---:|---:|---:|---:|---|",
    ]
    for item in items[:12]:
        lines.append(
            "| "
            f"{_format_int(item.get('rank'))} | "
            f"{item.get('project', 'n/a')} | "
            f"`{item.get('module', 'n/a')}` | "
            f"{_format_int(item.get('roi_score'))} | "
            f"{_format_int(item.get('impact_score'))} | "
            f"{_format_int(item.get('effort_score'))} | "
            f"{_format_int(item.get('debt_points'))} | "
            f"{item.get('quadrant', 'n/a')} |"
        )
    if len(lines) == 5:
        lines.append("| 0 | n/a | n/a | 0 | 0 | 0 | 0 | n/a |")
    return lines


def _render_targets(items: list[dict[str, Any]]) -> list[str]:
    lines = [
        "",
        "### Target architecture recommendations",
        "",
        "| Rank | Module | Target architecture | Recommended action |",
        "|---:|---|---|---|",
    ]
    for item in items[:10]:
        lines.append(
            "| "
            f"{_format_int(item.get('rank'))} | "
            f"`{item.get('module', 'n/a')}` | "
            f"{item.get('target_architecture', 'n/a')} | "
            f"{item.get('recommended_action', 'n/a')} |"
        )
    if len(lines) == 5:
        lines.append("| 0 | n/a | n/a | No generated refactor candidate. |")
    lines.append("")
    return lines
