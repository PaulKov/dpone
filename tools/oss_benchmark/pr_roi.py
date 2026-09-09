"""PR summary section for Refactor ROI roadmap evidence."""

from __future__ import annotations

from typing import Any


def render_refactor_roi_pr_section(payload: dict[str, Any]) -> list[str]:
    roadmap = payload.get("refactor_roi") or {}
    summary = (roadmap.get("summary") or {}).get("dpone") or {}
    items = roadmap.get("items") or []
    if not summary and not items:
        return []
    lines = [
        "### Refactor ROI Roadmap",
        "",
        f"- Quality debt: `{_value(summary.get('debt_points'))}` points, "
        f"driver `{summary.get('top_debt_driver', 'n/a')}`, "
        f"quick wins `{_value(summary.get('quick_win_count'))}`",
    ]
    for item in items[:3]:
        lines.append(
            f"- #{_value(item.get('rank'))} `{item.get('module', 'n/a')}`: "
            f"ROI `{_value(item.get('roi_score'))}`, {item.get('quadrant', 'n/a')}; "
            f"{item.get('target_architecture', 'n/a')} - {item.get('reason', 'n/a')}"
        )
    if not items:
        lines.append("- No ROI-ranked refactor candidate was generated.")
    lines.append("")
    return lines


def _value(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:g}"
