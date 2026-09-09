"""PR summary section for complexity and boundary discipline."""

from __future__ import annotations

from typing import Any


def render_complexity_boundary_pr_section(payload: dict[str, Any]) -> list[str]:
    evidence = payload.get("complexity_boundary") or {}
    summary = evidence.get("summary") or {}
    if not summary:
        return []
    dpone = summary.get("dpone") or next(iter(summary.values()), {})
    risks = evidence.get("risk_register") or []
    lines = [
        "### Complexity & Boundary Discipline",
        "",
        f"- Status: **{dpone.get('status', 'n/a')}** "
        f"(overall `{_value(dpone.get('overall_score'))}`, "
        f"complexity `{_value(dpone.get('complexity_score'))}`, "
        f"boundary `{_value(dpone.get('boundary_score'))}`, "
        f"DI `{_value(dpone.get('di_score'))}`)",
        f"- Violations: `{_value(dpone.get('boundary_violation_count'))}`, risks: `{_value(dpone.get('risk_count'))}`",
    ]
    top_unit = (dpone.get("top_complex_units") or [{}])[0]
    if top_unit:
        lines.append(
            f"- Top complexity hotspot: `{top_unit.get('module', 'n/a')}` "
            f"`{top_unit.get('unit', 'module')}` complexity `{_value(top_unit.get('complexity'))}`"
        )
    if risks:
        risk = risks[0]
        lines.append(
            f"- Top risk: **{risk.get('priority', 'P?')}** `{risk.get('module', 'n/a')}` - {risk.get('reason', 'n/a')}"
        )
    else:
        lines.append("- No generated complexity or boundary risk crossed the configured thresholds.")
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
