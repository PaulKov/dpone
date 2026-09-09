"""PR-summary renderer for scale-readiness evidence."""

from __future__ import annotations

from typing import Any


def render_scale_readiness_pr_section(payload: dict[str, Any]) -> list[str]:
    evidence = payload.get("scale_readiness") or {}
    summary = (evidence.get("summary") or {}).get("dpone") or {}
    if not summary:
        return []
    headroom = (evidence.get("quality_headroom") or {}).get("dpone") or {}
    warnings = [warning for warning in evidence.get("warnings", []) if warning.get("project") == "dpone"]
    first_warning = warnings[0].get("message") if warnings else "No scale-readiness warning crossed threshold."
    return [
        "### Scale Readiness & Growth Simulation",
        "",
        f"- Architecture runway: `{_value(summary.get('architecture_runway_score'))}` "
        f"({summary.get('overall_status', 'n/a')})",
        f"- Connector slots before yellow/red: `{_value(headroom.get('connector_slots_before_yellow'))}` / "
        f"`{_value(headroom.get('connector_slots_before_red'))}`",
        f"- Growth ceiling: `{_value(summary.get('growth_ceiling_sloc'))}` production SLOC",
        f"- Worst comparator-scale scenario: `{summary.get('worst_scenario', 'n/a')}`",
        f"- Runway warning: {first_warning}",
        "",
    ]


def _value(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:g}"
