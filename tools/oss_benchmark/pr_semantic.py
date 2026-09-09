"""PR-summary renderer for semantic maintainability."""

from __future__ import annotations

from typing import Any


def render_semantic_maintainability_pr_section(payload: dict[str, Any]) -> list[str]:
    evidence = payload.get("semantic_maintainability") or {}
    summary = evidence.get("summary") or {}
    if not summary:
        return []
    dpone = summary.get("dpone") or next(iter(summary.values()), {})
    god_objects = (
        int(dpone.get("god_module_count") or 0)
        + int(dpone.get("god_class_count") or 0)
        + int(dpone.get("god_function_count") or 0)
    )
    lines = [
        "### Semantic Maintainability Deep Scan",
        "",
        f"- Status: **{dpone.get('status', 'n/a')}** (overall `{_value(dpone.get('overall_score'))}`)",
        f"- SOLID/DI `{_value(dpone.get('solid_di_score'))}`, DRY/KISS `{_value(dpone.get('dry_kiss_score'))}`, boundary `{_value(dpone.get('boundary_score'))}`",
        f"- god objects: `{god_objects}` (modules `{_value(dpone.get('god_module_count'))}`, classes `{_value(dpone.get('god_class_count'))}`, functions `{_value(dpone.get('god_function_count'))}`)",
    ]
    risks = list(dpone.get("risk_register") or []) or list(evidence.get("risk_register") or [])
    if risks:
        risk = risks[0]
        lines.append(
            f"- Top semantic risk: **{risk.get('priority', 'P?')}** `{risk.get('module', 'n/a')}` - {risk.get('reason', 'n/a')}"
        )
    else:
        lines.append("- No semantic maintainability risk crossed the configured thresholds.")
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
