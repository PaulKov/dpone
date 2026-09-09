"""PR-summary renderer for benchmark scoring calibration."""

from __future__ import annotations

from typing import Any


def render_scoring_calibration_pr_section(payload: dict[str, Any]) -> list[str]:
    evidence = payload.get("scoring_calibration") or {}
    summary = evidence.get("summary") or {}
    if not summary:
        return []
    dpone = summary.get("dpone") or next(iter(summary.values()), {})
    sensitivity = (evidence.get("sensitivity") or {}).get("dpone") or {}
    guardrails = (evidence.get("guardrails") or {}).get("dpone") or []
    card = (evidence.get("explanation_cards") or {}).get("dpone") or {}
    warning_count = sum(1 for item in guardrails if item.get("status") != "passed")
    positive = (card.get("positive_drivers") or dpone.get("top_positive_drivers") or ["n/a"])[0]
    action = card.get("next_best_action") or dpone.get("next_best_action") or "n/a"
    lines = [
        "### Scoring Validity & Calibration",
        "",
        f"- dpone normalized score: `{_value(dpone.get('normalized_score'))}` "
        f"(raw `{_value(dpone.get('raw_score'))}`, profile `{dpone.get('normalization_profile', 'n/a')}`)",
        f"- Sensitivity: `{_value(sensitivity.get('threshold_10_percent_swing'))}` point swing, "
        f"rank stability `{sensitivity.get('rank_stability', dpone.get('calibration_band', 'n/a'))}`",
        f"- Anti-gaming guardrails: `{warning_count}` warnings across `{len(guardrails)}` checks",
        f"- Strongest driver: {positive}",
        f"- Next best action: {action}",
        "",
    ]
    return lines


def _value(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:g}"
