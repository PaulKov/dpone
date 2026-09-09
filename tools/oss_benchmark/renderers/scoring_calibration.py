"""Markdown renderer for benchmark score calibration evidence."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_float, format_int, format_percent


def render_scoring_calibration_section(payload: dict[str, Any]) -> str:
    evidence = payload.get("scoring_calibration") or {}
    summary = evidence.get("summary") or {}
    if not summary:
        return ""
    lines = [
        "",
        "## Scoring Validity & Calibration",
        "",
        "This section makes the executive scorecard harder to misread and harder to game. It shows raw scores next to language/repo-normalized scores, tests whether small threshold changes would reshuffle rankings, and flags signals that could inflate maintainability without improving the design.",
        "",
        "![Score calibration](assets/oss-score-calibration.svg)",
        "",
        "![Sensitivity analysis](assets/oss-score-sensitivity.svg)",
        "",
        "![Normalized vs raw](assets/oss-normalized-vs-raw.svg)",
        "",
        "### Language/repo normalization",
        "",
        "| Project | Raw | Normalized | Adjustment | Profile | Repo scale | Python | JVM | TS/JS | Other |",
        "|---|---:|---:|---:|---|---|---:|---:|---:|---:|",
    ]
    for slug, item in sorted(summary.items()):
        mix = item.get("language_mix") or {}
        lines.append(
            "| "
            f"{item.get('name', slug)} | "
            f"{format_int(item.get('raw_score'))} | "
            f"{format_int(item.get('normalized_score'))} | "
            f"{format_float(item.get('normalization_adjustment'), digits=1)} | "
            f"`{item.get('normalization_profile', 'n/a')}` | "
            f"`{item.get('repo_scale', 'n/a')}` | "
            f"{format_percent(mix.get('python'))} | "
            f"{format_percent(mix.get('jvm'))} | "
            f"{format_percent(mix.get('ts_js'))} | "
            f"{format_percent(mix.get('other'))} |"
        )
    lines.extend(_render_sensitivity(evidence))
    lines.extend(_render_guardrails(evidence))
    lines.extend(_render_explanation_cards(evidence))
    return "\n".join(lines)


def _render_sensitivity(evidence: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### Sensitivity analysis",
        "",
        "| Project | +/-10% threshold swing | Stability | Most sensitive metric | Low-threshold score | High-threshold score |",
        "|---|---:|---|---|---:|---:|",
    ]
    for slug, item in sorted((evidence.get("sensitivity") or {}).items()):
        lines.append(
            "| "
            f"{slug} | "
            f"{format_int(item.get('threshold_10_percent_swing'))} | "
            f"`{item.get('rank_stability', 'n/a')}` | "
            f"`{item.get('most_sensitive_metric', 'n/a')}` | "
            f"{format_int(item.get('low_threshold_score'))} | "
            f"{format_int(item.get('high_threshold_score'))} |"
        )
    return lines


def _render_guardrails(evidence: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### Anti-gaming guardrails",
        "",
        "| Project | Guardrail | Status | Message |",
        "|---|---|---|---|",
    ]
    for slug, items in sorted((evidence.get("guardrails") or {}).items()):
        for item in items:
            lines.append(
                "| "
                f"{slug} | "
                f"{item.get('label', item.get('id', 'n/a'))} | "
                f"`{item.get('status', 'n/a')}` | "
                f"{item.get('message', 'n/a')} |"
            )
    return lines


def _render_explanation_cards(evidence: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### Score explanation cards",
        "",
        "| Project | Positive drivers | Negative drivers | Next best action |",
        "|---|---|---|---|",
    ]
    for slug, card in sorted((evidence.get("explanation_cards") or {}).items()):
        positives = "; ".join(card.get("positive_drivers") or ["n/a"])
        negatives = "; ".join(card.get("negative_drivers") or ["n/a"])
        lines.append(f"| {slug} | {positives} | {negatives} | {card.get('next_best_action', 'n/a')} |")
    lines.append("")
    return lines
