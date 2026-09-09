"""PR-summary renderer for independent analyzer validation."""

from __future__ import annotations

from typing import Any


def render_independent_validation_pr_section(payload: dict[str, Any]) -> list[str]:
    """Render compact analyzer cross-validation evidence for refresh PRs."""

    validation = payload.get("independent_validation") or {}
    summary = validation.get("summary") or {}
    if not summary:
        return []
    dpone = summary.get("dpone") or next(iter(summary.values()), {})
    coverage = (validation.get("analyzer_coverage") or {}).get("dpone") or {}
    unavailable = coverage.get("unavailable_tools") or []
    lines = [
        "### Independent Analyzer Cross-Validation",
        "",
        f"- dpone confidence: **{dpone.get('confidence_score', 'n/a')}/100** (`{dpone.get('validation_band', 'n/a')}`)",
        f"- LOC/SLOC cross-check: `{dpone.get('loc_sloc_status', 'n/a')}`.",
        f"- Complexity cross-check: `{dpone.get('complexity_status', 'n/a')}`.",
        f"- Stale analyzer values retained: `{dpone.get('stale_analyzers', 0)}`.",
        "- Analyzer command ledger: `independent_validation.analyzer_commands` in raw benchmark evidence.",
    ]
    if unavailable:
        lines.append(f"- Unavailable analyzers retained explicitly: `{', '.join(map(str, unavailable))}`.")
    lines.append("")
    return lines
