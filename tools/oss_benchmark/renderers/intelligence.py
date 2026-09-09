"""Markdown sections for benchmark scoring intelligence."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import (
    find_project,
    format_delta,
    format_float,
    format_gate_value,
    format_int,
    format_score,
    project_name,
)


def render_benchmark_intelligence_sections(payload: dict[str, Any]) -> list[str]:
    """Render explainable scoring, baseline deltas and remediation backlog sections."""

    lines: list[str] = []
    lines.extend(_render_score_explanations(payload))
    lines.extend(_render_regression_summary(payload))
    lines.extend(_render_candidate_quality_delta(payload))
    lines.extend(_render_remediation_backlog(payload))
    return lines


def _render_score_explanations(payload: dict[str, Any]) -> list[str]:
    explanations = payload.get("score_explanations") or {}
    projects = explanations.get("projects") or {}
    lines = [
        "",
        "## Explainable scoring",
        "",
        "This section makes the executive score auditable. Each Industrial Maintainability Index is the sum of bounded component scores; the raw component inputs remain in the JSON evidence.",
        "",
    ]
    if not projects:
        lines.extend(["No score explanations are available for this payload.", ""])
        return lines
    for slug, explanation in projects.items():
        lines.append(f"### {project_name(find_project(payload.get('projects', []), slug))} scoring")
        lines.append("")
        lines.append(
            f"Formula: `{explanation.get('formula', 'n/a')}` -> **{format_int(explanation.get('score'))}** ({explanation.get('band', 'n/a')})."
        )
        lines.append("")
        lines.append("| Component | Score | Max | Actual | Reason |")
        lines.append("|---|---:|---:|---|---|")
        for component in explanation.get("components") or []:
            lines.append(
                "| "
                f"{component.get('label', 'n/a')} | "
                f"{format_float(component.get('score'), digits=1)} | "
                f"{format_int(component.get('max_score'))} | "
                f"{component.get('actual', 'n/a')} | "
                f"{component.get('reason', 'n/a')} |"
            )
        rubric = explanation.get("rubric") or {}
        solid = rubric.get("solid") or {}
        clean = rubric.get("clean_oop") or {}
        lines.append("")
        lines.append(
            f"Rubric evidence: SOLID `{format_score(solid.get('score'))}`, Clean OOP `{format_score(clean.get('score'))}`."
        )
        lines.append("")
    return lines


def _render_regression_summary(payload: dict[str, Any]) -> list[str]:
    regression = payload.get("regression_summary") or {}
    lines = [
        "",
        "## Regression summary",
        "",
        "The regression summary compares dpone against the previous evidence file used for the refresh. It is optimized for CI review: lower module size, lower fan-out, lower cross-slice ratio, higher test footprint and fresh metrics are treated as better.",
        "",
        f"Current status: **{regression.get('status', 'n/a')}**.",
        "",
    ]
    changes = regression.get("changes") or []
    if not changes:
        lines.extend(["No previous dpone evidence was available for comparison.", ""])
        return lines
    lines.append("| Metric | Previous | Current | Delta | Direction | Status |")
    lines.append("|---|---:|---:|---:|---|---|")
    for change in changes:
        lines.append(
            "| "
            f"{change.get('label', 'n/a')} | "
            f"{format_gate_value(change.get('previous'))} | "
            f"{format_gate_value(change.get('current'))} | "
            f"{format_delta(change.get('delta'))} | "
            f"{change.get('direction', 'n/a')} | "
            f"{change.get('status', 'n/a')} |"
        )
    lines.append("")
    return lines


def _render_candidate_quality_delta(payload: dict[str, Any]) -> list[str]:
    delta = payload.get("candidate_quality_delta") or {}
    lines = [
        "",
        "## Candidate quality delta",
        "",
        "This PR-oriented view compares the current dpone benchmark evidence with the selected baseline evidence. It keeps SOLID/Clean Code budgets explicit: no hidden god modules, no silent fan-out growth, and no unlabelled test-footprint erosion.",
        "",
        f"Current status: **{delta.get('status', 'n/a')}**.",
        "",
    ]
    baseline_source = delta.get("baseline_source")
    if baseline_source:
        lines.extend([f"Baseline evidence: `{baseline_source}`.", ""])
    budgets = delta.get("quality_budgets") or []
    if budgets:
        lines.append("| Budget | Actual | Target | Status |")
        lines.append("|---|---:|---:|---|")
        for budget in budgets:
            lines.append(
                "| "
                f"{budget.get('label', 'n/a')} | "
                f"{format_gate_value(budget.get('actual'))} | "
                f"{budget.get('operator', '')} {format_gate_value(budget.get('threshold'))} | "
                f"{budget.get('status', 'n/a')} |"
            )
        lines.append("")
    else:
        lines.extend(["No baseline evidence was available for candidate budget comparison.", ""])

    modules = delta.get("changed_modules") or []
    if modules:
        lines.extend(
            [
                "The watchlist includes changed modules plus unchanged production modules that already sit above the watch threshold.",
                "",
            ]
        )
        lines.append("| Module | Change | LOC delta | SLOC delta | Fan-out delta | Risk |")
        lines.append("|---|---|---:|---:|---:|---|")
        for module in modules:
            lines.append(
                "| "
                f"`{module.get('path', 'n/a')}` | "
                f"{module.get('status', 'n/a')} | "
                f"{format_delta(module.get('loc_delta'))} | "
                f"{format_delta(module.get('sloc_delta'))} | "
                f"{format_delta(module.get('fan_out_delta'))} | "
                f"{module.get('risk', 'n/a')} |"
            )
        lines.append("")
    else:
        lines.extend(["No production module crossed the candidate watchlist.", ""])
    return lines


def _render_remediation_backlog(payload: dict[str, Any]) -> list[str]:
    backlog = payload.get("remediation_backlog") or {}
    items = backlog.get("items") or []
    lines = [
        "",
        "## Remediation backlog",
        "",
        "The backlog translates benchmark evidence into engineering work. It is generated from quality gates, architecture hotspots, stale metric groups and dpone watch thresholds so the benchmark remains actionable.",
        "",
    ]
    if not items:
        lines.extend(["No remediation backlog items were generated for this run.", ""])
        return lines
    lines.append("| Priority | Project | Category | Evidence | Recommendation |")
    lines.append("|---|---|---|---|---|")
    for item in items:
        lines.append(
            "| "
            f"{item.get('priority', 'n/a')} | "
            f"{item.get('project', 'n/a')} | "
            f"{item.get('category', 'n/a')} | "
            f"{item.get('evidence', 'n/a')} | "
            f"{item.get('recommendation', 'n/a')} |"
        )
    lines.append("")
    return lines
