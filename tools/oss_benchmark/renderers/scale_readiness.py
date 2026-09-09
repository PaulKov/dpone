"""Markdown renderer for scale-readiness evidence."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_float, format_int


def render_scale_readiness_section(payload: dict[str, Any]) -> str:
    evidence = payload.get("scale_readiness") or {}
    summary = evidence.get("summary") or {}
    if not summary:
        return ""
    lines = [
        "",
        "## Scale Readiness & Growth Simulation",
        "",
        "This layer answers the main benchmark objection: dpone is smaller today, so the report models whether its architecture can keep quality when the codebase grows toward comparator scale. It is a planning proxy, not a product roadmap or runtime performance forecast.",
        "",
        "![Scale readiness](assets/oss-scale-readiness.svg)",
        "",
        "![Architecture runway](assets/oss-architecture-runway.svg)",
        "",
        "![Quality headroom](assets/oss-quality-headroom.svg)",
        "",
        "### Quality headroom",
        "",
        "| Project | Connector slots before yellow | Connector slots before red | Max module headroom | P90 fan-out headroom | Semantic headroom | Normalized score headroom |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for slug, item in sorted((evidence.get("quality_headroom") or {}).items()):
        lines.append(
            "| "
            f"{slug} | "
            f"{format_int(item.get('connector_slots_before_yellow'))} | "
            f"{format_int(item.get('connector_slots_before_red'))} | "
            f"{format_float(item.get('max_module_loc_headroom'), digits=1)} | "
            f"{format_float(item.get('p90_fan_out_headroom'), digits=2)} | "
            f"{format_float(item.get('semantic_score_headroom'), digits=1)} | "
            f"{format_float(item.get('normalized_score_headroom'), digits=1)} |"
        )
    lines.extend(_render_runway(evidence))
    lines.extend(_render_scenarios(evidence))
    lines.extend(_render_projection(evidence))
    return "\n".join(lines)


def _render_runway(evidence: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### Architecture runway",
        "",
        "| Project | Runway score | Status | Growth ceiling SLOC | Primary constraint |",
        "|---|---:|---|---:|---|",
    ]
    for slug, item in sorted((evidence.get("architecture_runway") or {}).items()):
        lines.append(
            "| "
            f"{slug} | "
            f"{format_int(item.get('score'))} | "
            f"`{item.get('status', 'n/a')}` | "
            f"{format_int(item.get('growth_ceiling_sloc'))} | "
            f"{item.get('primary_constraint', 'n/a')} |"
        )
    return lines


def _render_scenarios(evidence: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### Scale scenarios",
        "",
        "| Project | Scenario | Growth | Projected SLOC | Projected max module | Projected P90 fan-out | Projected maintainability | Risk |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for slug, scenarios in sorted((evidence.get("scale_scenarios") or {}).items()):
        for scenario in scenarios:
            lines.append(
                "| "
                f"{slug} | "
                f"{scenario.get('label', scenario.get('id', 'n/a'))} | "
                f"{format_float(scenario.get('growth_multiplier'), digits=2)}x | "
                f"{format_int(scenario.get('projected_sloc'))} | "
                f"{format_int(scenario.get('projected_max_module_loc'))} | "
                f"{format_float(scenario.get('projected_p90_fan_out'), digits=2)} | "
                f"{format_int(scenario.get('projected_maintainability'))} | "
                f"`{scenario.get('risk_level', 'n/a')}` |"
            )
    return lines


def _render_projection(evidence: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### Comparator-scale projection",
        "",
        "| Project | Worst scenario | Runway warning | Next target |",
        "|---|---|---|---|",
    ]
    warnings = evidence.get("warnings") or []
    for slug, item in sorted((evidence.get("summary") or {}).items()):
        first_warning = next((warning.get("message") for warning in warnings if warning.get("project") == slug), "none")
        next_target = _next_target(item, first_warning)
        lines.append(
            f"| {item.get('name', slug)} | `{item.get('worst_scenario', 'n/a')}` | {first_warning} | {next_target} |"
        )
    lines.append("")
    return lines


def _next_target(summary: dict[str, Any], warning: str) -> str:
    if warning != "none":
        return "Raise architecture runway before adding broad connector surface."
    if summary.get("overall_status") == "ready":
        return "Keep current module and fan-out budgets while expanding coverage."
    return "Increase connector slots before yellow by reducing primary constraints."
