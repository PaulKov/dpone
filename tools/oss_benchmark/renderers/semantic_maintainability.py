"""Markdown renderer for semantic maintainability evidence."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_float, format_int


def render_semantic_maintainability_section(payload: dict[str, Any]) -> str:
    evidence = payload.get("semantic_maintainability") or {}
    summary = evidence.get("summary") or {}
    if not summary:
        return ""
    lines = [
        "",
        "## Semantic Maintainability Deep Scan",
        "",
        "This layer looks inside the code shape, not just repository size. It highlights god modules, god classes, god functions, SOLID/DI contract pressure, Clean Code responsibility spread, and DRY/KISS signals that determine whether the framework can scale without turning into a hard-to-change platform.",
        "",
        "![Semantic maintainability](assets/oss-semantic-maintainability.svg)",
        "",
        "![God object radar](assets/oss-god-object-radar.svg)",
        "",
        "| Project | Overall | Status | God object | SOLID/DI | DRY/KISS | Boundary | God modules | God classes | God functions |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for slug, item in sorted(summary.items()):
        lines.append(
            "| "
            f"{item.get('name', slug)} | "
            f"{format_int(item.get('overall_score'))} | "
            f"{item.get('status', 'n/a')} | "
            f"{format_int(item.get('god_object_score'))} | "
            f"{format_int(item.get('solid_di_score'))} | "
            f"{format_int(item.get('dry_kiss_score'))} | "
            f"{format_int(item.get('boundary_score'))} | "
            f"{format_int(item.get('god_module_count'))} | "
            f"{format_int(item.get('god_class_count'))} | "
            f"{format_int(item.get('god_function_count'))} |"
        )
    lines.extend(_render_god_object_radar(summary))
    lines.extend(_render_solid_di(summary))
    lines.extend(_render_dry_kiss(summary))
    lines.extend(_render_risks(evidence))
    return "\n".join(lines)


def _render_god_object_radar(summary: dict[str, Any]) -> list[str]:
    lines = ["", "### God object radar", "", "| Project | Kind | Module | Object | Value |", "|---|---|---|---|---:|"]
    for slug, item in sorted(summary.items()):
        for obj in (item.get("top_god_objects") or [])[:5]:
            lines.append(
                "| "
                f"{item.get('name', slug)} | "
                f"`{obj.get('kind', 'n/a')}` | "
                f"`{obj.get('module', 'n/a')}` | "
                f"`{obj.get('name', 'n/a')}` | "
                f"{format_int(obj.get('value'))} {obj.get('unit', '')} |"
            )
    if len(lines) == 5:
        lines.append("| n/a | n/a | n/a | n/a | 0 |")
    return lines


def _render_solid_di(summary: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### SOLID/DI/Clean Code evidence",
        "",
        "| Project | Interface density | Direct impl imports | Finding |",
        "|---|---:|---:|---|",
    ]
    for slug, item in sorted(summary.items()):
        findings = "; ".join(finding.get("message", "n/a") for finding in (item.get("solid_di_findings") or [])[:2])
        lines.append(
            "| "
            f"{item.get('name', slug)} | "
            f"{format_float(item.get('interface_density'), digits=3)} | "
            f"{format_int(item.get('direct_implementation_imports'))} | "
            f"{findings} |"
        )
    return lines


def _render_dry_kiss(summary: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### DRY/KISS responsibility signals",
        "",
        "| Project | Responsibility spread | Branch hotspots | Finding |",
        "|---|---:|---:|---|",
    ]
    for slug, item in sorted(summary.items()):
        findings = "; ".join(finding.get("message", "n/a") for finding in (item.get("dry_kiss_findings") or [])[:2])
        lines.append(
            "| "
            f"{item.get('name', slug)} | "
            f"{format_int(item.get('responsibility_spread'))} | "
            f"{format_int(item.get('branch_hotspot_count'))} | "
            f"{findings} |"
        )
    return lines


def _render_risks(evidence: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### Semantic maintainability risk register",
        "",
        "| Priority | Project | Module | Reason | Recommendation |",
        "|---|---|---|---|---|",
    ]
    for item in (evidence.get("risk_register") or [])[:10]:
        lines.append(
            "| "
            f"{item.get('priority', 'P?')} | "
            f"{item.get('project', 'n/a')} | "
            f"`{item.get('module', 'n/a')}` | "
            f"{item.get('reason', 'n/a')} | "
            f"{item.get('recommendation', 'n/a')} |"
        )
    if len(lines) == 5:
        lines.append("| n/a | n/a | n/a | No generated semantic risk crossed thresholds. | Keep current gates. |")
    lines.append("")
    return lines
