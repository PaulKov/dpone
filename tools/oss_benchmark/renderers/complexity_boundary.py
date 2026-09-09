"""Markdown renderer for complexity and boundary discipline evidence."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_int as _format_int


def render_complexity_boundary_section(payload: dict[str, Any]) -> str:
    evidence = payload.get("complexity_boundary") or {}
    summary = evidence.get("summary") or {}
    if not summary:
        return ""
    lines = [
        "",
        "## Complexity & Boundary Discipline",
        "",
        "![Complexity and boundary discipline](assets/oss-complexity-boundary.svg)",
        "",
        "Enterprise change risk falls when decision paths stay small, boundaries stay explicit, and runtime code depends on thin contracts instead of concrete implementations. This section turns those Clean Code, SOLID, DRY, KISS and DI signals into reviewable benchmark evidence.",
        "",
        "| Project | Overall | Status | Complexity | Boundary | DI | Max complexity | P90 complexity | Violations | Risks |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for slug, item in sorted(summary.items()):
        lines.append(
            "| "
            f"{item.get('name', slug)} | "
            f"{_format_int(item.get('overall_score'))} | "
            f"{item.get('status', 'n/a')} | "
            f"{_format_int(item.get('complexity_score'))} | "
            f"{_format_int(item.get('boundary_score'))} | "
            f"{_format_int(item.get('di_score'))} | "
            f"{_format_int(item.get('max_complexity'))} | "
            f"{_format_int(item.get('p90_complexity'))} | "
            f"{_format_int(item.get('boundary_violation_count'))} | "
            f"{_format_int(item.get('risk_count'))} |"
        )
    lines.extend(_render_top_units(summary))
    lines.extend(_render_boundary_violations(summary))
    lines.extend(_render_risk_register(evidence))
    return "\n".join(lines)


def _render_top_units(summary: dict[str, Any]) -> list[str]:
    lines = ["", "### Complexity hotspots", "", "| Project | Module | Unit | Complexity |", "|---|---|---|---:|"]
    for slug, item in sorted(summary.items()):
        for unit in (item.get("top_complex_units") or [])[:5]:
            lines.append(
                "| "
                f"{item.get('name', slug)} | "
                f"`{unit.get('module', 'n/a')}` | "
                f"`{unit.get('unit', 'module')}` | "
                f"{_format_int(unit.get('complexity'))} |"
            )
    if len(lines) == 4:
        lines.append("| n/a | n/a | n/a | 0 |")
    return lines


def _render_boundary_violations(summary: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### Boundary violations",
        "",
        "| Project | Severity | Kind | Module | Message |",
        "|---|---|---|---|---|",
    ]
    for slug, item in sorted(summary.items()):
        for violation in (item.get("boundary_violations") or [])[:5]:
            lines.append(
                "| "
                f"{item.get('name', slug)} | "
                f"{violation.get('severity', 'n/a')} | "
                f"`{violation.get('kind', 'n/a')}` | "
                f"`{violation.get('path', 'n/a')}` | "
                f"{violation.get('message', 'n/a')} |"
            )
    if len(lines) == 4:
        lines.append("| n/a | n/a | n/a | n/a | No boundary violations crossed the benchmark rules. |")
    return lines


def _render_risk_register(evidence: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### Maintainability risk register",
        "",
        "| Priority | Project | Module | Reason | Recommended action |",
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
        lines.append("| n/a | n/a | n/a | No generated complexity or boundary risk. | Keep current quality gates. |")
    lines.append("")
    return lines
