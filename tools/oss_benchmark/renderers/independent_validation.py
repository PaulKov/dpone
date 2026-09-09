"""Markdown renderer for independent analyzer validation evidence."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_float, format_int


def render_independent_validation_section(payload: dict[str, Any]) -> str:
    evidence = payload.get("independent_validation") or {}
    summary = evidence.get("summary") or {}
    if not summary:
        return ""
    lines = [
        "",
        "## Independent Analyzer Cross-Validation & Audit Pack",
        "",
        "This audit layer makes the benchmark easier to trust: internal metrics remain the source of record, while external analyzer command metadata and cross-checks show where independent tools agree, warn, or are unavailable.",
        "",
        "**External analyzer execution:** manual CI installs and runs `tokei`, `cloc`, `radon`, and `lizard` when available. If a refresh cannot execute a tool but prior evidence exists, stale analyzer values remain visible with their last successful update instead of being overwritten.",
        "",
        "![Independent analyzer validation](assets/oss-independent-validation.svg)",
        "",
        "![Analyzer confidence](assets/oss-analyzer-confidence.svg)",
        "",
        "### Validation confidence",
        "",
        "| Project | Confidence | Band | LOC/SLOC status | Complexity status | Stale analyzers | Unavailable analyzers |",
        "|---|---:|---|---|---|---:|---:|",
    ]
    for slug, item in sorted(summary.items()):
        lines.append(
            "| "
            f"{item.get('name', slug)} | "
            f"{format_int(item.get('confidence_score'))} | "
            f"`{item.get('validation_band', 'n/a')}` | "
            f"`{item.get('loc_sloc_status', 'n/a')}` | "
            f"`{item.get('complexity_status', 'n/a')}` | "
            f"{format_int(item.get('stale_analyzers'))} | "
            f"{format_int(item.get('unavailable_analyzers'))} |"
        )
    lines.extend(_render_command_ledger(evidence))
    lines.extend(_render_loc_checks(evidence))
    lines.extend(_render_complexity_checks(evidence))
    return "\n".join(lines)


def _render_command_ledger(evidence: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### Analyzer command ledger",
        "",
        "| Tool | Project | Status | Exit | Version | Command |",
        "|---|---|---|---:|---|---|",
    ]
    for command in evidence.get("analyzer_commands", [])[:24]:
        lines.append(
            "| "
            f"`{command.get('tool', 'n/a')}` | "
            f"{command.get('project', 'n/a')} | "
            f"`{command.get('status', 'n/a')}` | "
            f"{format_int(command.get('exit_code'))} | "
            f"{command.get('version') or 'n/a'} | "
            f"`{command.get('command', 'n/a')}` |"
        )
    return lines


def _render_loc_checks(evidence: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### LOC/SLOC cross-check",
        "",
        "| Project | Tool | Status | Benchmark SLOC | External SLOC | Delta |",
        "|---|---|---|---:|---:|---:|",
    ]
    for slug, checks in sorted((evidence.get("loc_sloc_cross_checks") or {}).items()):
        for check in checks:
            lines.append(
                "| "
                f"{slug} | "
                f"`{check.get('tool', 'n/a')}` | "
                f"`{check.get('status', 'n/a')}` | "
                f"{format_int(check.get('benchmark_sloc'))} | "
                f"{format_int(check.get('external_sloc'))} | "
                f"{format_float(check.get('delta_percent'), digits=2)}% |"
            )
    return lines


def _render_complexity_checks(evidence: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### Complexity cross-check",
        "",
        "| Project | Tool | Status | Avg complexity | Max complexity | Files |",
        "|---|---|---|---:|---:|---:|",
    ]
    for slug, checks in sorted((evidence.get("complexity_cross_checks") or {}).items()):
        for check in checks:
            lines.append(
                "| "
                f"{slug} | "
                f"`{check.get('tool', 'n/a')}` | "
                f"`{check.get('status', 'n/a')}` | "
                f"{format_float(check.get('external_avg_complexity'), digits=2)} | "
                f"{format_float(check.get('external_max_complexity'), digits=2)} | "
                f"{format_int(check.get('files'))} |"
            )
    lines.append("")
    return lines
