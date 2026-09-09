"""Operational reliability Markdown and SVG renderers."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import format_int as _format_int


def render_operational_reliability_section(matrix: dict[str, Any]) -> str:
    if not matrix:
        return ""
    tools = list(matrix.get("tools") or [])
    dimensions = list(matrix.get("dimensions") or [])
    summary = matrix.get("summary") or {}
    entries = _entries_by_key(matrix)
    lines = [
        "",
        "## Operational Reliability",
        "",
        "This layer compares runtime trust signals: retry/resume, checkpoint safety, zero duplicate retry behavior, reconciliation, CDC recovery, observability, SLOs, schema drift and release evidence. dpone uses measured JSON proof where available; external tools use public documentation posture.",
        "",
        "![Operational reliability posture](assets/oss-operational-reliability.svg)",
        "",
        "| Tool | Reliability score | Band | Evidence mode | Measured controls | Documented controls | Comparator note |",
        "|---|---:|---|---|---:|---:|---|",
    ]
    for tool in tools:
        item = summary.get(tool.get("slug", ""), {})
        lines.append(
            "| "
            f"{tool.get('name', tool.get('slug', 'n/a'))} | "
            f"{_format_int(item.get('score'))} | "
            f"{item.get('band', 'n/a')} | "
            f"{item.get('evidence_mode', 'n/a')} | "
            f"{_format_int(item.get('measured_controls'))} | "
            f"{_format_int(item.get('documented_controls'))} | "
            f"{item.get('comparator_note', tool.get('comparator_note', 'n/a'))} |"
        )
    lines.extend(["", "### Reliability control matrix", ""])
    lines.append("| Control | " + " | ".join(tool.get("name", tool.get("slug", "")) for tool in tools) + " |")
    lines.append("|---|" + "|".join("---" for _ in tools) + "|")
    for dimension in dimensions:
        row = [str(dimension.get("label", dimension.get("slug", "")))]
        for tool in tools:
            row.append(_level_badge(entries.get((tool.get("slug", ""), dimension.get("slug", "")), {})))
        lines.append("| " + " | ".join(row) + " |")
    lines.extend(["", "### Reliability evidence", ""])
    for tool in tools:
        slug = str(tool.get("slug", ""))
        strongest = _top_entries(matrix, slug)
        evidence = "; ".join(
            f"{entry.get('dimension', 'control')} `{entry.get('level', 'n/a')}` "
            f"({_source_link((entry.get('sources') or [''])[0])})"
            for entry in strongest
        )
        lines.append(f"- **{tool.get('name', slug)}:** {evidence or 'no reliability evidence detected'}.")
    lines.extend(
        [
            "",
            "Reliability scoring is evidence-bounded: `measured` requires local machine-readable proof; `documented` is public or local documentation; `partial` is a weaker posture signal. The strongest dpone differentiator is measured zero duplicate retry and recovery evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def render_operational_reliability_svg(payload: dict[str, Any]) -> str:
    matrix = payload.get("operational_reliability") or payload
    tools = list(matrix.get("tools") or [])
    summary = matrix.get("summary") or {}
    width = 920
    height = 118 + 48 * len(tools)
    rows = [
        _svg_header(width, height),
        f'<rect width="920" height="{height}" fill="#ffffff"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Operational reliability posture</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Measured recovery proof is separated from public documented posture.</text>',
    ]
    for idx, tool in enumerate(tools):
        slug = str(tool.get("slug", ""))
        item = summary.get(slug, {})
        score = int(item.get("score") or 0)
        y = 98 + idx * 48
        color = "#0f766e" if score >= 85 else "#2563eb" if score >= 70 else "#f59e0b" if score >= 45 else "#dc2626"
        rows.append(
            f'<text x="32" y="{y + 15}" font-size="13" font-weight="700" fill="#111827">{escape(tool.get("name", slug))}</text>'
        )
        rows.append(f'<rect x="230" y="{y}" width="520" height="20" rx="4" fill="#e2e8f0"/>')
        rows.append(f'<rect x="230" y="{y}" width="{max(2, int(score * 5.2))}" height="20" rx="4" fill="{color}"/>')
        rows.append(
            f'<text x="770" y="{y + 15}" font-size="12" fill="#334155">{score} / {escape(str(item.get("evidence_mode", "n/a")))}</text>'
        )
    rows.append("</svg>\n")
    return "\n".join(rows)


def _entries_by_key(matrix: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(str(entry.get("tool", "")), str(entry.get("dimension", ""))): entry for entry in matrix.get("entries", [])}


def _level_badge(entry: dict[str, Any]) -> str:
    return {
        "measured": "`measured`",
        "documented": "`documented`",
        "partial": "`partial`",
        "not_detected": "`not detected`",
    }.get(str(entry.get("level", "")), "`n/a`")


def _top_entries(matrix: dict[str, Any], tool_slug: str) -> list[dict[str, Any]]:
    rank = {"measured": 3, "documented": 2, "partial": 1, "not_detected": 0}
    entries = [entry for entry in matrix.get("entries", []) if entry.get("tool") == tool_slug]
    return sorted(entries, key=lambda entry: (-rank.get(str(entry.get("level")), 0), entry.get("dimension", "")))[:4]


def _source_link(source: str) -> str:
    if not source:
        return "no source"
    if source.startswith("http"):
        return f"[source]({source})"
    return f"`{source}`"


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">'
    )
