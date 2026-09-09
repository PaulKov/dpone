"""Governance and compliance Markdown and SVG renderers."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import format_int as _format_int


def render_governance_compliance_section(matrix: dict[str, Any]) -> str:
    if not matrix:
        return ""
    tools = list(matrix.get("tools") or [])
    dimensions = list(matrix.get("dimensions") or [])
    summary = matrix.get("summary") or {}
    entries = _entries_by_key(matrix)
    lines = [
        "",
        "## Governance & Compliance",
        "",
        "This layer scores auditability, lineage catalog coverage, data contracts, schema governance, policy gates, evidence chain, access and secrets posture, release certification, compliance runbooks, and data quality reconciliation. Closed-core vendors are included as managed governance posture comparators.",
        "",
        "![Governance and compliance posture](assets/oss-governance-compliance.svg)",
        "",
        "| Tool | Governance score | Band | Evidence mode | Strong controls | Managed controls | Comparator note |",
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
            f"{_format_int(item.get('strong_controls'))} | "
            f"{_format_int(item.get('managed_controls'))} | "
            f"{item.get('comparator_note', tool.get('comparator_note', 'n/a'))} |"
        )
    lines.extend(["", "### Governance control matrix", ""])
    lines.append("| Control | " + " | ".join(tool.get("name", tool.get("slug", "")) for tool in tools) + " |")
    lines.append("|---|" + "|".join("---" for _ in tools) + "|")
    for dimension in dimensions:
        row = [str(dimension.get("label", dimension.get("slug", "")))]
        for tool in tools:
            row.append(_level_badge(entries.get((tool.get("slug", ""), dimension.get("slug", "")), {})))
        lines.append("| " + " | ".join(row) + " |")
    lines.extend(["", "### Governance evidence", ""])
    for tool in tools:
        slug = str(tool.get("slug", ""))
        strongest = _top_entries(matrix, slug)
        evidence = "; ".join(
            f"{entry.get('dimension', 'control')} `{entry.get('level', 'n/a')}` "
            f"({_source_link((entry.get('sources') or [''])[0])})"
            for entry in strongest
        )
        lines.append(f"- **{tool.get('name', slug)}:** {evidence or 'no governance evidence detected'}.")
    lines.extend(
        [
            "",
            "Managed governance posture note: Fivetran and Informatica publish governance controls, audit material and enterprise operating models, but their closed-core implementation details cannot be code-scored with the same static benchmark.",
            "",
        ]
    )
    return "\n".join(lines)


def render_governance_compliance_svg(payload: dict[str, Any]) -> str:
    matrix = payload.get("governance_compliance") or payload
    tools = list(matrix.get("tools") or [])
    summary = matrix.get("summary") or {}
    width = 920
    height = 118 + 48 * len(tools)
    rows = [
        _svg_header(width, height),
        f'<rect width="920" height="{height}" fill="#f8fafc"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Governance and compliance posture</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Auditability, lineage, contracts, policy gates, evidence chain, release certification and runbooks.</text>',
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
            f'<text x="770" y="{y + 15}" font-size="12" fill="#334155">{score} / {escape(str(item.get("band", "n/a")))}</text>'
        )
    rows.append("</svg>\n")
    return "\n".join(rows)


def _entries_by_key(matrix: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(str(entry.get("tool", "")), str(entry.get("dimension", ""))): entry for entry in matrix.get("entries", [])}


def _level_badge(entry: dict[str, Any]) -> str:
    return {
        "strong": "`strong`",
        "managed": "`managed`",
        "documented": "`documented`",
        "partial": "`partial`",
        "external": "`external`",
        "opaque": "`opaque`",
        "not_detected": "`not detected`",
    }.get(str(entry.get("level", "")), "`n/a`")


def _top_entries(matrix: dict[str, Any], tool_slug: str) -> list[dict[str, Any]]:
    rank = {
        "strong": 5,
        "managed": 4,
        "documented": 3,
        "external": 2,
        "partial": 1,
        "opaque": 0,
        "not_detected": 0,
    }
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
