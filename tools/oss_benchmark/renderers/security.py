"""Security supply-chain Markdown and SVG renderers."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import format_int as _format_int


def render_security_supply_chain_section(matrix: dict[str, Any]) -> str:
    if not matrix:
        return ""
    tools = list(matrix.get("tools") or [])
    dimensions = list(matrix.get("dimensions") or [])
    summary = matrix.get("summary") or {}
    entries = _entries_by_key(matrix)
    lines = [
        "",
        "## Security & Supply Chain",
        "",
        "This layer checks repository-local security and supply-chain controls that matter for industrial adoption: vulnerability scanning, dependency response, reproducible builds, SBOM inventory, security policy and release provenance. Closed-core vendors are listed as posture notes, not code-scored source benchmarks.",
        "",
        "![Security supply-chain posture](assets/oss-security-supply-chain.svg)",
        "",
        "| Tool | Security score | Band | Present controls | Missing controls | Comparator note |",
        "|---|---:|---|---:|---:|---|",
    ]
    for tool in tools:
        item = summary.get(tool.get("slug", ""), {})
        lines.append(
            "| "
            f"{tool.get('name', tool.get('slug', 'n/a'))} | "
            f"{_score(item.get('score'))} | "
            f"{item.get('band', 'n/a')} | "
            f"{_format_int(item.get('present_controls'))} | "
            f"{_format_int(item.get('missing_controls'))} | "
            f"{item.get('comparator_note', tool.get('comparator_note', 'n/a'))} |"
        )
    lines.extend(["", "### Security control matrix", ""])
    lines.append("| Control | " + " | ".join(tool.get("name", tool.get("slug", "")) for tool in tools) + " |")
    lines.append("|---|" + "|".join("---" for _ in tools) + "|")
    for dimension in dimensions:
        row = [str(dimension.get("label", dimension.get("slug", "")))]
        for tool in tools:
            row.append(_status_badge(entries.get((tool.get("slug", ""), dimension.get("slug", "")), {})))
        lines.append("| " + " | ".join(row) + " |")
    lines.extend(["", "### Security evidence", ""])
    for tool in tools:
        slug = str(tool.get("slug", ""))
        strong = _top_security_entries(matrix, slug)
        evidence = "; ".join(
            f"{entry.get('dimension', 'control')} `{entry.get('status', 'n/a')}` "
            f"({_source_link((entry.get('sources') or [''])[0])})"
            for entry in strong
        )
        lines.append(f"- **{tool.get('name', slug)}:** {evidence or 'no source evidence detected'}.")
    lines.extend(
        [
            "",
            "Closed-core posture note: Fivetran and Informatica publish security/trust material, but their managed platform source supply-chain controls cannot be measured with the same static repository benchmark.",
            "",
            "Security supply-chain evidence is stored in raw JSON under `security_supply_chain.entries[]`.",
            "",
        ]
    )
    return "\n".join(lines)


def render_security_supply_chain_svg(payload: dict[str, Any]) -> str:
    matrix = payload.get("security_supply_chain") or payload
    tools = list(matrix.get("tools") or [])
    summary = matrix.get("summary") or {}
    width = 920
    height = 118 + 48 * len(tools)
    rows = [
        _svg_header(width, height),
        f'<rect width="920" height="{height}" fill="#f8fafc"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Security supply-chain posture</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Repository-local controls: SAST, secrets, dependency response, SBOM, policy, permissions, provenance.</text>',
    ]
    for idx, tool in enumerate(tools):
        slug = str(tool.get("slug", ""))
        item = summary.get(slug, {})
        score = item.get("score")
        y = 98 + idx * 48
        rows.append(
            f'<text x="32" y="{y + 15}" font-size="13" font-weight="700" fill="#111827">{escape(tool.get("name", slug))}</text>'
        )
        rows.append(f'<rect x="230" y="{y}" width="520" height="20" rx="4" fill="#e2e8f0"/>')
        if isinstance(score, int):
            color = "#0f766e" if score >= 85 else "#2563eb" if score >= 70 else "#f59e0b" if score >= 45 else "#dc2626"
            rows.append(f'<rect x="230" y="{y}" width="{max(2, int(score * 5.2))}" height="20" rx="4" fill="{color}"/>')
            label = f"{score} / {item.get('band', 'n/a')}"
        else:
            label = str(item.get("band", "not code-scored"))
        rows.append(f'<text x="770" y="{y + 15}" font-size="12" fill="#334155">{escape(label)}</text>')
    rows.append("</svg>\n")
    return "\n".join(rows)


def _entries_by_key(matrix: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(str(entry.get("tool", "")), str(entry.get("dimension", ""))): entry for entry in matrix.get("entries", [])}


def _status_badge(entry: dict[str, Any]) -> str:
    return {
        "present": "`present`",
        "partial": "`partial`",
        "not_detected": "`not detected`",
        "unavailable": "`n/a`",
    }.get(str(entry.get("status", "")), "`n/a`")


def _top_security_entries(matrix: dict[str, Any], tool_slug: str) -> list[dict[str, Any]]:
    rank = {"present": 3, "partial": 2, "unavailable": 1, "not_detected": 0}
    entries = [entry for entry in matrix.get("entries", []) if entry.get("tool") == tool_slug]
    return sorted(entries, key=lambda entry: (-rank.get(str(entry.get("status")), 0), entry.get("dimension", "")))[:4]


def _source_link(source: str) -> str:
    if not source:
        return "no source"
    if source.startswith("http"):
        return f"[source]({source})"
    return f"`{source}`"


def _score(value: Any) -> str:
    return _format_int(value) if value is not None else "n/a"


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">'
    )
