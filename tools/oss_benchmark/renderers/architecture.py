"""Architecture taxonomy Markdown and SVG renderers."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import format_int as _format_int


def render_architecture_taxonomy_section(matrix: dict[str, Any]) -> str:
    """Render architecture slice and contract discipline evidence."""

    if not matrix:
        return ""
    summary = matrix.get("summary") or {}
    lines = [
        "",
        "## Architecture Taxonomy & Contract Discipline",
        "",
        "This layer turns SOLID/DRY/KISS expectations into a public benchmark signal: thin architectural slices, explicit source/sink/CDC contracts, canonical naming, dependency-inversion boundaries and god-module prevention.",
        "",
        "![Architecture taxonomy and contract discipline](assets/oss-architecture-taxonomy.svg)",
        "",
        "| Project | Taxonomy score | Band | Contract conformance | Naming | DI boundary | Approved facades | Slices | Top violation |",
        "|---|---:|---|---:|---:|---:|---:|---:|---|",
    ]
    for slug, item in summary.items():
        contract = item.get("contract_conformance") or {}
        violations = contract.get("violations") or []
        lines.append(
            "| "
            f"{item.get('name', slug)} | "
            f"{_format_int(item.get('score'))} | "
            f"{item.get('band', 'n/a')} | "
            f"{_format_int(contract.get('score'))} | "
            f"{_format_int((contract.get('naming_consistency') or {}).get('score'))} | "
            f"{_format_int((contract.get('di_boundary') or {}).get('score'))} | "
            f"{_format_int((contract.get('compatibility_facades') or {}).get('count'))} | "
            f"{_format_int(item.get('slice_count'))} | "
            f"{_top_violation(violations)} |"
        )
    lines.extend(["", "### Slice heatmap", ""])
    for slug, item in summary.items():
        lines.append(f"#### {item.get('name', slug)} slices")
        lines.append("")
        lines.append("| Slice | Modules | LOC | SLOC | Max LOC | Interface files | Naming violations |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|")
        for slice_item in (item.get("slices") or [])[:8]:
            lines.append(
                "| "
                f"{slice_item.get('slice', 'n/a')} | "
                f"{_format_int(slice_item.get('modules'))} | "
                f"{_format_int(slice_item.get('loc'))} | "
                f"{_format_int(slice_item.get('sloc'))} | "
                f"{_format_int(slice_item.get('max_loc'))} | "
                f"{_format_int(slice_item.get('interface_files'))} | "
                f"{_format_int(slice_item.get('naming_violations'))} |"
            )
        lines.append("")
    lines.extend(["### Contract conformance findings", ""])
    for slug, item in summary.items():
        contract = item.get("contract_conformance") or {}
        violations = contract.get("violations") or []
        if not violations:
            approved = (contract.get("compatibility_facades") or {}).get("count", 0)
            lines.append(
                f"- **{item.get('name', slug)}:** no contract-discipline violations crossed the watch threshold; "
                f"{approved} documented compatibility facades were excluded from active-risk scoring."
            )
            continue
        for violation in violations[:5]:
            lines.append(
                f"- **{item.get('name', slug)} / {violation.get('slice', 'n/a')}:** "
                f"`{violation.get('path', 'n/a')}` {violation.get('kind', 'risk')} - "
                f"{violation.get('message', 'review contract boundary')}"
            )
    lines.extend(
        ["", "Architecture taxonomy evidence is stored in raw JSON under `architecture_taxonomy.summary`.", ""]
    )
    return "\n".join(lines)


def render_architecture_taxonomy_svg(payload: dict[str, Any]) -> str:
    """Render a compact scorecard for architecture taxonomy scores."""

    matrix = payload.get("architecture_taxonomy") or payload
    summary = matrix.get("summary") or {}
    items = list(summary.items())
    width = 920
    height = 118 + 56 * len(items)
    rows = [
        _svg_header(width, height),
        f'<rect width="920" height="{height}" fill="#f8fafc"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Architecture taxonomy discipline</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Scores contract reuse, canonical naming, DI boundaries, slice taxonomy, and module-size budgets.</text>',
    ]
    for idx, (slug, item) in enumerate(items):
        score = int(item.get("score") or 0)
        contract = item.get("contract_conformance") or {}
        y = 98 + idx * 56
        color = "#0f766e" if score >= 85 else "#2563eb" if score >= 70 else "#f59e0b" if score >= 50 else "#dc2626"
        rows.append(
            f'<text x="32" y="{y + 16}" font-size="13" font-weight="700" fill="#111827">{escape(item.get("name", slug))}</text>'
        )
        rows.append(f'<rect x="230" y="{y}" width="520" height="22" rx="4" fill="#e2e8f0"/>')
        rows.append(f'<rect x="230" y="{y}" width="{max(2, int(score * 5.2))}" height="22" rx="4" fill="{color}"/>')
        label = f"{score} / {item.get('band', 'n/a')} / contract {contract.get('score', 'n/a')}"
        rows.append(f'<text x="768" y="{y + 16}" font-size="12" fill="#334155">{escape(label)}</text>')
    rows.append("</svg>\n")
    return "\n".join(rows)


def _top_violation(violations: list[dict[str, Any]]) -> str:
    if not violations:
        return "none"
    item = violations[0]
    return f"`{item.get('path', 'n/a')}` {item.get('kind', 'risk')}"


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">'
    )
