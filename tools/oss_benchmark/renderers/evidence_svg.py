"""SVG renderer for evidence confidence."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import as_int


def render_evidence_confidence_svg(payload: dict[str, Any]) -> str:
    """Render per-project evidence confidence and evidence-mode counts."""

    trust = payload.get("evidence_trust") or {}
    projects = list((trust.get("projects") or {}).values())
    width = 920
    height = 146 + 54 * max(1, len(projects)) + 34 * len(trust.get("mode_counts") or [])
    rows = [
        _svg_header(width, height),
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Evidence confidence</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Auditability score from freshness, source measurements, derived formulas and closed-core caveats.</text>',
        f'<text x="760" y="42" font-size="14" font-weight="700" fill="#334155">{as_int(trust.get("overall_confidence_score"))}/100</text>',
    ]
    y = 104
    if not projects:
        rows.append(
            '<text x="32" y="118" font-size="13" fill="#64748b">Evidence confidence appears after benchmark generation.</text>'
        )
    for item in projects:
        score = as_int(item.get("confidence_score"))
        color = "#0f766e" if score >= 90 else "#2563eb" if score >= 75 else "#f59e0b" if score >= 55 else "#dc2626"
        fill_width = max(2, int(score * 5.6))
        rows.append(
            f'<text x="32" y="{y + 16}" font-size="13" font-weight="700" fill="#111827">{escape(str(item.get("name", "n/a")))}</text>'
        )
        rows.append(f'<rect x="210" y="{y}" width="560" height="22" rx="4" fill="#e2e8f0"/>')
        rows.append(f'<rect x="210" y="{y}" width="{fill_width}" height="22" rx="4" fill="{color}"/>')
        rows.append(
            f'<text x="790" y="{y + 16}" font-size="12" fill="#334155">{score} / {escape(str(item.get("band", "n/a")))}</text>'
        )
        y += 54
    rows.append(f'<text x="32" y="{y + 10}" font-size="14" font-weight="700" fill="#111827">Evidence mode mix</text>')
    y += 34
    for mode in trust.get("mode_counts") or []:
        rows.append(
            f'<text x="52" y="{y}" font-size="12" fill="#334155">{escape(str(mode.get("mode", "n/a")))}: {as_int(mode.get("count"))}</text>'
        )
        y += 28
    rows.append("</svg>\n")
    return "\n".join(rows)


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="Evidence confidence">'
    )
