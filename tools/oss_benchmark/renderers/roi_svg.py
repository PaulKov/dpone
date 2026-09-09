"""SVG renderer for the Refactor ROI roadmap."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import as_int


def render_refactor_roi_svg(payload: dict[str, Any]) -> str:
    items = list((payload.get("refactor_roi") or {}).get("items") or [])[:8]
    width = 920
    height = 154 + 58 * max(1, len(items))
    rows = [
        _svg_header(width, height),
        f'<rect width="920" height="{height}" fill="#f8fafc"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Refactor ROI roadmap</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Higher ROI means more quality debt reduction per unit of likely refactor effort.</text>',
        '<text x="210" y="102" font-size="11" fill="#64748b">ROI</text>',
        '<text x="500" y="102" font-size="11" fill="#64748b">Impact</text>',
        '<text x="650" y="102" font-size="11" fill="#64748b">Effort feasibility</text>',
    ]
    if not items:
        rows.append(
            '<text x="290" y="162" font-size="14" fill="#64748b">ROI roadmap appears after benchmark generation.</text>'
        )
        rows.append("</svg>\n")
        return "\n".join(rows)
    for idx, item in enumerate(items):
        y = 126 + idx * 58
        roi = as_int(item.get("roi_score"))
        impact = as_int(item.get("impact_score"))
        effort = as_int(item.get("effort_score"))
        color = "#0f766e" if roi >= 85 else "#2563eb" if roi >= 70 else "#f59e0b" if roi >= 55 else "#dc2626"
        label = escape(str(item.get("module", "n/a")).split("/")[-1])[:34]
        unit = escape(str(item.get("reason", "")))[:42]
        rows.append(f'<text x="32" y="{y + 14}" font-size="12" font-weight="700" fill="#111827">{label}</text>')
        rows.append(f'<text x="32" y="{y + 34}" font-size="10" fill="#64748b">{unit}</text>')
        rows.append(f'<rect x="210" y="{y}" width="250" height="18" rx="4" fill="#e2e8f0"/>')
        rows.append(f'<rect x="210" y="{y}" width="{max(2, int(roi * 2.5))}" height="18" rx="4" fill="{color}"/>')
        rows.append(f'<text x="472" y="{y + 14}" font-size="11" fill="#334155">{roi}</text>')
        rows.append(f'<text x="500" y="{y + 14}" font-size="11" fill="#334155">{impact}</text>')
        rows.append(f'<text x="650" y="{y + 14}" font-size="11" fill="#334155">{effort}</text>')
        rows.append(
            f'<text x="760" y="{y + 14}" font-size="10" fill="#475569">{escape(str(item.get("quadrant", "n/a")))}</text>'
        )
    rows.append("</svg>\n")
    return "\n".join(rows)


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">'
    )
