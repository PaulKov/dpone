"""SVG renderers for semantic maintainability evidence."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import as_int


def render_semantic_maintainability_svg(payload: dict[str, Any]) -> str:
    summary = (payload.get("semantic_maintainability") or {}).get("summary") or {}
    items = list(summary.items())
    width = 920
    height = 126 + 62 * max(1, len(items))
    rows = [
        _svg_header(width, height, "Semantic maintainability"),
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Semantic maintainability</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">God objects, SOLID/DI, DRY/KISS and boundary discipline from static source-shape evidence.</text>',
    ]
    if not items:
        rows.append(
            '<text x="32" y="118" font-size="13" fill="#64748b">Semantic evidence appears after benchmark generation.</text>'
        )
    for idx, (_, item) in enumerate(items):
        y = 102 + idx * 62
        score = as_int(item.get("overall_score"))
        rows.append(
            f'<text x="32" y="{y + 16}" font-size="13" font-weight="700" fill="#111827">{escape(str(item.get("name", "n/a")))}</text>'
        )
        rows.append(_bar(210, y, score, _color(score)))
        rows.append(
            f'<text x="790" y="{y + 16}" font-size="12" fill="#334155">{score} / {escape(str(item.get("status", "n/a")))}</text>'
        )
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_god_object_radar_svg(payload: dict[str, Any]) -> str:
    summary = (payload.get("semantic_maintainability") or {}).get("summary") or {}
    items = list(summary.items())
    width = 920
    height = 126 + 58 * max(1, len(items))
    rows = [
        _svg_header(width, height, "God object radar"),
        f'<rect width="{width}" height="{height}" fill="#f8fafc"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">God object radar</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Counts of oversized modules, classes and functions. Lower is better.</text>',
    ]
    if not items:
        rows.append(
            '<text x="32" y="118" font-size="13" fill="#64748b">God object evidence appears after benchmark generation.</text>'
        )
    for idx, (_, item) in enumerate(items):
        y = 102 + idx * 58
        count = (
            as_int(item.get("god_module_count"))
            + as_int(item.get("god_class_count"))
            + as_int(item.get("god_function_count"))
        )
        color = "#0f766e" if count == 0 else "#f59e0b" if count <= 3 else "#dc2626"
        width_value = min(560, max(2, count * 42))
        rows.append(
            f'<text x="32" y="{y + 16}" font-size="13" font-weight="700" fill="#111827">{escape(str(item.get("name", "n/a")))}</text>'
        )
        rows.append(f'<rect x="210" y="{y}" width="560" height="22" rx="4" fill="#e2e8f0"/>')
        rows.append(f'<rect x="210" y="{y}" width="{width_value}" height="22" rx="4" fill="{color}"/>')
        rows.append(f'<text x="790" y="{y + 16}" font-size="12" fill="#334155">{count} god objects</text>')
    rows.append("</svg>\n")
    return "\n".join(rows)


def _bar(x: int, y: int, score: int, color: str) -> str:
    width_value = max(2, int(score * 5.6))
    return "\n".join(
        [
            f'<rect x="{x}" y="{y}" width="560" height="22" rx="4" fill="#e2e8f0"/>',
            f'<rect x="{x}" y="{y}" width="{width_value}" height="22" rx="4" fill="{color}"/>',
        ]
    )


def _color(score: int) -> str:
    if score >= 90:
        return "#0f766e"
    if score >= 75:
        return "#2563eb"
    if score >= 55:
        return "#f59e0b"
    return "#dc2626"


def _svg_header(width: int, height: int, label: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="{escape(label)}">'
    )
