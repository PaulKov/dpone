"""SVG renderers for scale-readiness evidence."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import as_int


def render_scale_readiness_svg(payload: dict[str, Any]) -> str:
    summary = (payload.get("scale_readiness") or {}).get("summary") or {}
    items = list(summary.items())
    width = 920
    height = 132 + 62 * max(1, len(items))
    rows = [
        _svg_header(width, height, "Scale readiness"),
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        _text(32, 42, "Scale readiness", size=23, weight=700, color="#111827"),
        _text(32, 66, "Architecture runway score for growth toward comparator repository scale.", size=13),
    ]
    if not items:
        rows.append(_empty("Scale-readiness evidence appears after benchmark generation."))
    for idx, (_, item) in enumerate(items):
        y = 102 + idx * 62
        score = as_int(item.get("architecture_runway_score"))
        rows.append(_label(32, y + 16, item.get("name", "n/a")))
        rows.append(_bar(232, y, score, _score_color(score)))
        rows.append(_text(792, y + 16, f"{score} / {item.get('overall_status', 'n/a')}", size=12))
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_architecture_runway_svg(payload: dict[str, Any]) -> str:
    runway = (payload.get("scale_readiness") or {}).get("architecture_runway") or {}
    items = list(runway.items())
    width = 920
    height = 132 + 58 * max(1, len(items))
    rows = [
        _svg_header(width, height, "Architecture runway"),
        f'<rect width="{width}" height="{height}" fill="#f8fafc"/>',
        _text(32, 42, "Architecture runway", size=23, weight=700, color="#111827"),
        _text(32, 66, "Projected production SLOC ceiling before quality budgets enter red territory.", size=13),
    ]
    if not items:
        rows.append(_empty("Architecture runway evidence appears after benchmark generation."))
    maximum = max([as_int(item.get("growth_ceiling_sloc")) for _, item in items] or [1])
    for idx, (slug, item) in enumerate(items):
        y = 102 + idx * 58
        ceiling = as_int(item.get("growth_ceiling_sloc"))
        rows.append(_label(32, y + 16, slug))
        rows.append(_bar_width(232, y, 560, ceiling / maximum if maximum else 0.0, "#2563eb"))
        rows.append(_text(792, y + 16, f"{ceiling:,} SLOC", size=12))
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_quality_headroom_svg(payload: dict[str, Any]) -> str:
    headroom = (payload.get("scale_readiness") or {}).get("quality_headroom") or {}
    items = list(headroom.items())
    width = 920
    height = 132 + 58 * max(1, len(items))
    rows = [
        _svg_header(width, height, "Quality headroom"),
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        _text(32, 42, "Quality headroom", size=23, weight=700, color="#111827"),
        _text(32, 66, "Estimated connector slots before the architecture enters yellow budget pressure.", size=13),
    ]
    if not items:
        rows.append(_empty("Quality headroom evidence appears after benchmark generation."))
    maximum = max([as_int(item.get("connector_slots_before_yellow")) for _, item in items] or [1])
    for idx, (slug, item) in enumerate(items):
        y = 102 + idx * 58
        slots = as_int(item.get("connector_slots_before_yellow"))
        rows.append(_label(32, y + 16, slug))
        rows.append(_bar_width(232, y, 560, slots / maximum if maximum else 0.0, _slot_color(slots)))
        rows.append(_text(792, y + 16, f"{slots} slots", size=12))
    rows.append("</svg>\n")
    return "\n".join(rows)


def _bar(x: int, y: int, score: int, color: str) -> str:
    return _bar_width(x, y, 560, score / 100, color)


def _bar_width(x: int, y: int, full_width: int, ratio: float, color: str) -> str:
    width = max(2, min(full_width, int(full_width * ratio)))
    return "\n".join(
        [
            f'<rect x="{x}" y="{y}" width="{full_width}" height="22" rx="4" fill="#e2e8f0"/>',
            f'<rect x="{x}" y="{y}" width="{width}" height="22" rx="4" fill="{color}"/>',
        ]
    )


def _score_color(score: int) -> str:
    if score >= 80:
        return "#0f766e"
    if score >= 65:
        return "#f59e0b"
    return "#dc2626"


def _slot_color(slots: int) -> str:
    if slots >= 12:
        return "#0f766e"
    if slots >= 5:
        return "#f59e0b"
    return "#dc2626"


def _svg_header(width: int, height: int, label: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="{escape(label)}">'
    )


def _label(x: int, y: int, value: Any) -> str:
    return _text(x, y, str(value), size=13, weight=700, color="#111827")


def _text(x: int, y: int, value: str, *, size: int, weight: int = 400, color: str = "#475569") -> str:
    return f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{color}">{escape(value)}</text>'


def _empty(message: str) -> str:
    return _text(32, 118, message, size=13, color="#64748b")
