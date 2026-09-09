"""SVG renderers for score calibration evidence."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import as_int


def render_score_calibration_svg(payload: dict[str, Any]) -> str:
    summary = (payload.get("scoring_calibration") or {}).get("summary") or {}
    items = list(summary.items())
    width = 920
    height = 132 + 64 * max(1, len(items))
    rows = [
        _svg_header(width, height, "Score calibration"),
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Score calibration</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Language/repo normalized maintainability scores with profile-aware thresholds.</text>',
    ]
    if not items:
        rows.append(_empty("Calibration evidence appears after benchmark generation."))
    for idx, (_, item) in enumerate(items):
        y = 102 + idx * 64
        score = as_int(item.get("normalized_score"))
        rows.append(_label(32, y + 16, item.get("name", "n/a")))
        rows.append(_bar(218, y, score, _score_color(score)))
        rows.append(_text(792, y + 16, f"{score} / {item.get('normalization_profile', 'n/a')}", size=12))
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_score_sensitivity_svg(payload: dict[str, Any]) -> str:
    sensitivity = (payload.get("scoring_calibration") or {}).get("sensitivity") or {}
    items = list(sensitivity.items())
    width = 920
    height = 132 + 58 * max(1, len(items))
    rows = [
        _svg_header(width, height, "Sensitivity analysis"),
        f'<rect width="{width}" height="{height}" fill="#f8fafc"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Sensitivity analysis</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Score swing when calibration thresholds move by +/-10%. Lower swing is better.</text>',
    ]
    if not items:
        rows.append(_empty("Sensitivity evidence appears after benchmark generation."))
    for idx, (slug, item) in enumerate(items):
        y = 102 + idx * 58
        swing = as_int(item.get("threshold_10_percent_swing"))
        width_value = min(560, max(2, swing * 36))
        rows.append(_label(32, y + 16, slug))
        rows.append(f'<rect x="218" y="{y}" width="560" height="22" rx="4" fill="#e2e8f0"/>')
        rows.append(f'<rect x="218" y="{y}" width="{width_value}" height="22" rx="4" fill="{_swing_color(swing)}"/>')
        rows.append(_text(792, y + 16, f"{swing} pts / {item.get('rank_stability', 'n/a')}", size=12))
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_normalized_vs_raw_svg(payload: dict[str, Any]) -> str:
    summary = (payload.get("scoring_calibration") or {}).get("summary") or {}
    items = list(summary.items())
    width = 920
    height = 132 + 66 * max(1, len(items))
    rows = [
        _svg_header(width, height, "Normalized vs raw"),
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Normalized vs raw</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Raw score remains visible; normalized score adds profile-aware context.</text>',
    ]
    if not items:
        rows.append(_empty("Raw-vs-normalized evidence appears after benchmark generation."))
    for idx, (_, item) in enumerate(items):
        y = 104 + idx * 66
        raw = as_int(item.get("raw_score"))
        normalized = as_int(item.get("normalized_score"))
        rows.append(_label(32, y + 17, item.get("name", "n/a")))
        rows.append(f'<rect x="218" y="{y}" width="560" height="24" rx="4" fill="#e2e8f0"/>')
        rows.append(f'<rect x="218" y="{y}" width="{max(2, raw * 5.6):.0f}" height="10" rx="3" fill="#64748b"/>')
        rows.append(
            f'<rect x="218" y="{y + 14}" width="{max(2, normalized * 5.6):.0f}" height="10" rx="3" fill="#2563eb"/>'
        )
        rows.append(_text(792, y + 17, f"raw {raw} -> norm {normalized}", size=12))
    rows.append(_legend(height - 24))
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


def _score_color(score: int) -> str:
    if score >= 90:
        return "#0f766e"
    if score >= 75:
        return "#2563eb"
    if score >= 55:
        return "#f59e0b"
    return "#dc2626"


def _swing_color(swing: int) -> str:
    if swing <= 5:
        return "#0f766e"
    if swing <= 10:
        return "#f59e0b"
    return "#dc2626"


def _svg_header(width: int, height: int, label: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="{escape(label)}">'
    )


def _label(x: int, y: int, value: Any) -> str:
    return _text(x, y, str(value), size=13, weight=700, color="#111827")


def _text(x: int, y: int, value: str, *, size: int, weight: int = 400, color: str = "#334155") -> str:
    return f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{color}">{escape(value)}</text>'


def _empty(message: str) -> str:
    return _text(32, 118, message, size=13, color="#64748b")


def _legend(y: int) -> str:
    return "\n".join(
        [
            f'<rect x="218" y="{y - 10}" width="18" height="8" rx="2" fill="#64748b"/>',
            _text(244, y - 2, "raw", size=11, color="#64748b"),
            f'<rect x="290" y="{y - 10}" width="18" height="8" rx="2" fill="#2563eb"/>',
            _text(316, y - 2, "normalized", size=11, color="#64748b"),
        ]
    )
