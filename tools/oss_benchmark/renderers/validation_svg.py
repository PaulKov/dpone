"""SVG renderers for independent analyzer validation evidence."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import as_float, as_int


def render_independent_validation_svg(payload: dict[str, Any]) -> str:
    """Render analyzer availability and cross-check status by project."""

    validation = payload.get("independent_validation") or {}
    summary = list((validation.get("summary") or {}).items())
    width = 920
    height = 138 + 60 * max(1, len(summary))
    rows = [
        _svg_header(width, height, "Independent analyzer validation"),
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        _text(32, 42, "Independent analyzer validation", size=23, weight=700, color="#111827"),
        _text(32, 66, "External analyzer cross-check status for LOC/SLOC and complexity evidence.", size=13),
    ]
    if not summary:
        rows.append(_text(32, 116, "Independent analyzer evidence appears after benchmark generation.", size=13))
    for idx, (_, item) in enumerate(summary):
        y = 104 + idx * 60
        loc_status = str(item.get("loc_sloc_status", "n/a"))
        complexity_status = str(item.get("complexity_status", "n/a"))
        rows.append(_text(32, y + 17, item.get("name", "n/a"), size=13, weight=700, color="#111827"))
        rows.append(_status_pill(220, y, loc_status, "LOC/SLOC"))
        rows.append(_status_pill(390, y, complexity_status, "Complexity"))
        rows.append(
            _text(
                610,
                y + 17,
                f"{as_int(item.get('confidence_score'))}/100 {item.get('validation_band', 'n/a')}",
                size=12,
                color="#334155",
            )
        )
        rows.append(
            _text(
                790,
                y + 17,
                f"stale {as_int(item.get('stale_analyzers'))} / missing {as_int(item.get('unavailable_analyzers'))}",
                size=12,
                color="#64748b",
            )
        )
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_analyzer_confidence_svg(payload: dict[str, Any]) -> str:
    """Render per-project validation confidence scores."""

    validation = payload.get("independent_validation") or {}
    summary = list((validation.get("summary") or {}).items())
    width = 920
    height = 132 + 56 * max(1, len(summary))
    rows = [
        _svg_header(width, height, "Analyzer confidence"),
        f'<rect width="{width}" height="{height}" fill="#f8fafc"/>',
        _text(32, 42, "Analyzer confidence", size=23, weight=700, color="#111827"),
        _text(
            32,
            66,
            "Confidence score from analyzer agreement, coverage and explicit unavailable-tool handling.",
            size=13,
        ),
    ]
    if not summary:
        rows.append(_text(32, 112, "Analyzer confidence appears after benchmark generation.", size=13))
    for idx, (_, item) in enumerate(summary):
        y = 102 + idx * 56
        score = as_int(item.get("confidence_score"))
        rows.append(_text(32, y + 16, item.get("name", "n/a"), size=13, weight=700, color="#111827"))
        rows.append(_bar(222, y, score))
        rows.append(_text(792, y + 16, f"{score}/100 {item.get('validation_band', 'n/a')}", size=12))
    rows.append("</svg>\n")
    return "\n".join(rows)


def _status_pill(x: int, y: int, status: str, label: str) -> str:
    color = _status_color(status)
    text = f"{label}: {status}"
    return "\n".join(
        [
            f'<rect x="{x}" y="{y}" width="144" height="24" rx="4" fill="{color}" opacity="0.14"/>',
            f'<text x="{x + 10}" y="{y + 16}" font-size="12" font-weight="700" fill="{color}">{escape(text)}</text>',
        ]
    )


def _bar(x: int, y: int, score: int) -> str:
    full_width = 540
    fill_width = max(2, min(full_width, int(full_width * (as_float(score) / 100))))
    color = _score_color(score)
    return "\n".join(
        [
            f'<rect x="{x}" y="{y}" width="{full_width}" height="22" rx="4" fill="#e2e8f0"/>',
            f'<rect x="{x}" y="{y}" width="{fill_width}" height="22" rx="4" fill="{color}"/>',
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


def _status_color(status: str) -> str:
    if status == "passed":
        return "#0f766e"
    if status == "warning":
        return "#b45309"
    if status == "failed":
        return "#dc2626"
    return "#64748b"


def _svg_header(width: int, height: int, label: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="{escape(label)}">'
    )


def _text(
    x: int,
    y: int,
    value: Any,
    *,
    size: int,
    weight: int = 400,
    color: str = "#475569",
) -> str:
    return f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" fill="{color}">{escape(str(value))}</text>'
