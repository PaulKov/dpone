"""SVG renderer for source citation verification."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import as_int


def render_source_verification_svg(payload: dict[str, Any]) -> str:
    verification = payload.get("source_verification") or {}
    summary = verification.get("summary") or {}
    health = as_int(summary.get("source_health_score"))
    traceability = as_int(summary.get("claim_traceability_percent"))
    verified = as_int(summary.get("verified_count"))
    stale = as_int(summary.get("stale_count"))
    unavailable = as_int(summary.get("unavailable_count"))
    width = 920
    height = 230
    rows = [
        _svg_header(width, height),
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        _text(32, 42, "Source citation verification", size=23, weight=700, color="#111827"),
        _text(32, 66, "Source health, stale-safe citation refresh and claim-to-source traceability.", size=13),
        _bar(32, 106, "Source health", health, _score_color(health)),
        _bar(32, 154, "Claim traceability", traceability, "#2563eb"),
        _text(690, 116, f"verified: {verified}", size=13, weight=700, color="#0f766e"),
        _text(690, 146, f"stale: {stale}", size=13, weight=700, color="#f59e0b"),
        _text(690, 176, f"unavailable: {unavailable}", size=13, weight=700, color="#dc2626"),
        _text(690, 204, f"status: {verification.get('status', 'n/a')}", size=13, weight=700, color="#334155"),
        "</svg>\n",
    ]
    return "\n".join(rows)


def _bar(x: int, y: int, label: str, value: int, color: str) -> str:
    full_width = 420
    fill = max(2, min(full_width, int(full_width * value / 100)))
    return "\n".join(
        [
            _text(x, y - 10, label, size=12, weight=700, color="#334155"),
            f'<rect x="{x}" y="{y}" width="{full_width}" height="22" rx="4" fill="#e2e8f0"/>',
            f'<rect x="{x}" y="{y}" width="{fill}" height="22" rx="4" fill="{color}"/>',
            _text(x + full_width + 18, y + 16, f"{value}/100", size=12, color="#334155"),
        ]
    )


def _score_color(score: int) -> str:
    if score >= 95:
        return "#0f766e"
    if score >= 70:
        return "#f59e0b"
    return "#dc2626"


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="Source citation verification">'
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
