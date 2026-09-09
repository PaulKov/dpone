"""SVG renderer for public evidence-integrity score."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import as_int


def render_public_evidence_integrity_svg(payload: dict[str, Any]) -> str:
    integrity = payload.get("public_evidence_integrity") or {}
    score = as_int(integrity.get("score"))
    coverage = as_int(integrity.get("claim_coverage_percent"))
    violations = as_int(integrity.get("redaction_violation_count"))
    width = 920
    height = 220
    rows = [
        _svg_header(width, height),
        f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
        _text(32, 42, "Public evidence integrity", size=23, weight=700, color="#111827"),
        _text(32, 66, "Public artifact redaction, claim evidence coverage and publication safety.", size=13),
        _bar(32, 104, "Integrity score", score, _score_color(score)),
        _bar(32, 150, "Claim coverage", coverage, "#2563eb"),
        _text(690, 120, f"redaction violations: {violations}", size=13, weight=700, color=_violation_color(violations)),
        _text(690, 150, f"status: {integrity.get('status', 'n/a')}", size=13, weight=700, color="#334155"),
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


def _violation_color(count: int) -> str:
    return "#0f766e" if count == 0 else "#dc2626"


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="Public evidence integrity">'
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
