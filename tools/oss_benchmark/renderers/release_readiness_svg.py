"""SVG renderer for benchmark release-readiness seal."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.payload_utils import as_int


def render_release_readiness_svg(payload: dict[str, Any]) -> str:
    readiness = payload.get("benchmark_release_readiness") or {}
    seal = readiness.get("evidence_seal") or {}
    score = as_int(seal.get("score"))
    status = str(readiness.get("status") or "n/a")
    width = 920
    height = 210
    return "\n".join(
        [
            _svg_header(width, height),
            f'<rect width="{width}" height="{height}" fill="#ffffff"/>',
            _text(32, 42, "Benchmark v3 release readiness", size=23, weight=700, color="#111827"),
            _text(32, 66, "Release seal, evidence policy and go/no-go evidence for the public benchmark.", size=13),
            f'<rect x="32" y="96" width="380" height="72" rx="8" fill="{_status_color(status)}"/>',
            _text(56, 128, seal.get("label", "Benchmark v3 n/a"), size=24, weight=700, color="#ffffff"),
            _text(56, 154, f"score {score}/100 · {status}", size=14, color="#ecfeff"),
            _text(455, 112, f"stable groups: {len(_policy(readiness, 'stable_metric_groups'))}", size=13, weight=700),
            _text(455, 142, f"experimental groups: {len(_policy(readiness, 'experimental_metric_groups'))}", size=13),
            _text(455, 172, f"generated: {seal.get('generated_at', 'n/a')}", size=13),
            "</svg>\n",
        ]
    )


def _policy(readiness: dict[str, Any], key: str) -> list[Any]:
    return list((readiness.get("freeze_policy") or {}).get(key) or [])


def _status_color(status: str) -> str:
    if status == "release-ready":
        return "#0f766e"
    if status == "watch":
        return "#f59e0b"
    return "#dc2626"


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="Benchmark v3 release readiness">'
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
