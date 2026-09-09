"""SVG renderers for the OSS code-quality benchmark."""

from __future__ import annotations

from html import escape
from typing import Any

from tools.oss_benchmark.models import ProjectMetrics
from tools.oss_benchmark.payload_utils import (
    as_float as _as_float,
)
from tools.oss_benchmark.payload_utils import (
    as_int as _as_int,
)
from tools.oss_benchmark.payload_utils import (
    format_float as _format_float,
)
from tools.oss_benchmark.payload_utils import (
    get_value as _get,
)
from tools.oss_benchmark.payload_utils import (
    metric_to_renderable,
)
from tools.oss_benchmark.payload_utils import (
    project_name as _project_name,
)
from tools.oss_benchmark.state import project_freshness_status


def _svg_projects(payload: dict[str, Any] | tuple[ProjectMetrics, ...]) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        return [
            project for project in payload.get("projects", []) if project_freshness_status(project) != "unavailable"
        ]
    return [metric_to_renderable(metric) for metric in payload]


def render_scorecard_svg(payload: dict[str, Any] | tuple[ProjectMetrics, ...]) -> str:
    metrics = _svg_projects(payload)
    width = 920
    height = 118 + 72 * len(metrics)
    rows: list[str] = [
        _svg_header(width, height),
        f'<rect width="920" height="{height}" fill="#f8fafc"/>',
        '<text x="32" y="42" font-size="24" font-weight="700" fill="#111827">OSS code quality scorecard</text>',
        '<text x="32" y="68" font-size="13" fill="#475569">SOLID and Clean OOP scores use static maintainability proxies on pinned source snapshots.</text>',
    ]
    for idx, metric in enumerate(metrics):
        y = 106 + idx * 72
        rows.append(
            f'<text x="32" y="{y}" font-size="15" font-weight="700" fill="#111827">{escape(_project_name(metric))}</text>'
        )
        rows.append(_score_bar(210, y - 14, _as_float(_get(metric, "quality", "solid")), "#0f766e", "SOLID"))
        rows.append(_score_bar(500, y - 14, _as_float(_get(metric, "quality", "clean_oop")), "#2563eb", "Clean OOP"))
        rows.append(
            f'<text x="790" y="{y}" font-size="12" fill="#475569">Ce {_format_float(_get(metric, "coupling", "avg_ce"), digits=2)} / cohesion {_format_float(_get(metric, "coupling", "cohesion_ratio"), digits=2)}</text>'
        )
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_feature_parity_svg(payload: dict[str, Any]) -> str:
    matrix = payload.get("feature_parity") or {}
    tools = list(matrix.get("tools") or [])
    summary = matrix.get("summary") or {}
    width = 920
    height = 118 + 48 * len(tools)
    rows = [
        _svg_header(width, height),
        f'<rect width="920" height="{height}" fill="#ffffff"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Feature parity coverage</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Qualitative capability score from public product documentation and local dpone evidence.</text>',
    ]
    for idx, tool in enumerate(tools):
        slug = tool.get("slug", "")
        score = _as_int((summary.get(slug) or {}).get("score"))
        y = 98 + idx * 48
        color = "#0f766e" if score >= 85 else "#2563eb" if score >= 70 else "#f59e0b" if score >= 50 else "#dc2626"
        rows.append(
            f'<text x="32" y="{y + 15}" font-size="13" font-weight="700" fill="#111827">{escape(tool.get("name", slug))}</text>'
        )
        rows.append(f'<rect x="210" y="{y}" width="560" height="20" rx="4" fill="#e2e8f0"/>')
        rows.append(f'<rect x="210" y="{y}" width="{max(2, int(score * 5.6))}" height="20" rx="4" fill="{color}"/>')
        note = "code" if (summary.get(slug) or {}).get("code_comparable") else "closed-core"
        rows.append(f'<text x="790" y="{y + 15}" font-size="12" fill="#334155">{score} / {note}</text>')
    rows.append("</svg>\n")
    return "\n".join(rows)


def _score_bar(x: int, y: int, score: float, color: str, label: str) -> str:
    fill_width = int(210 * (score / 5.0))
    return "\n".join(
        [
            f'<text x="{x}" y="{y - 7}" font-size="11" fill="#475569">{label}: {score:.1f}/5</text>',
            f'<rect x="{x}" y="{y}" width="210" height="16" rx="4" fill="#e2e8f0"/>',
            f'<rect x="{x}" y="{y}" width="{fill_width}" height="16" rx="4" fill="{color}"/>',
        ]
    )


def render_loc_sloc_svg(payload: dict[str, Any] | tuple[ProjectMetrics, ...]) -> str:
    metrics = _svg_projects(payload)
    width = 920
    height = 360
    max_value = max((_as_int(_get(metric, "loc_without_tests", "total_lines")) for metric in metrics), default=1)
    bar_width = 96
    gap = 70
    x0 = 70
    base = 295
    scale = 210 / max_value
    rows = [
        _svg_header(width, height),
        '<rect width="920" height="360" fill="#ffffff"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">LOC/SLOC without tests</text>',
        '<line x1="48" y1="295" x2="884" y2="295" stroke="#cbd5e1"/>',
    ]
    for idx, metric in enumerate(metrics):
        x = x0 + idx * (bar_width + gap)
        loc_value = _as_int(_get(metric, "loc_without_tests", "total_lines"))
        sloc_value = _as_int(_get(metric, "loc_without_tests", "total_sloc"))
        loc_h = max(2, int(loc_value * scale))
        sloc_h = max(2, int(sloc_value * scale))
        rows.append(f'<rect x="{x}" y="{base - loc_h}" width="40" height="{loc_h}" fill="#0f766e"/>')
        rows.append(f'<rect x="{x + 44}" y="{base - sloc_h}" width="40" height="{sloc_h}" fill="#f59e0b"/>')
        rows.append(f'<text x="{x - 10}" y="322" font-size="11" fill="#334155">{escape(_project_name(metric))}</text>')
        rows.append(f'<text x="{x}" y="{base - loc_h - 8}" font-size="10" fill="#334155">{loc_value}</text>')
    rows.append('<text x="710" y="54" font-size="12" fill="#0f766e">green = LOC</text>')
    rows.append('<text x="810" y="54" font-size="12" fill="#b45309">amber = SLOC</text>')
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_quadrant_svg(payload: dict[str, Any] | tuple[ProjectMetrics, ...]) -> str:
    metrics = _svg_projects(payload)
    width = 920
    height = 420
    rows = [
        _svg_header(width, height),
        '<rect width="920" height="420" fill="#f8fafc"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Coupling / cohesion quadrant</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Upper-left is preferred: high cohesion with low average fan-out.</text>',
        '<line x1="90" y1="340" x2="850" y2="340" stroke="#94a3b8"/>',
        '<line x1="90" y1="340" x2="90" y2="92" stroke="#94a3b8"/>',
        '<text x="390" y="386" font-size="12" fill="#475569">Average fan-out (Ce)</text>',
        '<text x="18" y="210" font-size="12" fill="#475569" transform="rotate(-90 18 210)">Cohesion ratio</text>',
    ]
    max_ce = max((_as_float(_get(metric, "coupling", "avg_ce")) for metric in metrics), default=1.0)
    for metric in metrics:
        avg_ce = _as_float(_get(metric, "coupling", "avg_ce"))
        cohesion = _as_float(_get(metric, "coupling", "cohesion_ratio"))
        x = 90 + int((avg_ce / max(max_ce, 0.01)) * 720)
        y = 340 - int(cohesion * 240)
        rows.append(f'<circle cx="{x}" cy="{y}" r="9" fill="#2563eb" opacity="0.88"/>')
        rows.append(
            f'<text x="{x + 12}" y="{y + 4}" font-size="12" fill="#111827">{escape(_project_name(metric))}</text>'
        )
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_hotspots_svg(payload: dict[str, Any] | tuple[ProjectMetrics, ...]) -> str:
    metrics = _svg_projects(payload)
    width = 920
    height = 370
    max_value = max((_as_int(_get(metric, "loc_without_tests", "max_lines")) for metric in metrics), default=1)
    rows = [
        _svg_header(width, height),
        '<rect width="920" height="370" fill="#ffffff"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Largest production module hotspot</text>',
    ]
    for idx, metric in enumerate(metrics):
        y = 86 + idx * 56
        max_lines = _as_int(_get(metric, "loc_without_tests", "max_lines"))
        width_value = max(2, int((max_lines / max(max_value, 1)) * 640))
        color = "#dc2626" if max_lines > 1000 else "#f59e0b" if max_lines > 600 else "#0f766e"
        rows.append(f'<text x="32" y="{y + 16}" font-size="13" fill="#111827">{escape(_project_name(metric))}</text>')
        rows.append(f'<rect x="180" y="{y}" width="640" height="22" rx="4" fill="#e2e8f0"/>')
        rows.append(f'<rect x="180" y="{y}" width="{width_value}" height="22" rx="4" fill="{color}"/>')
        rows.append(f'<text x="832" y="{y + 16}" font-size="12" fill="#334155">{max_lines} LOC</text>')
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_architecture_risk_svg(payload: dict[str, Any] | tuple[ProjectMetrics, ...]) -> str:
    metrics = _svg_projects(payload)
    width = 920
    height = 118 + 58 * len(metrics)
    rows = [
        _svg_header(width, height),
        f'<rect width="920" height="{height}" fill="#ffffff"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Architecture risk heatmap</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Risk score from module size, fan-out, fan-in, cohesion, and clustering hotspots.</text>',
    ]
    for idx, metric in enumerate(metrics):
        risk = metric.get("architecture_risk") or {}
        score = _as_int(risk.get("score"))
        y = 98 + idx * 58
        color = "#0f766e" if score < 25 else "#f59e0b" if score < 50 else "#dc2626" if score < 75 else "#7f1d1d"
        width_value = max(2, int((score / 100.0) * 560))
        rows.append(
            f'<text x="32" y="{y + 16}" font-size="13" font-weight="700" fill="#111827">{escape(_project_name(metric))}</text>'
        )
        rows.append(f'<rect x="210" y="{y}" width="560" height="22" rx="4" fill="#e2e8f0"/>')
        rows.append(f'<rect x="210" y="{y}" width="{width_value}" height="22" rx="4" fill="{color}"/>')
        rows.append(
            f'<text x="790" y="{y + 16}" font-size="12" fill="#334155">{risk.get("level", "n/a")} / {score}</text>'
        )
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_architecture_delta_svg(payload: dict[str, Any]) -> str:
    delta = payload.get("architecture_delta") or {}
    items = list(delta.get("items") or [])
    width = 920
    height = 132 + 54 * max(1, len(items))
    rows = [
        _svg_header(width, height),
        f'<rect width="920" height="{height}" fill="#f8fafc"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Architecture delta</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Governance movement against previous benchmark evidence.</text>',
        f'<text x="760" y="42" font-size="13" font-weight="700" fill="#334155">{escape(str(delta.get("status", "n/a")))}</text>',
    ]
    if not items:
        rows.append(
            '<text x="290" y="142" font-size="14" fill="#64748b">Architecture deltas appear after a comparable previous benchmark exists.</text>'
        )
        rows.append("</svg>\n")
        return "\n".join(rows)
    for idx, item in enumerate(items):
        y = 104 + idx * 54
        status = str(item.get("status", "unchanged"))
        color = "#0f766e" if status == "improved" else "#dc2626" if status == "regressed" else "#64748b"
        label = escape(str(item.get("label", "metric")))
        previous = _format_delta_value(item.get("previous"))
        current = _format_delta_value(item.get("current"))
        delta_text = _format_signed_value(item.get("delta"))
        rows.append(f'<text x="32" y="{y}" font-size="13" font-weight="700" fill="#111827">{label}</text>')
        rows.append(f'<rect x="260" y="{y - 18}" width="360" height="22" rx="4" fill="#e2e8f0"/>')
        rows.append(f'<rect x="260" y="{y - 18}" width="180" height="22" rx="4" fill="{color}" opacity="0.84"/>')
        rows.append(f'<text x="642" y="{y}" font-size="12" fill="#334155">{previous} -> {current}</text>')
        rows.append(f'<text x="790" y="{y}" font-size="12" font-weight="700" fill="{color}">{delta_text}</text>')
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_complexity_boundary_svg(payload: dict[str, Any]) -> str:
    summary = (payload.get("complexity_boundary") or {}).get("summary") or {}
    items = list(summary.items())
    width = 920
    height = 122 + 62 * max(1, len(items))
    rows = [
        _svg_header(width, height),
        f'<rect width="920" height="{height}" fill="#ffffff"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Complexity and boundary discipline</text>',
        '<text x="32" y="66" font-size="13" fill="#475569">Higher is better: smaller decision paths, cleaner boundaries, and stronger DI contract pressure.</text>',
    ]
    if not items:
        rows.append(
            '<text x="286" y="152" font-size="14" fill="#64748b">Complexity and boundary evidence appears after benchmark generation.</text>'
        )
        rows.append("</svg>\n")
        return "\n".join(rows)
    for idx, (slug, item) in enumerate(items):
        y = 102 + idx * 62
        score = _as_int(item.get("overall_score"))
        color = "#0f766e" if score >= 90 else "#2563eb" if score >= 80 else "#f59e0b" if score >= 65 else "#dc2626"
        width_value = max(2, int((score / 100.0) * 530))
        label = escape(str(item.get("name", slug)))
        rows.append(f'<text x="32" y="{y + 16}" font-size="13" font-weight="700" fill="#111827">{label}</text>')
        rows.append(f'<rect x="210" y="{y}" width="530" height="22" rx="4" fill="#e2e8f0"/>')
        rows.append(f'<rect x="210" y="{y}" width="{width_value}" height="22" rx="4" fill="{color}"/>')
        rows.append(
            f'<text x="758" y="{y + 16}" font-size="12" fill="#334155">{score} / violations {_as_int(item.get("boundary_violation_count"))}</text>'
        )
        rows.append(
            f'<text x="210" y="{y + 42}" font-size="11" fill="#64748b">complexity {_as_int(item.get("complexity_score"))} · boundary {_as_int(item.get("boundary_score"))} · DI {_as_int(item.get("di_score"))}</text>'
        )
    rows.append("</svg>\n")
    return "\n".join(rows)


def render_quality_trend_svg(history: dict[str, Any]) -> str:
    entries = list(history.get("entries", []))[-8:]
    width = 920
    height = 360
    left = 72
    right = 848
    top = 76
    bottom = 292
    rows = [
        _svg_header(width, height),
        '<rect width="920" height="360" fill="#f8fafc"/>',
        '<text x="32" y="42" font-size="23" font-weight="700" fill="#111827">Industrial Maintainability Index trend</text>',
        '<text x="32" y="64" font-size="13" fill="#475569">0-100 score from static maintainability, test-footprint, and freshness evidence.</text>',
        f'<line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" stroke="#94a3b8"/>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{bottom}" stroke="#94a3b8"/>',
        f'<text x="32" y="{top + 4}" font-size="11" fill="#64748b">100</text>',
        f'<text x="40" y="{bottom + 4}" font-size="11" fill="#64748b">0</text>',
    ]
    if not entries:
        rows.append(
            '<text x="290" y="188" font-size="14" fill="#64748b">Trend history will appear after the first benchmark run.</text>'
        )
        rows.append("</svg>\n")
        return "\n".join(rows)

    slugs = sorted(entries[-1].get("projects", {}))
    colors = ("#0f766e", "#2563eb", "#f59e0b", "#dc2626", "#7c3aed")
    x_step = (right - left) / max(1, len(entries) - 1)
    for idx, slug in enumerate(slugs):
        points: list[tuple[float, float]] = []
        for entry_idx, entry in enumerate(entries):
            project = entry.get("projects", {}).get(slug)
            if not project:
                continue
            score = max(0.0, min(100.0, _as_float(project.get("score"))))
            x = left + (entry_idx * x_step)
            y = bottom - ((score / 100.0) * (bottom - top))
            points.append((x, y))
        if not points:
            continue
        color = colors[idx % len(colors)]
        point_text = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
        rows.append(f'<polyline points="{point_text}" fill="none" stroke="{color}" stroke-width="3"/>')
        for x, y in points:
            rows.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}"/>')
        last_x, last_y = points[-1]
        label = escape(entries[-1].get("projects", {}).get(slug, {}).get("name", slug))
        rows.append(f'<text x="{last_x + 8:.1f}" y="{last_y + 4:.1f}" font-size="12" fill="#111827">{label}</text>')
    rows.append("</svg>\n")
    return "\n".join(rows)


def _svg_header(width: int, height: int) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">'
    )


def _format_delta_value(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if number.is_integer():
        return f"{int(number)}"
    return f"{number:.3f}".rstrip("0").rstrip(".")


def _format_signed_value(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if number == 0:
        return "0"
    if number.is_integer():
        return f"{int(number):+d}"
    return f"{number:+.3f}".rstrip("0").rstrip(".")
