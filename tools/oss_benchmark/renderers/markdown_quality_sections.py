"""Quality, gate, and project-detail Markdown sections."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.models import FileMetric
from tools.oss_benchmark.payload_utils import (
    dirty_label as _dirty_label,
)
from tools.oss_benchmark.payload_utils import (
    format_delta as _format_delta,
)
from tools.oss_benchmark.payload_utils import (
    format_float as _format_float,
)
from tools.oss_benchmark.payload_utils import (
    format_gate_value as _format_gate_value,
)
from tools.oss_benchmark.payload_utils import (
    format_hotspot as _format_hotspot,
)
from tools.oss_benchmark.payload_utils import (
    format_int as _format_int,
)
from tools.oss_benchmark.payload_utils import (
    format_percent as _format_percent,
)
from tools.oss_benchmark.payload_utils import (
    format_score as _format_score,
)
from tools.oss_benchmark.payload_utils import (
    get_value as _get,
)
from tools.oss_benchmark.payload_utils import (
    project_commit as _project_commit,
)
from tools.oss_benchmark.payload_utils import (
    project_name as _project_name,
)
from tools.oss_benchmark.payload_utils import (
    project_slug as _project_slug,
)
from tools.oss_benchmark.payload_utils import (
    test_footprint as _test_footprint,
)
from tools.oss_benchmark.payload_utils import (
    test_sloc as _test_sloc,
)
from tools.oss_benchmark.state import project_freshness_label


def render_trust_center_snapshot(payload: dict[str, Any]) -> list[str]:
    trust = payload.get("trust_center") or {}
    badge = trust.get("badge") or {}
    return [
        "",
        "## Customer Trust Center Snapshot",
        "",
        "A customer-ready trust center export is generated with every benchmark refresh for sales, security review and procurement evidence packets.",
        "",
        "![dpone trust center badge](assets/dpone-trust-center-badge.svg)",
        "",
        "| Export | Value |",
        "|---|---|",
        f"| Status | `{trust.get('status', 'n/a')}` |",
        f"| Badge | `{badge.get('label', 'n/a')}` / `{badge.get('score', 'n/a')}` |",
        "| Markdown snapshot | `docs/benchmarks/dpone-trust-center-snapshot-2026-06-12.md` ([open snapshot](dpone-trust-center-snapshot-2026-06-12.md)) |",
        "| JSON snapshot | `docs/benchmarks/data/dpone-trust-center-snapshot-2026-06-12.json` ([open JSON](data/dpone-trust-center-snapshot-2026-06-12.json)) |",
        "| Badge asset | `docs/benchmarks/assets/dpone-trust-center-badge.svg` |",
        "",
    ]


def render_maintainability_index(projects: list[dict[str, Any]], trends: dict[str, dict[str, Any]]) -> list[str]:
    lines = [
        "",
        "## Industrial Maintainability Index",
        "",
        "The Industrial Maintainability Index is a 0-100 executive KPI layered over the raw evidence. It combines SOLID/Clean OOP score, largest production module size, import coupling, cohesion/clustering, static test footprint, and data freshness. Raw metric tables remain below for auditability.",
        "",
        "| Project | Index | Band | Delta | Quality | Module size | Coupling | Cohesion | Test footprint | Freshness |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for project in projects:
        index = project.get("industrial_maintainability") or {}
        components = index.get("components") or {}
        trend = trends.get(_project_slug(project), {})
        lines.append(
            "| "
            f"{_project_name(project)} | "
            f"{_format_int(index.get('score'))} | "
            f"{index.get('band', 'n/a')} | "
            f"{_format_delta(trend.get('score'))} | "
            f"{_format_float(components.get('quality'), digits=1)} | "
            f"{_format_float(components.get('module_size'), digits=1)} | "
            f"{_format_float(components.get('coupling'), digits=1)} | "
            f"{_format_float(components.get('cohesion'), digits=1)} | "
            f"{_format_float(components.get('test_footprint'), digits=1)} | "
            f"{_format_float(components.get('freshness'), digits=1)} |"
        )
    return lines


def render_release_delta_section(payload: dict[str, Any]) -> list[str]:
    delta = payload.get("release_delta") or {}
    project_delta = (delta.get("projects") or {}).get("dpone") or {}
    metrics = project_delta.get("metrics") or {}
    lines = [
        "",
        "## Release Delta",
        "",
        "Release delta compares current merged evidence with the selected baseline. Stale comparisons are excluded from improvement claims and rendered as not comparable.",
        "",
        "| Metric | Previous | Current | Delta | Classification |",
        "|---|---:|---:|---:|---|",
    ]
    for metric_name, metric in metrics.items():
        lines.append(
            "| "
            f"{metric_name.replace('_', ' ')} | "
            f"{_format_gate_value(metric.get('previous'))} | "
            f"{_format_gate_value(metric.get('current'))} | "
            f"{_format_gate_value(metric.get('delta'))} | "
            f"{metric.get('classification', 'n/a')} |"
        )
    blockers = delta.get("blocking_regressions") or []
    lines.extend(["", f"Blocking release regressions: `{len(blockers)}`.", ""])
    for blocker in blockers[:8]:
        lines.append(f"- `{blocker.get('metric', 'n/a')}`: {blocker.get('reason', 'regression')}.")
    if blockers:
        lines.append("")
    return lines


def render_quality_gates(payload: dict[str, Any]) -> list[str]:
    gates = payload.get("quality_gates") or {}
    lines = [
        "",
        "## Quality Gates",
        "",
        "Quality gates turn the benchmark into a governance control for dpone. They are evaluated from the merged evidence payload after stale preservation, so CI blocks regressions without erasing previous comparator data.",
        "",
        f"Current gate status: **{gates.get('status', 'n/a')}**.",
        "",
        "| Gate | Actual | Target | Status | Severity |",
        "|---|---:|---:|---|---|",
    ]
    for check in gates.get("checks", []):
        lines.append(
            "| "
            f"{check.get('label', 'n/a')} | "
            f"{_format_gate_value(check.get('actual'))} | "
            f"{check.get('operator', '')} {_format_gate_value(check.get('threshold'))} | "
            f"{check.get('status', 'n/a')} | "
            f"{check.get('severity', 'n/a')} |"
        )
    lines.extend(
        [
            "",
            "The manual CI workflow also writes a **PR benchmark summary** at `test_artifacts/oss-code-quality-benchmark/pr-comment.md`, so reviewers can see gate status, score movement, freshness, and dpone hotspots without opening the full report.",
            "",
        ]
    )
    return lines


def render_coverage_confidence(projects: list[dict[str, Any]]) -> list[str]:
    lines = [
        "",
        "## Coverage Confidence Matrix",
        "",
        "Coverage confidence separates measured runtime coverage from static test footprint. The benchmark does not claim branch/line coverage for external projects unless a coverage signal is visible in the source or CI configuration.",
        "",
        "| Project | Confidence | Score | Runtime coverage | Test footprint | Test file ratio | CI evidence | Coverage config | Evidence |",
        "|---|---|---:|---|---:|---:|---|---|---|",
    ]
    for project in projects:
        confidence = project.get("coverage_confidence") or {}
        ci = project.get("ci_evidence") or {}
        evidence = "; ".join((confidence.get("evidence") or [])[:2])
        lines.append(
            "| "
            f"{_project_name(project)} | "
            f"{confidence.get('confidence', 'n/a')} | "
            f"{_format_int(confidence.get('score'))} | "
            f"{confidence.get('runtime_coverage', 'n/a')} | "
            f"{_format_percent(confidence.get('test_footprint_ratio'))} | "
            f"{_format_percent(confidence.get('test_file_ratio'))} | "
            f"{'yes' if ci.get('has_ci') else 'no'} | "
            f"{'yes' if ci.get('has_coverage_config') else 'no'} | "
            f"{evidence} |"
        )
    return lines


def render_architecture_risk(projects: list[dict[str, Any]]) -> list[str]:
    lines = [
        "",
        "## Architecture Risk Heatmap",
        "",
        "![Architecture risk heatmap](assets/oss-architecture-risk-heatmap.svg)",
        "",
        "The heatmap turns hotspot evidence into a risk score. It highlights where future changes are likely to be expensive: oversized modules, fan-out/fan-in concentration, low cohesion, and dependency clustering.",
        "",
        "| Project | Risk | Score | Top hotspot | Max LOC | Max Ce | Max Ca | Cohesion | Clustering |",
        "|---|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    for project in projects:
        risk = project.get("architecture_risk") or {}
        hotspots = risk.get("hotspots") or []
        top_hotspot = _format_hotspot(hotspots[0]) if hotspots else "none inside thresholds"
        lines.append(
            "| "
            f"{_project_name(project)} | "
            f"{risk.get('level', 'n/a')} | "
            f"{_format_int(risk.get('score'))} | "
            f"{top_hotspot} | "
            f"{_format_int(_get(project, 'loc_without_tests', 'max_lines'))} | "
            f"{_format_int(_get(project, 'coupling', 'max_ce'))} | "
            f"{_format_int(_get(project, 'coupling', 'max_ca'))} | "
            f"{_format_float(_get(project, 'coupling', 'cohesion_ratio'), digits=3)} | "
            f"{_format_float(_get(project, 'coupling', 'avg_clustering'), digits=3)} |"
        )
    return [*lines, "", "### Architecture risk drill-down", "", *render_hotspot_drilldown(projects)]


def render_scorecard_table(payload: dict[str, Any]) -> list[str]:
    projects = list(payload.get("projects", []))
    lines = [
        "",
        "## Executive scorecard",
        "",
        "| Project | Corpus | Source commit | Files without tests | LOC without tests | SLOC without tests | Max LOC | Max SLOC | Avg Ce | P90 Ce | Cohesion | Clustering | SOLID | Clean OOP | Test footprint | Release delta | Stale age | Freshness |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---|",
    ]
    for project in projects:
        slug = _project_slug(project)
        lines.append(
            "| "
            f"{_project_name(project)}{_dirty_label(project)} | "
            f"{_get(project, 'spec', 'kind', default='n/a')} | "
            f"{_project_commit(project)} | "
            f"{_format_int(_get(project, 'loc_without_tests', 'files'))} | "
            f"{_format_int(_get(project, 'loc_without_tests', 'total_lines'))} | "
            f"{_format_int(_get(project, 'loc_without_tests', 'total_sloc'))} | "
            f"{_format_int(_get(project, 'loc_without_tests', 'max_lines'))} | "
            f"{_format_int(_get(project, 'loc_without_tests', 'max_sloc'))} | "
            f"{_format_float(_get(project, 'coupling', 'avg_ce'), digits=2)} | "
            f"{_format_float(_get(project, 'coupling', 'p90_ce'), digits=0)} | "
            f"{_format_float(_get(project, 'coupling', 'cohesion_ratio'), digits=3)} | "
            f"{_format_float(_get(project, 'coupling', 'avg_clustering'), digits=3)} | "
            f"{_format_score(_get(project, 'quality', 'solid'))} | "
            f"{_format_score(_get(project, 'quality', 'clean_oop'))} | "
            f"{_test_footprint(project)} | "
            f"{_project_delta_label(payload, slug)} | "
            f"{_format_int(_max_stale_age(project))} | "
            f"{project_freshness_label(project)} |"
        )
    return lines


def render_quality_reading(projects: list[dict[str, Any]]) -> list[str]:
    lines = ["## Quality reading", ""]
    for project in projects:
        lines.append(f"### {_project_name(project)}")
        lines.append("")
        if project.get("unavailable"):
            lines.append(f"- Freshness: {project_freshness_label(project)}.")
            lines.append("- Metrics: `n/a` because no previous evidence exists for the failed refresh.")
            lines.append("")
            continue
        lines.append(f"- Freshness: {project_freshness_label(project)}.")
        lines.append(
            f"- Test footprint: `{_test_footprint(project)}` based on `{_format_int(_test_sloc(project))}` test SLOC over `{_format_int(_get(project, 'loc_without_tests', 'total_sloc'))}` production SLOC."
        )
        lines.append(
            f"- Coupling: avg Ce `{_format_float(_get(project, 'coupling', 'avg_ce'), digits=2)}`, P90 Ce `{_format_float(_get(project, 'coupling', 'p90_ce'), digits=0)}`, max Ce `{_format_int(_get(project, 'coupling', 'max_ce'))}` at `{_get(project, 'coupling', 'max_ce_module', default='n/a') or 'n/a'}`."
        )
        lines.append(
            f"- Cohesion: `{_format_float(_get(project, 'coupling', 'cohesion_ratio'), digits=3)}` within slice, cross-slice ratio `{_format_float(_get(project, 'coupling', 'cross_slice_ratio'), digits=3)}`, clustering `{_format_float(_get(project, 'coupling', 'avg_clustering'), digits=3)}`."
        )
        evidence = "; ".join((_get(project, "quality", "evidence", default=[]) or [])[:4])
        lines.append(
            f"- SOLID `{_format_score(_get(project, 'quality', 'solid'))}`, Clean OOP `{_format_score(_get(project, 'quality', 'clean_oop'))}`: {evidence}."
        )
        lines.append("")
    return lines


def render_hotspot_drilldown(projects: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for project in projects:
        risk = project.get("architecture_risk") or {}
        hotspots = risk.get("hotspots") or []
        approved_label = _approved_contract_label(risk)
        details = (
            "; ".join(_format_hotspot(item) for item in hotspots[:5])
            if hotspots
            else "no hotspot crossed the benchmark thresholds."
        )
        lines.append(f"- **{_project_name(project)}:** {details}{approved_label}")
    return lines


def render_file_table(items: list[dict[str, Any]] | tuple[FileMetric, ...]) -> list[str]:
    lines = ["| Module | LOC | SLOC | Test? |", "|---|---:|---:|---|"]
    for item in items:
        path = item.path if isinstance(item, FileMetric) else item.get("path", "n/a")
        lines_count = item.lines if isinstance(item, FileMetric) else item.get("lines", "n/a")
        sloc = item.sloc if isinstance(item, FileMetric) else item.get("sloc", "n/a")
        is_test = item.is_test if isinstance(item, FileMetric) else item.get("is_test", False)
        lines.append(f"| `{path}` | {lines_count} | {sloc} | {'yes' if is_test else 'no'} |")
    return lines


def _project_delta_label(payload: dict[str, Any], slug: str) -> str:
    project_delta = ((payload.get("release_delta") or {}).get("projects") or {}).get(slug) or {}
    metrics = project_delta.get("metrics") or {}
    classes = {str(metric.get("classification")) for metric in metrics.values()}
    if "regressed" in classes:
        return "regressed"
    if "improved" in classes:
        return "improved"
    if "not_comparable_stale" in classes:
        return "not comparable"
    if metrics:
        return "unchanged"
    return "n/a"


def _max_stale_age(project: dict[str, Any]) -> int | None:
    ages = [
        group.get("stale_age_days")
        for group in (project.get("metric_groups") or {}).values()
        if isinstance(group.get("stale_age_days"), int)
    ]
    return max(ages) if ages else None


def _approved_contract_label(risk: dict[str, Any]) -> str:
    approved_contracts = risk.get("approved_fan_in_contracts") or []
    if not approved_contracts:
        return " Approved high fan-in contracts: none in current top fan-in candidates."
    contracts = ", ".join(
        f"`{item.get('module', 'n/a')}` ({item.get('kind', 'contract')}, Ca {_format_int(item.get('fan_in'))})"
        for item in approved_contracts[:5]
    )
    return f" Approved high fan-in contracts: {contracts}"
