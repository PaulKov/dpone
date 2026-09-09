"""Header, navigation, and overview sections for the benchmark report."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import (
    anchor as _anchor,
)
from tools.oss_benchmark.payload_utils import (
    find_project as _find_project,
)
from tools.oss_benchmark.payload_utils import (
    format_int as _format_int,
)
from tools.oss_benchmark.payload_utils import (
    get_value as _get,
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
    test_loc as _test_loc,
)
from tools.oss_benchmark.payload_utils import (
    test_sloc as _test_sloc,
)
from tools.oss_benchmark.state import project_freshness_label

TOOL_DESCRIPTIONS = {
    "dpone": "Local dpone framework snapshot, measured as the reference implementation for industrial ETL quality posture.",
    "airbyte": "Open-source data movement platform with a broad connector and platform codebase.",
    "dlt": "Open-source Python data loading library focused on developer-first ELT pipelines.",
    "pentaho-kettle": "Mature open-source ETL/data-integration baseline from the Pentaho Kettle lineage.",
    "apache-hop": "Modern open-source orchestration and data-integration platform from the Kettle/Hop lineage.",
    "sling": "Open-source CLI-first data movement tool for database and file replication workflows.",
}


def render_header(payload: dict[str, Any]) -> list[str]:
    run_context = payload.get("run_context") or {}
    release_context = payload.get("release_context") or {}
    summary = payload.get("freshness_summary") or {}
    generated_at = str(payload.get("generated_at", "unknown"))
    gates = payload.get("quality_gates") or {}
    governance = payload.get("freshness_governance") or {}
    lines = [
        "# OSS code quality benchmark 2026-06-12",
        "",
        "This benchmark compares dpone with open-source data-integration codebases using static maintainability proxies: LOC/SLOC, module hotspots, import coupling, cohesion, SOLID correspondence and Clean OOP correspondence. It also includes an evidence-bounded feature parity matrix from public product documentation; it does not claim runtime performance.",
        "",
        "| Refresh metadata | Value |",
        "|---|---|",
        f"| Schema version | `{payload.get('schema_version', 1)}` |",
        f"| Last refresh | `{run_context.get('generated_at') or generated_at}` |",
        f"| Last manual CI runner | `{run_context.get('updated_by') or 'local'}` |",
        f"| Workflow | `{run_context.get('workflow_name') or 'local'}` |",
        f"| GitHub Actions run | {_run_label(run_context)} |",
        f"| Source revision | `{run_context.get('branch') or 'local'}@{run_context.get('git_sha') or 'unknown'}` |",
        f"| dpone release under test | `{release_context.get('dpone_version') or 'unknown'}` / `{release_context.get('release_tag') or 'no tag'}` / `{release_context.get('release_sha') or 'unknown sha'}` |",
        f"| Release resolution | `{release_context.get('resolved_by') or 'unresolved'}`; dirty `{release_context.get('dirty', 'unknown')}` |",
        f"| Freshness | {render_freshness_rollup(summary)} |",
        f"| Freshness max age policy | `{governance.get('max_stale_days', 'n/a')}` days; warnings `{governance.get('warning_count', 0)}` |",
        f"| Quality gates | `{gates.get('status', 'n/a')}` ({gates.get('passed', 0)} passed, {gates.get('failed', 0)} failed) |",
        "",
    ]
    if summary.get("stale", 0) or summary.get("unavailable", 0) or governance.get("warning_count", 0):
        lines.extend(render_freshness_warning())
    return lines


def render_freshness_rollup(summary: dict[str, Any]) -> str:
    return (
        f"fresh `{summary.get('fresh', 0)}`, stale `{summary.get('stale', 0)}`, "
        f"unavailable `{summary.get('unavailable', 0)}`"
    )


def render_freshness_warning() -> list[str]:
    return [
        "> **Freshness warning:** at least one metric group could not be refreshed or exceeded the stale-age policy. The benchmark keeps the last known value when prior evidence exists and marks it stale instead of overwriting it.",
        "",
    ]


def render_table_of_contents(projects: list[dict[str, Any]]) -> list[str]:
    lines = [
        "## Table of contents",
        "",
        "### General comparisons",
        "",
        "- [Tool overview and total corpus](#tool-overview-and-total-corpus)",
        "- [Customer Trust Center Snapshot](#customer-trust-center-snapshot)",
        "- [Benchmark v3 Release Readiness](#benchmark-v3-release-readiness)",
        "- [Claims Ledger](#claims-ledger)",
        "- [Runtime Certification Matrix](#runtime-certification-matrix)",
        "- [Executable Certification](#executable-certification)",
        "- [Golden Dataset Evidence](#golden-dataset-evidence)",
        "- [Run Ledger](#run-ledger)",
        "- [Certification Gates](#certification-gates)",
        "- [Quality Budget As Code](#quality-budget-as-code)",
        "- [Evidence Warehouse Export](#evidence-warehouse-export)",
        "- [Evidence Trust & Auditability](#evidence-trust-auditability)",
        "- [Public Evidence Integrity](#public-evidence-integrity)",
        "- [Source Citation Verification](#source-citation-verification)",
        "- [Independent Analyzer Cross-Validation & Audit Pack](#independent-analyzer-cross-validation-audit-pack)",
        "- [Feature Parity Matrix](#feature-parity-matrix)",
        "- [Governance & Compliance](#governance-compliance)",
        "- [Security & Supply Chain](#security-supply-chain)",
        "- [Operational Reliability](#operational-reliability)",
        "- [TCO & Operability](#tco-operability)",
        "- [Industrial Maintainability Index](#industrial-maintainability-index)",
        "- [Release Delta](#release-delta)",
        "- [Quality Gates](#quality-gates)",
        "- [Explainable scoring](#explainable-scoring)",
        "- [Regression summary](#regression-summary)",
        "- [Candidate quality delta](#candidate-quality-delta)",
        "- [Remediation backlog](#remediation-backlog)",
        "- [Trend history](#trend-history)",
        "- [Architecture delta](#architecture-delta)",
        "- [Complexity & Boundary Discipline](#complexity-boundary-discipline)",
        "- [Semantic Maintainability Deep Scan](#semantic-maintainability-deep-scan)",
        "- [Scoring Validity & Calibration](#scoring-validity-calibration)",
        "- [Scale Readiness & Growth Simulation](#scale-readiness-growth-simulation)",
        "- [Refactor ROI Roadmap](#refactor-roi-roadmap)",
        "- [Coverage Confidence Matrix](#coverage-confidence-matrix)",
        "- [Architecture Risk Heatmap](#architecture-risk-heatmap)",
        "- [Architecture Taxonomy & Contract Discipline](#architecture-taxonomy-contract-discipline)",
        "- [Executive scorecard](#executive-scorecard)",
        "- [Comparable OSS corpus](#comparable-oss-corpus)",
        "- [Methodology](#methodology)",
        "- [Data freshness policy](#data-freshness-policy)",
        "- [Quality reading](#quality-reading)",
        "- [dpone position](#dpone-position)",
        "",
        "### Detailed project drill-downs",
        "",
    ]
    for project in projects:
        name = _project_name(project)
        anchor = _anchor(name)
        lines.append(f"- [{name}: quality reading](#{anchor})")
        lines.append(f"- [{name}: top modules with tests and without tests](#{anchor}-top-modules)")
    lines.append("")
    return lines


def render_tool_overview(projects: list[dict[str, Any]]) -> list[str]:
    lines = [
        "![OSS quality scorecard](assets/oss-quality-scorecard.svg)",
        "",
        "## Tool overview and total corpus",
        "",
        "Test coverage here is a static **Test footprint** proxy: test SLOC divided by production SLOC. It shows how much test code is present in the published source tree, not runtime branch/line coverage from each project's own test runner.",
        "",
        "| Project | Description | Total LOC | Total SLOC | LOC without tests | SLOC without tests | Test LOC | Test SLOC | Test footprint | Freshness |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for project in projects:
        lines.append(
            "| "
            f"{_project_name(project)} | "
            f"{TOOL_DESCRIPTIONS.get(_project_slug(project), 'Comparable OSS data-integration project.')} | "
            f"{_format_int(_get(project, 'loc_with_tests', 'total_lines'))} | "
            f"{_format_int(_get(project, 'loc_with_tests', 'total_sloc'))} | "
            f"{_format_int(_get(project, 'loc_without_tests', 'total_lines'))} | "
            f"{_format_int(_get(project, 'loc_without_tests', 'total_sloc'))} | "
            f"{_format_int(_test_loc(project))} | "
            f"{_format_int(_test_sloc(project))} | "
            f"{_test_footprint(project)} | "
            f"{project_freshness_label(project)} |"
        )
    return lines


def render_dpone_position(payload: dict[str, Any]) -> list[str]:
    projects = list(payload.get("projects", []))
    dpone = _find_project(projects, "dpone")
    dpone_avg_ce = _format_int(_get(dpone, "coupling", "avg_ce"))
    return [
        "## dpone position",
        "",
        f"dpone is much smaller than Airbyte, Pentaho Kettle and Apache Hop, but its current quality posture is competitive because the codebase keeps production modules under the release SLOC gate, maintains low average fan-out (`{dpone_avg_ce}`), and exposes quality metrics as a first-class documented gate.",
        "",
    ]


def _run_label(run_context: dict[str, Any]) -> str:
    run_url = str(run_context.get("run_url") or "")
    run_id = str(run_context.get("run_id") or "local")
    return f"[{run_id}]({run_url})" if run_url else run_id
