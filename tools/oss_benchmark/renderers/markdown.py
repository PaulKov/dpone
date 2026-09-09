"""Markdown renderer for the OSS code-quality benchmark."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import (
    find_project as _find_project,
)
from tools.oss_benchmark.payload_utils import (
    format_float as _format_float,
)
from tools.oss_benchmark.payload_utils import (
    get_value as _get,
)
from tools.oss_benchmark.payload_utils import (
    project_name as _project_name,
)
from tools.oss_benchmark.renderers.architecture import render_architecture_taxonomy_section
from tools.oss_benchmark.renderers.budgets import render_quality_budgets_section
from tools.oss_benchmark.renderers.certification import render_runtime_certification_section
from tools.oss_benchmark.renderers.certification_runtime import render_executable_certification_section
from tools.oss_benchmark.renderers.claims import render_claims_ledger_section
from tools.oss_benchmark.renderers.complexity_boundary import render_complexity_boundary_section
from tools.oss_benchmark.renderers.dpone_position import render_contract_position
from tools.oss_benchmark.renderers.evidence_trust import render_evidence_trust_section
from tools.oss_benchmark.renderers.exports import render_evidence_exports_section
from tools.oss_benchmark.renderers.governance import render_governance_compliance_section
from tools.oss_benchmark.renderers.independent_validation import render_independent_validation_section
from tools.oss_benchmark.renderers.intelligence import render_benchmark_intelligence_sections
from tools.oss_benchmark.renderers.markdown_feature_parity import render_feature_parity_section
from tools.oss_benchmark.renderers.markdown_header import (
    render_header,
    render_table_of_contents,
    render_tool_overview,
)
from tools.oss_benchmark.renderers.markdown_quality_sections import (
    render_architecture_risk,
    render_coverage_confidence,
    render_file_table,
    render_maintainability_index,
    render_quality_gates,
    render_quality_reading,
    render_release_delta_section,
    render_scorecard_table,
    render_trust_center_snapshot,
)
from tools.oss_benchmark.renderers.methodology import render_static_methodology_sections
from tools.oss_benchmark.renderers.operability import render_operability_tco_section
from tools.oss_benchmark.renderers.public_evidence_integrity import render_public_evidence_integrity_section
from tools.oss_benchmark.renderers.refactor_roi import render_refactor_roi_section
from tools.oss_benchmark.renderers.release_readiness import render_release_readiness_section
from tools.oss_benchmark.renderers.reliability import render_operational_reliability_section
from tools.oss_benchmark.renderers.scale_readiness import render_scale_readiness_section
from tools.oss_benchmark.renderers.scoring_calibration import render_scoring_calibration_section
from tools.oss_benchmark.renderers.security import render_security_supply_chain_section
from tools.oss_benchmark.renderers.semantic_maintainability import render_semantic_maintainability_section
from tools.oss_benchmark.renderers.source_verification import render_source_verification_section
from tools.oss_benchmark.renderers.trends import (
    render_architecture_delta_section,
    render_trend_history_section,
)

__all__ = ["render_markdown", "render_feature_parity_section", "render_file_table"]


def render_markdown(payload: dict[str, Any]) -> str:
    projects = list(payload.get("projects", []))
    lines: list[str] = []
    lines.extend(render_header(payload))
    lines.extend(render_table_of_contents(projects))
    lines.extend(render_tool_overview(projects))
    lines.extend(render_trust_center_snapshot(payload))
    _append_text(lines, render_release_readiness_section(payload))
    _append_text(lines, render_claims_ledger_section(payload))
    _append_text(lines, render_runtime_certification_section(payload))
    _append_text(lines, render_executable_certification_section(payload))
    _append_text(lines, render_quality_budgets_section(payload))
    _append_text(lines, render_evidence_exports_section(payload))
    _append_text(lines, render_evidence_trust_section(payload))
    _append_text(lines, render_public_evidence_integrity_section(payload))
    _append_text(lines, render_source_verification_section(payload))
    _append_text(lines, render_independent_validation_section(payload))
    _append_text(lines, render_feature_parity_section(payload.get("feature_parity") or {}))
    _append_text(lines, render_governance_compliance_section(payload.get("governance_compliance") or {}))
    _append_text(lines, render_security_supply_chain_section(payload.get("security_supply_chain") or {}))
    _append_text(lines, render_operational_reliability_section(payload.get("operational_reliability") or {}))
    _append_text(lines, render_operability_tco_section(payload.get("operability_tco") or {}))
    lines.extend(render_maintainability_index(projects, payload.get("trend_summary", {})))
    lines.extend(render_release_delta_section(payload))
    lines.extend(render_quality_gates(payload))
    lines.extend(render_benchmark_intelligence_sections(payload))
    lines.extend(render_trend_history_section(payload))
    lines.extend(render_architecture_delta_section(payload))
    _append_text(lines, render_complexity_boundary_section(payload))
    _append_text(lines, render_semantic_maintainability_section(payload))
    _append_text(lines, render_scoring_calibration_section(payload))
    _append_text(lines, render_scale_readiness_section(payload))
    _append_text(lines, render_refactor_roi_section(payload))
    lines.extend(render_coverage_confidence(projects))
    lines.extend(render_architecture_risk(projects))
    _append_text(lines, render_architecture_taxonomy_section(payload.get("architecture_taxonomy") or {}))
    lines.extend(render_scorecard_table(payload))
    lines.extend(render_static_methodology_sections())
    lines.extend(render_quality_reading(projects))
    lines.extend(_render_score_deltas(payload))
    lines.extend(_render_dpone_position(payload))
    lines.extend(_render_project_appendix(projects))
    lines.extend(_render_reproducibility(payload))
    return "\n".join(lines)


def _append_text(lines: list[str], section: str) -> None:
    if section:
        lines.extend(section.splitlines())


def _render_score_deltas(payload: dict[str, Any]) -> list[str]:
    projects = list(payload.get("projects", []))
    deltas = payload.get("score_deltas") or {}
    if not deltas:
        return []
    lines = ["## Score deltas", ""]
    for slug, delta in sorted(deltas.items()):
        lines.append(
            f"- {_project_name(_find_project(projects, slug))}: SOLID `{delta.get('solid', 0):+g}`, "
            f"Clean OOP `{delta.get('clean_oop', 0):+g}` versus previous evidence."
        )
    lines.append("")
    return lines


def _render_dpone_position(payload: dict[str, Any]) -> list[str]:
    projects = list(payload.get("projects", []))
    dpone = _find_project(projects, "dpone")
    taxonomy = ((payload.get("architecture_taxonomy") or {}).get("summary") or {}).get("dpone") or {}
    dpone_avg_ce = _format_float(_get(dpone, "coupling", "avg_ce"), digits=2)
    return [
        "## dpone position",
        "",
        f"dpone is much smaller than Airbyte, Pentaho Kettle and Apache Hop, but its current quality posture is competitive because the codebase keeps production modules under the release SLOC gate, maintains low average fan-out (`{dpone_avg_ce}`), and exposes quality metrics as a first-class documented gate.",
        "",
        render_contract_position(taxonomy),
        "",
        "The main comparative advantage is architectural controllability: dpone can keep industrial ETL features, route certification, source-sink matrices and runtime evidence under a tight module-size and dependency budget while larger platforms carry more historical surface area. The next quality target is to keep clustering and contract conformance inside the green zone as new runtime capabilities land, so the product can grow toward Airbyte/Pentaho/Hop feature breadth without inheriting their maintainability drag.",
        "",
    ]


def _render_project_appendix(projects: list[dict[str, Any]]) -> list[str]:
    lines = ["## Appendix: detailed metric tables", ""]
    for project in projects:
        lines.append(f"### {_project_name(project)} top modules")
        lines.append("")
        if project.get("unavailable"):
            lines.append("Metrics are unavailable for this project.")
            lines.append("")
            continue
        _append_top_module_tables(lines, project)
    return lines


def _append_top_module_tables(lines: list[str], project: dict[str, Any]) -> None:
    tables = (
        ("Top 15 modules by LOC (with tests)", "top_loc_with_tests"),
        ("Top 15 modules by SLOC (with tests)", "top_sloc_with_tests"),
        ("Top 15 modules by LOC (without tests)", "top_loc_without_tests"),
        ("Top 15 modules by SLOC (without tests)", "top_sloc_without_tests"),
    )
    for title, key in tables:
        lines.append(f"#### {title}")
        lines.append("")
        lines.extend(render_file_table(_get(project, key, default=[])))
        lines.append("")


def _render_reproducibility(payload: dict[str, Any]) -> list[str]:
    generated_at = str(payload.get("generated_at", "unknown"))
    release_context = payload.get("release_context") or {}
    return [
        "## Reproducibility",
        "",
        "```bash",
        "uv run python tools/oss_code_quality_benchmark.py --include-external --allow-stale --previous-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json --baseline-data docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json",
        "```",
        "",
        f"Generated at `{generated_at}` for dpone `{release_context.get('release_tag') or 'unknown release'}`. Static analysis is approximate by design and should be read as a maintainability benchmark, not as a feature or performance benchmark.",
        "",
    ]
