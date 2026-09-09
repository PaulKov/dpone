"""PR-ready markdown summary for benchmark refresh runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tools.oss_benchmark.pr_calibration import render_scoring_calibration_pr_section
from tools.oss_benchmark.pr_certification import render_executable_certification_pr_section
from tools.oss_benchmark.pr_complexity import render_complexity_boundary_pr_section
from tools.oss_benchmark.pr_evidence import render_evidence_trust_pr_section
from tools.oss_benchmark.pr_public_integrity import render_public_evidence_integrity_pr_section
from tools.oss_benchmark.pr_release_readiness import render_release_readiness_pr_section
from tools.oss_benchmark.pr_roi import render_refactor_roi_pr_section
from tools.oss_benchmark.pr_scale import render_scale_readiness_pr_section
from tools.oss_benchmark.pr_semantic import render_semantic_maintainability_pr_section
from tools.oss_benchmark.pr_source_verification import render_source_verification_pr_section
from tools.oss_benchmark.pr_validation import render_independent_validation_pr_section
from tools.oss_benchmark.quality_gates import evaluate_quality_gates
from tools.oss_benchmark.state import project_freshness_label

PR_SUMMARY_PATH = Path("test_artifacts/oss-code-quality-benchmark/pr-comment.md")


def write_pr_summary(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_pr_summary(payload), encoding="utf-8")


def render_pr_summary(payload: dict[str, Any]) -> str:
    run_context = payload.get("run_context") or {}
    gates = payload.get("quality_gates") or evaluate_quality_gates(payload)
    projects = list(payload.get("projects", []))
    lines = [
        "## OSS code quality benchmark refresh",
        "",
        f"- Last refresh: `{run_context.get('generated_at') or payload.get('generated_at', 'unknown')}`",
        f"- Runner: `{run_context.get('updated_by') or 'local'}`",
        f"- Source revision: `{run_context.get('branch') or 'local'}@{run_context.get('git_sha') or 'unknown'}`",
        f"- Quality gates: **{gates.get('status', 'unknown')}** "
        f"({gates.get('passed', 0)} passed, {gates.get('failed', 0)} failed)",
        f"- Freshness: {_freshness_rollup(payload)}",
        "",
    ]
    run_url = str(run_context.get("run_url") or "")
    if run_url:
        lines.extend([f"[Open GitHub Actions run]({run_url})", ""])

    failed_checks = gates.get("failed_checks") or []
    if failed_checks:
        lines.extend(["### Blocking quality gates", ""])
        for check in failed_checks:
            lines.append(f"- {check['label']}: `{check['actual']}` {check['operator']} `{check['threshold']}` failed")
        lines.append("")
    else:
        lines.extend(["### Quality gate snapshot", ""])
        for check in (gates.get("checks") or [])[:7]:
            lines.append(f"- {check['label']}: `{check['actual']}` {check['operator']} `{check['threshold']}`")
        lines.append("")

    regression = payload.get("regression_summary") or {}
    lines.extend(["### Regression summary", ""])
    lines.append(f"- Status: **{regression.get('status', 'n/a')}**")
    for change in (regression.get("regressions") or [])[:5]:
        lines.append(
            f"- {change.get('label', 'metric')}: `{_format_value(change.get('previous'))}` -> "
            f"`{_format_value(change.get('current'))}` ({_format_delta(change.get('delta'))})"
        )
    if not regression.get("regressions"):
        lines.append("- No dpone quality regression detected against previous evidence.")
    lines.append("")

    gate = payload.get("pr_regression_gate") or {}
    gate_summary = gate.get("summary") or {}
    lines.extend(["### PR regression gate", ""])
    lines.append(
        f"- Status: **{gate.get('status', 'n/a')}** "
        f"({gate_summary.get('blockers', 0)} blockers, "
        f"{gate_summary.get('warnings', 0)} warnings, "
        f"{gate_summary.get('improvements', 0)} improvements)"
    )
    for check in (gate.get("checks") or [])[:8]:
        lines.append(
            f"- {check.get('severity', 'info')} {check.get('label', 'metric')}: "
            f"{check.get('status', 'n/a')} ({_format_delta(check.get('delta'))})"
        )
    if not gate.get("checks"):
        lines.append("- No PR regression gate events were generated.")
    lines.append("")
    lines.extend(render_complexity_boundary_pr_section(payload))
    lines.extend(render_semantic_maintainability_pr_section(payload))
    lines.extend(render_scoring_calibration_pr_section(payload))
    lines.extend(render_scale_readiness_pr_section(payload))
    lines.extend(render_refactor_roi_pr_section(payload))
    lines.extend(render_executable_certification_pr_section(payload))
    lines.extend(render_evidence_trust_pr_section(payload))
    lines.extend(render_release_readiness_pr_section(payload))
    lines.extend(render_public_evidence_integrity_pr_section(payload))
    lines.extend(render_source_verification_pr_section(payload))
    lines.extend(render_independent_validation_pr_section(payload))

    candidate_delta = payload.get("candidate_quality_delta") or {}
    lines.extend(["### Candidate quality delta", ""])
    lines.append(f"- Status: **{candidate_delta.get('status', 'n/a')}**")
    baseline_source = candidate_delta.get("baseline_source")
    if baseline_source:
        lines.append(f"- Baseline: `{baseline_source}`")
    failed_budgets = candidate_delta.get("failed_budgets") or []
    if failed_budgets:
        lines.append("- Failed budgets:")
        for budget in failed_budgets[:5]:
            lines.append(
                f"  - {budget.get('label', 'budget')}: `{_format_value(budget.get('actual'))}` "
                f"{budget.get('operator', '')} `{_format_value(budget.get('threshold'))}`"
            )
    else:
        lines.append("- No candidate quality budget failed.")
    changed_modules = candidate_delta.get("changed_modules") or []
    if changed_modules:
        lines.append("- Candidate module watchlist:")
        for module in changed_modules[:5]:
            lines.append(
                f"  - `{module.get('path', 'n/a')}` {module.get('status', 'n/a')} "
                f"LOC {_format_delta(module.get('loc_delta'))}, "
                f"fan-out {_format_delta(module.get('fan_out_delta'))}, risk `{module.get('risk', 'n/a')}`"
            )
    else:
        lines.append("- No production module crossed the candidate watchlist.")
    lines.append("")

    lines.extend(
        [
            "### Score movement",
            "",
            "| Project | Index | Delta | Risk | Coverage confidence | Freshness |",
            "|---|---:|---:|---|---|---|",
        ]
    )
    trends = payload.get("trend_summary") or {}
    for project in projects:
        slug = _project_slug(project)
        index = project.get("industrial_maintainability") or {}
        risk = project.get("architecture_risk") or {}
        coverage = project.get("coverage_confidence") or {}
        trend = trends.get(slug) or {}
        lines.append(
            "| "
            f"{_project_name(project)} | "
            f"{_format_value(index.get('score'))} | "
            f"{_format_delta(trend.get('score'))} | "
            f"{risk.get('level', 'n/a')} ({_format_value(risk.get('score'))}) | "
            f"{coverage.get('confidence', 'n/a')} ({_format_value(coverage.get('score'))}) | "
            f"{project_freshness_label(project)} |"
        )

    dpone = _find_project(projects, "dpone")
    hotspots = ((dpone.get("architecture_risk") or {}).get("hotspots") or [])[:3]
    lines.extend(["", "### dpone architecture hotspots", ""])
    if hotspots:
        for hotspot in hotspots:
            lines.append(f"- {_format_hotspot(hotspot)}")
    else:
        lines.append("- No dpone hotspot crossed the configured benchmark thresholds.")
    lines.append("")

    backlog_items = ((payload.get("remediation_backlog") or {}).get("items") or [])[:5]
    lines.extend(["### Remediation backlog", ""])
    if backlog_items:
        for item in backlog_items:
            lines.append(
                f"- **{item.get('priority', 'P?')}** `{item.get('project', 'n/a')}` "
                f"{item.get('title', 'n/a')}: {item.get('recommendation', 'n/a')}"
            )
    else:
        lines.append("- No remediation backlog items were generated.")
    lines.append("")
    return "\n".join(lines)


def _freshness_rollup(payload: dict[str, Any]) -> str:
    summary = payload.get("freshness_summary") or {}
    return (
        f"fresh `{summary.get('fresh', 0)}`, stale `{summary.get('stale', 0)}`, "
        f"unavailable `{summary.get('unavailable', 0)}`"
    )


def _find_project(projects: list[dict[str, Any]], slug: str) -> dict[str, Any]:
    for project in projects:
        if _project_slug(project) == slug:
            return project
    return {}


def _project_slug(project: dict[str, Any]) -> str:
    return str(_get(project, "spec", "slug", default=""))


def _project_name(project: dict[str, Any]) -> str:
    return str(_get(project, "spec", "name", default="n/a"))


def _get(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _format_value(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:g}"


def _format_delta(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if number == 0:
        return "0"
    return f"{number:+g}"


def _format_hotspot(item: dict[str, Any]) -> str:
    value = item.get("value", "n/a")
    if isinstance(value, float):
        value = f"{value:.3f}"
    return f"{item.get('severity', 'risk')} {item.get('kind', 'hotspot')} `{item.get('label', 'n/a')}` ({value} {item.get('unit', '')})"
