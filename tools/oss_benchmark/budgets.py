"""Quality budget evaluation for benchmark governance."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from tools.oss_benchmark.config import ROOT
from tools.oss_benchmark.payload_utils import as_float, as_int, get_value, project_name, project_slug

DEFAULT_BUDGET_PATH = ROOT / "docs" / "benchmarks" / "quality_budgets.yml"


def evaluate_quality_budgets(
    payload: dict[str, Any],
    *,
    budget_path: Path = DEFAULT_BUDGET_PATH,
) -> dict[str, Any]:
    """Evaluate payload metrics against quality budgets as code."""

    budgets = load_quality_budgets(budget_path)
    findings = _project_findings(payload, budgets)
    summary = _summary(findings)
    return {
        "schema_version": 1,
        "budget_path": _display_path(budget_path),
        "status": _status(summary),
        "summary": summary,
        "budgets": budgets,
        "findings": findings,
        "debt_ledger": [finding for finding in findings if finding["status"] != "passed"],
    }


def load_quality_budgets(path: Path = DEFAULT_BUDGET_PATH) -> dict[str, Any]:
    """Load budget policy, falling back to strict defaults when the file is absent."""

    if not path.exists():
        return {"global": _default_global(), "layers": {}}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    global_budget = {**_default_global(), **(data.get("global") or {})}
    return {"global": global_budget, "layers": data.get("layers") or {}}


def _project_findings(payload: dict[str, Any], budgets: dict[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    global_budget = budgets.get("global") or _default_global()
    for project in payload.get("projects") or []:
        slug = project_slug(project)
        if not slug:
            continue
        findings.extend(_size_findings(project, global_budget))
        findings.extend(_clustering_findings(project, global_budget))
        findings.extend(_freshness_findings(project, global_budget))
    return findings


def _size_findings(project: dict[str, Any], budget: dict[str, Any]) -> list[dict[str, Any]]:
    max_sloc = as_int(get_value(project, "loc_without_tests", "max_sloc"))
    max_loc = as_int(get_value(project, "loc_without_tests", "max_lines"))
    blocking = project_slug(project) == "dpone"
    return [
        _finding(project, "max_sloc", max_sloc, budget.get("warn_sloc"), budget.get("max_sloc"), "lower", blocking),
        _finding(project, "max_loc", max_loc, budget.get("warn_loc"), budget.get("max_loc"), "lower", blocking),
    ]


def _clustering_findings(project: dict[str, Any], budget: dict[str, Any]) -> list[dict[str, Any]]:
    value = as_float(get_value(project, "coupling", "avg_clustering"))
    return [
        _finding(
            project,
            "avg_clustering",
            value,
            None,
            budget.get("max_avg_clustering"),
            "lower",
            project_slug(project) == "dpone",
        )
    ]


def _freshness_findings(project: dict[str, Any], budget: dict[str, Any]) -> list[dict[str, Any]]:
    max_stale = as_int(budget.get("max_stale_days"))
    findings = []
    for group_name, group in (project.get("metric_groups") or {}).items():
        age = group.get("stale_age_days")
        if age is not None:
            findings.append(
                _finding(
                    project,
                    f"{group_name}_stale_age_days",
                    as_int(age),
                    None,
                    max_stale,
                    "lower",
                    project_slug(project) == "dpone",
                )
            )
    return findings


def _finding(
    project: dict[str, Any],
    metric: str,
    value: int | float,
    warn: Any,
    fail: Any,
    direction: str,
    blocking: bool,
) -> dict[str, Any]:
    status = _finding_status(value, warn, fail, direction, blocking)
    return {
        "project_id": project_slug(project),
        "project_name": project_name(project),
        "metric": metric,
        "value": value,
        "warn_threshold": warn,
        "fail_threshold": fail,
        "direction": direction,
        "status": status,
        "scope": "release" if blocking else "comparator",
    }


def _finding_status(value: int | float, warn: Any, fail: Any, direction: str, blocking: bool) -> str:
    if _threshold_crossed(value, fail, direction):
        return "failed" if blocking else "warning"
    if _threshold_crossed(value, warn, direction):
        return "warning"
    return "passed"


def _threshold_crossed(value: int | float, threshold: Any, direction: str) -> bool:
    if threshold is None:
        return False
    limit = as_float(threshold)
    if direction == "lower":
        return float(value) > limit
    return float(value) < limit


def _summary(findings: list[dict[str, Any]]) -> dict[str, int]:
    summary = {"passed": 0, "warning": 0, "failed": 0}
    for finding in findings:
        status = str(finding.get("status") or "failed")
        summary[status] = summary.get(status, 0) + 1
    return summary


def _status(summary: dict[str, int]) -> str:
    if summary.get("failed", 0):
        return "failed"
    if summary.get("warning", 0):
        return "warning"
    return "passed"


def _default_global() -> dict[str, Any]:
    return {
        "max_sloc": 400,
        "warn_sloc": 350,
        "max_loc": 600,
        "warn_loc": 450,
        "max_avg_clustering": 0.180,
        "max_stale_days": 30,
    }


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()
