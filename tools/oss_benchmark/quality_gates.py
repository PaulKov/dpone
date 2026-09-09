"""Quality gate evaluation for the OSS benchmark governance layer."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.contexts import release_context_is_resolved
from tools.oss_benchmark.state import project_freshness_status

DEFAULT_QUALITY_GATE_THRESHOLDS: dict[str, int | float | str] = {
    "industrial_maintainability_min": 85,
    "architecture_risk_max": 24,
    "coverage_confidence_min": 75,
    "max_module_loc_max": 600,
    "max_module_sloc_max": 400,
    "max_fan_out_max": 30,
    "cohesion_min": 0.55,
    "avg_clustering_max": 0.180,
    "freshness": "fresh",
}


def evaluate_quality_gates(
    payload: dict[str, Any],
    *,
    project_slug: str = "dpone",
    thresholds: dict[str, int | float | str] | None = None,
) -> dict[str, Any]:
    """Evaluate governance thresholds against the merged benchmark payload."""

    active_thresholds = {**DEFAULT_QUALITY_GATE_THRESHOLDS, **(thresholds or {})}
    project = _find_project(list(payload.get("projects", [])), project_slug)
    if not project:
        return _gate_result(project_slug, [_availability_check("missing", project_slug)])

    checks = [
        _quality_gate_check(
            "industrial_maintainability",
            "Industrial Maintainability Index",
            _get(project, "industrial_maintainability", "score"),
            ">=",
            active_thresholds["industrial_maintainability_min"],
        ),
        _quality_gate_check(
            "architecture_risk",
            "Architecture risk score",
            _get(project, "architecture_risk", "score"),
            "<=",
            active_thresholds["architecture_risk_max"],
        ),
        _quality_gate_check(
            "coverage_confidence",
            "Coverage confidence score",
            _get(project, "coverage_confidence", "score"),
            ">=",
            active_thresholds["coverage_confidence_min"],
        ),
        _quality_gate_check(
            "max_module_loc",
            "Largest production module LOC",
            _get(project, "loc_without_tests", "max_lines"),
            "<=",
            active_thresholds["max_module_loc_max"],
        ),
        _quality_gate_check(
            "max_fan_out",
            "Maximum fan-out",
            _get(project, "coupling", "max_ce"),
            "<=",
            active_thresholds["max_fan_out_max"],
        ),
        _quality_gate_check(
            "cohesion",
            "Cohesion ratio",
            _get(project, "coupling", "cohesion_ratio"),
            ">=",
            active_thresholds["cohesion_min"],
        ),
        _quality_gate_check(
            "freshness",
            "Metric freshness",
            project_freshness_status(project),
            "==",
            active_thresholds["freshness"],
        ),
    ]
    if _uses_v2_gates(payload, project):
        checks = [
            _availability_check("unavailable" if project.get("unavailable") else "available", "available"),
            *checks[:4],
            _quality_gate_check(
                "max_module_sloc",
                "Largest production module SLOC",
                _get(project, "loc_without_tests", "max_sloc"),
                "<=",
                active_thresholds["max_module_sloc_max"],
            ),
            *checks[4:],
            _quality_gate_check(
                "avg_clustering",
                "Average clustering",
                _get(project, "coupling", "avg_clustering"),
                "<=",
                active_thresholds["avg_clustering_max"],
            ),
            _quality_gate_check(
                "release_context",
                "Release context resolved",
                "resolved" if release_context_is_resolved(payload) else "missing",
                "==",
                "resolved",
            ),
        ]
    if payload.get("quality_budgets"):
        checks.append(
            _quality_gate_check(
                "quality_budget",
                "Quality budget hard failures",
                _budget_gate_status(payload.get("quality_budgets") or {}),
                "==",
                "passed",
            )
        )
    if payload.get("runtime_certification_v2"):
        checks.append(
            _quality_gate_check(
                "executable_certification",
                "Executable certification",
                ((payload.get("runtime_certification_v2") or {}).get("gates") or {}).get("status"),
                "==",
                "passed",
            )
        )
    return _gate_result(project_slug, checks)


def _gate_result(project_slug: str, checks: list[dict[str, Any]]) -> dict[str, Any]:
    failed_checks = [check for check in checks if check["status"] == "failed"]
    return {
        "project": project_slug,
        "status": "failed" if failed_checks else "passed",
        "passed": len(checks) - len(failed_checks),
        "failed": len(failed_checks),
        "checks": checks,
        "failed_checks": failed_checks,
    }


def _quality_gate_check(
    check_id: str,
    label: str,
    actual: Any,
    operator: str,
    threshold: int | float | str,
) -> dict[str, Any]:
    return {
        "id": check_id,
        "label": label,
        "actual": actual,
        "operator": operator,
        "threshold": threshold,
        "status": "passed" if _compare_gate_value(actual, operator, threshold) else "failed",
        "severity": "blocker",
    }


def _availability_check(actual: str, threshold: str) -> dict[str, Any]:
    return _quality_gate_check(
        "project_available",
        "Project evidence available",
        actual,
        "==",
        threshold,
    )


def _uses_v2_gates(payload: dict[str, Any], project: dict[str, Any]) -> bool:
    return (
        int(payload.get("schema_version", 1) or 1) >= 2
        or "release_context" in payload
        or bool(project.get("project_id"))
        or bool(project.get("unavailable"))
        or _get(project, "loc_without_tests", "max_sloc") is not None
        or _get(project, "coupling", "avg_clustering") is not None
    )


def _budget_gate_status(budgets: dict[str, Any]) -> str:
    return "failed" if budgets.get("status") == "failed" else "passed"


def _compare_gate_value(actual: Any, operator: str, threshold: int | float | str) -> bool:
    if operator == "==":
        return str(actual) == str(threshold)
    try:
        actual_number = float(actual)
        threshold_number = float(threshold)
    except (TypeError, ValueError):
        return False
    if operator == ">=":
        return actual_number >= threshold_number
    if operator == "<=":
        return actual_number <= threshold_number
    raise ValueError(f"Unsupported quality gate operator: {operator}")


def _find_project(projects: list[dict[str, Any]], slug: str) -> dict[str, Any]:
    for project in projects:
        if _get(project, "spec", "slug") == slug:
            return project
    return {}


def _get(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current
