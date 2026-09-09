"""Release-to-baseline benchmark delta analysis."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import as_float, get_value
from tools.oss_benchmark.schema import project_id
from tools.oss_benchmark.state import project_freshness_status

MetricSpec = tuple[str, tuple[str, ...], str, float]

_METRICS: tuple[MetricSpec, ...] = (
    ("industrial_maintainability", ("industrial_maintainability", "score"), "higher", 2.0),
    ("solid", ("quality", "solid"), "higher", 0.01),
    ("clean_oop", ("quality", "clean_oop"), "higher", 0.01),
    ("cohesion", ("coupling", "cohesion_ratio"), "higher", 0.001),
    ("evidence_confidence", ("evidence_trust", "overall_confidence"), "higher", 0.01),
    ("max_module_sloc", ("loc_without_tests", "max_sloc"), "lower", 0.0),
    ("max_module_loc", ("loc_without_tests", "max_lines"), "lower", 0.0),
    ("max_fan_out", ("coupling", "max_ce"), "lower", 0.0),
    ("avg_clustering", ("coupling", "avg_clustering"), "lower", 0.001),
    ("architecture_risk", ("architecture_risk", "score"), "lower", 0.0),
)


def build_release_delta(
    payload: dict[str, Any],
    *,
    baseline_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    """Compare current evidence with a baseline payload using directional metrics."""

    baseline_projects = _projects_by_id(baseline_payload)
    projects: dict[str, Any] = {}
    blocking: list[dict[str, Any]] = []
    for project in payload.get("projects", []):
        pid = project_id(project)
        if not pid:
            continue
        baseline = baseline_projects.get(pid)
        item = _project_delta(project, baseline)
        projects[pid] = item
        if pid == "dpone":
            blocking.extend(_dpone_blockers(project, item, payload))
    return {
        "schema_version": 1,
        "projects": projects,
        "blocking_regressions": blocking,
        "status": "failed" if blocking else "passed",
    }


def _project_delta(project: dict[str, Any], baseline: dict[str, Any] | None) -> dict[str, Any]:
    if baseline is None:
        return {"status": "missing_baseline", "metrics": {}}
    if project_freshness_status(project) != "fresh" or project_freshness_status(baseline) != "fresh":
        return {
            "status": "not_comparable_stale",
            "metrics": {
                spec[0]: {"classification": "not_comparable_stale", "current": None, "baseline": None}
                for spec in _METRICS
            },
        }
    metrics = {
        name: _metric_delta(project, baseline, path, direction, tolerance)
        for name, path, direction, tolerance in _METRICS
    }
    return {"status": "compared", "metrics": metrics}


def _metric_delta(
    project: dict[str, Any],
    baseline: dict[str, Any],
    path: tuple[str, ...],
    direction: str,
    tolerance: float,
) -> dict[str, Any]:
    current = as_float(get_value(project, *path))
    previous = as_float(get_value(baseline, *path))
    delta = current - previous
    classification = _classify(delta, direction, tolerance)
    return {
        "current": current,
        "baseline": previous,
        "delta": round(delta, 4),
        "direction": direction,
        "classification": classification,
    }


def _classify(delta: float, direction: str, tolerance: float) -> str:
    if abs(delta) <= tolerance:
        return "unchanged"
    better = delta > 0 if direction == "higher" else delta < 0
    return "improved" if better else "regressed"


def _dpone_blockers(project: dict[str, Any], item: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    metrics = item.get("metrics", {})
    if as_float(get_value(project, "loc_without_tests", "max_sloc")) > 400:
        blockers.append(_blocker("max_module_sloc", "dpone max production SLOC exceeds 400."))
    if as_float(get_value(project, "coupling", "avg_clustering")) > 0.180:
        blockers.append(_blocker("avg_clustering", "dpone average clustering exceeds 0.180."))
    if metrics.get("industrial_maintainability", {}).get("delta", 0) < -2:
        blockers.append(_blocker("industrial_maintainability", "Industrial Maintainability dropped by more than 2."))
    if not payload.get("release_context"):
        blockers.append(_blocker("release_context", "Release context is missing."))
    if project_freshness_status(project) == "unavailable":
        blockers.append(_blocker("dpone_available", "dpone evidence is unavailable."))
    return blockers


def _blocker(check_id: str, message: str) -> dict[str, str]:
    return {"id": check_id, "message": message, "severity": "blocker"}


def _projects_by_id(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not payload:
        return {}
    return {project_id(project): project for project in payload.get("projects", []) if project_id(project)}
