"""Explainable benchmark scoring, regression detection and remediation backlog."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import as_float, as_int, find_project, get_value, project_name, project_slug
from tools.oss_benchmark.state import project_freshness_status

_COMPONENT_MAX_SCORES = {
    "quality": 35,
    "module_size": 20,
    "coupling": 15,
    "cohesion": 15,
    "test_footprint": 10,
    "freshness": 5,
}

_COMPONENT_LABELS = {
    "quality": "SOLID and Clean OOP",
    "module_size": "Module size",
    "coupling": "Coupling",
    "cohesion": "Cohesion",
    "test_footprint": "Test footprint",
    "freshness": "Metric freshness",
}

_PRIORITY_RANK = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}


def build_score_explanations(payload: dict[str, Any]) -> dict[str, Any]:
    """Build per-project scoring breakdowns from merged benchmark evidence."""

    projects: dict[str, Any] = {}
    for project in payload.get("projects", []):
        slug = project_slug(project)
        if not slug or project.get("unavailable"):
            continue
        index = project.get("industrial_maintainability") or {}
        components = index.get("components") or {}
        projects[slug] = {
            "project": project_name(project),
            "score": index.get("score"),
            "band": index.get("band"),
            "formula": "quality + module_size + coupling + cohesion + test_footprint + freshness",
            "components": [
                _component_explanation(project, component_id, score)
                for component_id, score in components.items()
                if component_id in _COMPONENT_MAX_SCORES
            ],
            "rubric": {
                "solid": _rubric_entry(project, "solid", "SOLID correspondence"),
                "clean_oop": _rubric_entry(project, "clean_oop", "Clean OOP correspondence"),
            },
        }
    return {
        "schema_version": 1,
        "methodology": "Scores are deterministic static maintainability proxies derived from the merged benchmark evidence.",
        "projects": projects,
    }


def build_regression_summary(
    payload: dict[str, Any],
    *,
    previous_payload: dict[str, Any] | None,
    project_slug_value: str = "dpone",
) -> dict[str, Any]:
    """Compare the current benchmark payload with previous evidence for governance review."""

    current = find_project(list(payload.get("projects", [])), project_slug_value)
    previous = find_project(list((previous_payload or {}).get("projects", [])), project_slug_value)
    if not current or not previous:
        return {
            "schema_version": 1,
            "project": project_slug_value,
            "status": "no_previous_evidence",
            "changes": [],
            "regressions": [],
        }

    changes = [
        _numeric_change(
            "industrial_maintainability",
            "Industrial Maintainability Index",
            get_value(previous, "industrial_maintainability", "score"),
            get_value(current, "industrial_maintainability", "score"),
            higher_is_better=True,
        ),
        _numeric_change(
            "max_module_loc",
            "Largest production module LOC",
            get_value(previous, "loc_without_tests", "max_lines"),
            get_value(current, "loc_without_tests", "max_lines"),
            higher_is_better=False,
        ),
        _numeric_change(
            "p90_fan_out",
            "P90 fan-out",
            get_value(previous, "coupling", "p90_ce"),
            get_value(current, "coupling", "p90_ce"),
            higher_is_better=False,
        ),
        _numeric_change(
            "cross_slice_ratio",
            "Cross-slice dependency ratio",
            get_value(previous, "coupling", "cross_slice_ratio"),
            get_value(current, "coupling", "cross_slice_ratio"),
            higher_is_better=False,
            tolerance=0.005,
        ),
        _numeric_change(
            "test_footprint",
            "Static test footprint",
            get_value(previous, "coverage_confidence", "test_footprint_ratio"),
            get_value(current, "coverage_confidence", "test_footprint_ratio"),
            higher_is_better=True,
            tolerance=0.005,
        ),
        _freshness_change(previous, current),
    ]
    regressions = [change for change in changes if change["status"] == "regressed"]
    improvements = [change for change in changes if change["status"] == "improved"]
    status = "regressed" if regressions else "improved" if improvements else "unchanged"
    return {
        "schema_version": 1,
        "project": project_slug_value,
        "status": status,
        "changes": changes,
        "regressions": regressions,
    }


def build_remediation_backlog(payload: dict[str, Any], *, max_items: int = 12) -> dict[str, Any]:
    """Build a deterministic quality-improvement backlog from benchmark evidence."""

    items: list[dict[str, Any]] = []
    _append_failed_quality_gates(items, payload)
    for project in payload.get("projects", []):
        _append_stale_metric_items(items, project)
        if project_slug(project) != "dpone" or project.get("unavailable"):
            continue
        _append_architecture_hotspot_items(items, project)
        _append_module_size_watch_item(items, project)
        _append_fan_out_watch_item(items, project)
    unique_items = _deduplicate_items(items)
    unique_items.sort(
        key=lambda item: (_PRIORITY_RANK.get(item["priority"], 9), 0 if item["project"] == "dpone" else 1)
    )
    return {
        "schema_version": 1,
        "methodology": "Backlog items are generated from quality gates, architecture hotspots, stale metrics and dpone watch thresholds.",
        "items": unique_items[:max_items],
    }


def _component_explanation(project: dict[str, Any], component_id: str, score: Any) -> dict[str, Any]:
    return {
        "id": component_id,
        "label": _COMPONENT_LABELS[component_id],
        "score": round(as_float(score), 1),
        "max_score": _COMPONENT_MAX_SCORES[component_id],
        "actual": _component_actual(project, component_id),
        "reason": _component_reason(project, component_id),
    }


def _component_actual(project: dict[str, Any], component_id: str) -> str:
    if component_id == "quality":
        return (
            f"SOLID {as_float(get_value(project, 'quality', 'solid')):.1f}/5, "
            f"Clean OOP {as_float(get_value(project, 'quality', 'clean_oop')):.1f}/5"
        )
    if component_id == "module_size":
        return f"{as_int(get_value(project, 'loc_without_tests', 'max_lines'))} LOC max module"
    if component_id == "coupling":
        return (
            f"avg Ce {as_float(get_value(project, 'coupling', 'avg_ce')):.2f}, "
            f"P90 Ce {as_float(get_value(project, 'coupling', 'p90_ce')):.0f}"
        )
    if component_id == "cohesion":
        return (
            f"cohesion {as_float(get_value(project, 'coupling', 'cohesion_ratio')):.3f}, "
            f"clustering {as_float(get_value(project, 'coupling', 'avg_clustering')):.3f}"
        )
    if component_id == "test_footprint":
        return f"{as_float(get_value(project, 'coverage_confidence', 'test_footprint_ratio')) * 100:.1f}%"
    if component_id == "freshness":
        return project_freshness_status(project)
    return "n/a"


def _component_reason(project: dict[str, Any], component_id: str) -> str:
    if component_id == "quality":
        evidence = get_value(project, "quality", "evidence", default=[]) or []
        return "; ".join(str(item) for item in evidence[:2]) or "quality rubric evidence is unavailable"
    if component_id == "module_size":
        return "Rewards production modules below the 600 LOC governance target."
    if component_id == "coupling":
        return "Rewards low average and P90 outgoing dependency pressure."
    if component_id == "cohesion":
        return "Rewards dependencies that stay inside architectural slices and avoid dense clustering."
    if component_id == "test_footprint":
        return "Rewards a meaningful static test footprint without treating it as runtime coverage."
    if component_id == "freshness":
        return "Rewards metric groups that refreshed successfully in the latest benchmark run."
    return "n/a"


def _rubric_entry(project: dict[str, Any], score_id: str, label: str) -> dict[str, Any]:
    return {
        "label": label,
        "score": as_float(get_value(project, "quality", score_id)),
        "scale": "0-5",
        "evidence": list((get_value(project, "quality", "evidence", default=[]) or [])[:4]),
    }


def _numeric_change(
    change_id: str,
    label: str,
    previous: Any,
    current: Any,
    *,
    higher_is_better: bool,
    tolerance: float = 0.0,
) -> dict[str, Any]:
    previous_value = as_float(previous)
    current_value = as_float(current)
    delta = round(current_value - previous_value, 4)
    if abs(delta) <= tolerance:
        status = "unchanged"
    elif (delta > 0 and higher_is_better) or (delta < 0 and not higher_is_better):
        status = "improved"
    else:
        status = "regressed"
    return {
        "id": change_id,
        "label": label,
        "previous": previous_value,
        "current": current_value,
        "delta": delta,
        "direction": "higher_is_better" if higher_is_better else "lower_is_better",
        "status": status,
    }


def _freshness_change(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    order = {"fresh": 2, "stale": 1, "unavailable": 0}
    previous_status = project_freshness_status(previous)
    current_status = project_freshness_status(current)
    delta = order.get(current_status, 0) - order.get(previous_status, 0)
    status = "unchanged" if delta == 0 else "improved" if delta > 0 else "regressed"
    return {
        "id": "freshness",
        "label": "Metric freshness",
        "previous": previous_status,
        "current": current_status,
        "delta": delta,
        "direction": "fresh_is_better",
        "status": status,
    }


def _append_failed_quality_gates(items: list[dict[str, Any]], payload: dict[str, Any]) -> None:
    gates = payload.get("quality_gates") or {}
    for check in gates.get("failed_checks") or []:
        items.append(
            {
                "priority": "P0",
                "project": gates.get("project", "dpone"),
                "category": "quality_gate",
                "title": f"Fix failing quality gate: {check.get('label', 'unknown gate')}",
                "evidence": f"actual {check.get('actual')} {check.get('operator')} target {check.get('threshold')}",
                "recommendation": "Treat this as a release-governance blocker before publishing a benchmark refresh PR.",
            }
        )


def _append_stale_metric_items(items: list[dict[str, Any]], project: dict[str, Any]) -> None:
    slug = project_slug(project)
    for group_name, freshness in (project.get("metric_groups") or {}).items():
        status = freshness.get("status")
        if status not in {"stale", "unavailable"}:
            continue
        priority = "P1" if status == "unavailable" else "P2"
        items.append(
            {
                "priority": priority,
                "project": slug,
                "category": "freshness",
                "title": f"Refresh {group_name} metrics for {project_name(project)}",
                "evidence": f"{status}; last updated {freshness.get('last_updated_at') or 'never'}",
                "recommendation": "Rerun the manual benchmark workflow for this project and inspect collector logs.",
            }
        )


def _append_architecture_hotspot_items(items: list[dict[str, Any]], project: dict[str, Any]) -> None:
    for hotspot in (project.get("architecture_risk") or {}).get("hotspots") or []:
        severity = str(hotspot.get("severity") or "medium")
        priority = "P1" if severity in {"critical", "high"} else "P2"
        items.append(
            {
                "priority": priority,
                "project": project_slug(project),
                "category": str(hotspot.get("kind") or "architecture_hotspot"),
                "title": f"Reduce {hotspot.get('kind', 'architecture')} pressure in {hotspot.get('label', 'module')}",
                "evidence": f"{hotspot.get('value')} {hotspot.get('unit', '')}; severity {severity}",
                "recommendation": "Split responsibilities behind narrow contracts and keep public facades as compatibility shims.",
            }
        )


def _append_module_size_watch_item(items: list[dict[str, Any]], project: dict[str, Any]) -> None:
    max_lines = as_int(get_value(project, "loc_without_tests", "max_lines"))
    if max_lines < 400:
        return
    top_module = (get_value(project, "top_loc_without_tests", default=[]) or [{}])[0]
    module_path = str(top_module.get("path") or "largest production module")
    items.append(
        {
            "priority": "P2" if max_lines >= 500 else "P3",
            "project": project_slug(project),
            "category": "module_size_watch",
            "title": f"Keep {module_path} below the 600 LOC hard gate",
            "evidence": f"{max_lines} LOC",
            "recommendation": "Move the next new responsibility into a focused collaborator before this module crosses 600 LOC.",
        }
    )


def _append_fan_out_watch_item(items: list[dict[str, Any]], project: dict[str, Any]) -> None:
    max_ce = as_int(get_value(project, "coupling", "max_ce"))
    if max_ce < 15:
        return
    module_name = str(get_value(project, "coupling", "max_ce_module", default="max fan-out module"))
    items.append(
        {
            "priority": "P2",
            "project": project_slug(project),
            "category": "fan_out_watch",
            "title": f"Lower fan-out in {module_name}",
            "evidence": f"Ce {max_ce}",
            "recommendation": "Introduce narrower ports or policy objects so this module depends on contracts rather than concrete peers.",
        }
    )


def _deduplicate_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    unique: list[dict[str, Any]] = []
    for item in items:
        key = (str(item.get("project")), str(item.get("category")), str(item.get("title")))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique
