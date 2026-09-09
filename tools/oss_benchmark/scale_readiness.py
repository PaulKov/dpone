"""Scale readiness and growth simulation for the OSS benchmark."""

from __future__ import annotations

import math
from typing import Any

from tools.oss_benchmark.payload_utils import as_float, as_int, project_name, project_slug

_SCENARIO_SLUGS = {
    "dlt": ("dlt-scale", "dlt scale"),
    "airbyte": ("airbyte-scale", "Airbyte scale"),
    "pentaho-kettle": ("pentaho-scale", "Pentaho Kettle scale"),
    "apache-hop": ("hop-scale", "Apache Hop scale"),
}

_BUDGETS = {
    "max_module_yellow": 600.0,
    "max_module_red": 900.0,
    "p90_ce_yellow": 8.0,
    "p90_ce_red": 12.0,
    "semantic_yellow": 75.0,
    "semantic_red": 65.0,
    "normalized_yellow": 75.0,
    "normalized_red": 65.0,
}

_CONNECTOR_SLOC = 3_200


def build_scale_readiness(payload: dict[str, Any]) -> dict[str, Any]:
    """Build dpone scale-readiness projections from current evidence."""

    projects = [project for project in payload.get("projects", []) if not project.get("unavailable")]
    targets = _scenario_targets(projects)
    summary: dict[str, dict[str, Any]] = {}
    headroom: dict[str, dict[str, Any]] = {}
    runway: dict[str, dict[str, Any]] = {}
    scenarios_by_project: dict[str, list[dict[str, Any]]] = {}
    warnings: list[dict[str, Any]] = []

    for project in _reference_projects(projects):
        slug = project_slug(project)
        project_headroom = _quality_headroom(project, payload)
        project_scenarios = [_scale_scenario(project, target, payload) for target in targets]
        project_runway = _architecture_runway(project, payload, project_headroom, project_scenarios)
        project_warnings = _warnings(slug, project_runway, project_headroom, project_scenarios)
        headroom[slug] = project_headroom
        runway[slug] = project_runway
        scenarios_by_project[slug] = project_scenarios
        summary[slug] = {
            "name": project_name(project),
            "architecture_runway_score": project_runway["score"],
            "overall_status": project_runway["status"],
            "growth_ceiling_sloc": project_runway["growth_ceiling_sloc"],
            "quality_headroom": {
                "connector_slots_before_yellow": project_headroom["connector_slots_before_yellow"],
                "connector_slots_before_red": project_headroom["connector_slots_before_red"],
            },
            "worst_scenario": _worst_scenario_id(project_scenarios),
        }
        warnings.extend(project_warnings)

    return {
        "schema_version": 1,
        "methodology": {
            "purpose": "Model whether dpone has enough architectural runway to grow toward comparator repository scale.",
            "budgets": _BUDGETS,
            "connector_sloc_assumption": _CONNECTOR_SLOC,
            "note": "The simulation is a static planning proxy, not a product roadmap or runtime performance forecast.",
        },
        "summary": summary,
        "quality_headroom": headroom,
        "architecture_runway": runway,
        "scale_scenarios": scenarios_by_project,
        "warnings": warnings,
    }


def _reference_projects(projects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    local = [project for project in projects if project_slug(project) == "dpone"]
    if local:
        return local
    return projects[:1]


def _scenario_targets(projects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    by_slug = {project_slug(project): project for project in projects}
    for slug, (scenario_id, label) in _SCENARIO_SLUGS.items():
        project = by_slug.get(slug)
        if project:
            targets.append({"scenario_id": scenario_id, "label": label, "project": project})
    return targets


def _quality_headroom(project: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    max_module = as_float(_get(project, "loc_without_tests", "max_lines"))
    p90_ce = as_float(_get(project, "coupling", "p90_ce"))
    semantic = _semantic_score(project, payload)
    normalized = _normalized_score(project, payload)
    sloc = as_int(_get(project, "loc_without_tests", "total_sloc"))
    yellow_slots = min(
        _slot_room(_BUDGETS["max_module_yellow"] - max_module, 18),
        _slot_room((_BUDGETS["p90_ce_yellow"] - p90_ce) * 2_000, 700),
        _score_slots(semantic, _BUDGETS["semantic_yellow"]),
        _score_slots(normalized, _BUDGETS["normalized_yellow"]),
    )
    red_slots = min(
        _slot_room(_BUDGETS["max_module_red"] - max_module, 18),
        _slot_room((_BUDGETS["p90_ce_red"] - p90_ce) * 2_000, 700),
        _score_slots(semantic, _BUDGETS["semantic_red"]),
        _score_slots(normalized, _BUDGETS["normalized_red"]),
    )
    return {
        "connector_slots_before_yellow": max(0, yellow_slots),
        "connector_slots_before_red": max(0, red_slots),
        "max_module_loc_headroom": round(_BUDGETS["max_module_yellow"] - max_module, 1),
        "p90_fan_out_headroom": round(_BUDGETS["p90_ce_yellow"] - p90_ce, 2),
        "semantic_score_headroom": round(semantic - _BUDGETS["semantic_yellow"], 1),
        "normalized_score_headroom": round(normalized - _BUDGETS["normalized_yellow"], 1),
        "current_sloc": sloc,
        "connector_sloc_assumption": _CONNECTOR_SLOC,
    }


def _scale_scenario(project: dict[str, Any], target: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    target_project = target["project"]
    current_sloc = max(1, as_int(_get(project, "loc_without_tests", "total_sloc")))
    target_sloc = max(current_sloc, as_int(_get(target_project, "loc_without_tests", "total_sloc")))
    multiplier = max(1.0, target_sloc / current_sloc)
    max_module = as_float(_get(project, "loc_without_tests", "max_lines"))
    p90_ce = as_float(_get(project, "coupling", "p90_ce"))
    base_score = _normalized_score(project, payload)
    projected_module = int(round(max_module * (1.0 + (math.sqrt(multiplier) - 1.0) * 0.55)))
    projected_p90 = round(p90_ce + math.log2(multiplier) * 1.15, 2)
    pressure = max(0.0, projected_module - _BUDGETS["max_module_yellow"]) / 55.0
    pressure += max(0.0, projected_p90 - _BUDGETS["p90_ce_yellow"]) * 3.0
    projected_score = _clamp_score(base_score - pressure - max(0.0, multiplier - 1.0) * 1.25)
    return {
        "id": target["scenario_id"],
        "label": target["label"],
        "growth_multiplier": round(multiplier, 2),
        "projected_sloc": target_sloc,
        "projected_max_module_loc": projected_module,
        "projected_p90_fan_out": projected_p90,
        "projected_maintainability": projected_score,
        "risk_level": _risk_level(projected_score, projected_module, projected_p90),
    }


def _architecture_runway(
    project: dict[str, Any],
    payload: dict[str, Any],
    headroom: dict[str, Any],
    scenarios: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized = _normalized_score(project, payload)
    yellow_slots = as_int(headroom.get("connector_slots_before_yellow"))
    worst_score = min([as_int(scenario.get("projected_maintainability")) for scenario in scenarios] or [normalized])
    score = _clamp_score((normalized * 0.42) + (min(30, yellow_slots) / 30 * 28) + (worst_score * 0.30))
    status = "ready" if score >= 80 else "watch" if score >= 65 else "constrained"
    ceiling = _growth_ceiling_sloc(project, headroom)
    constraints = {
        "max module LOC": as_float(headroom.get("max_module_loc_headroom")),
        "p90 fan-out": as_float(headroom.get("p90_fan_out_headroom")),
        "semantic score": as_float(headroom.get("semantic_score_headroom")),
        "normalized score": as_float(headroom.get("normalized_score_headroom")),
    }
    return {
        "score": score,
        "status": status,
        "growth_ceiling_sloc": ceiling,
        "primary_constraint": min(constraints, key=lambda key: constraints[key]),
        "constraints": constraints,
    }


def _warnings(
    slug: str,
    runway: dict[str, Any],
    headroom: dict[str, Any],
    scenarios: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    if as_int(runway.get("score")) < 70:
        warnings.append(_warning(slug, "architecture_runway_low", "Architecture runway score", runway["score"], 70))
    if as_int(headroom.get("connector_slots_before_yellow")) < 5:
        warnings.append(
            _warning(
                slug,
                "connector_headroom_low",
                "Connector slots before yellow",
                headroom["connector_slots_before_yellow"],
                5,
            )
        )
    airbyte = next((scenario for scenario in scenarios if scenario.get("id") == "airbyte-scale"), {})
    if airbyte and as_int(airbyte.get("projected_maintainability")) < 70:
        warnings.append(
            _warning(
                slug,
                "airbyte_scale_projection_low",
                "Airbyte-scale projected maintainability",
                airbyte["projected_maintainability"],
                70,
            )
        )
    return warnings


def _warning(project: str, warning_id: str, label: str, current: Any, threshold: Any) -> dict[str, Any]:
    return {
        "id": warning_id,
        "project": project,
        "label": label,
        "severity": "warning",
        "message": f"{label} is below the scale-readiness warning threshold.",
        "current": current,
        "threshold": threshold,
    }


def _growth_ceiling_sloc(project: dict[str, Any], headroom: dict[str, Any]) -> int:
    current_sloc = as_int(_get(project, "loc_without_tests", "total_sloc"))
    red_slots = as_int(headroom.get("connector_slots_before_red"))
    return current_sloc + red_slots * _CONNECTOR_SLOC


def _semantic_score(project: dict[str, Any], payload: dict[str, Any]) -> float:
    slug = project_slug(project)
    return as_float(_get(payload, "semantic_maintainability", "summary", slug, "overall_score")) or as_float(
        _get(project, "industrial_maintainability", "score")
    )


def _normalized_score(project: dict[str, Any], payload: dict[str, Any]) -> float:
    slug = project_slug(project)
    return as_float(_get(payload, "scoring_calibration", "summary", slug, "normalized_score")) or as_float(
        _get(project, "industrial_maintainability", "score")
    )


def _slot_room(headroom: float, per_slot: float) -> int:
    if per_slot <= 0:
        return 0
    return int(math.floor(headroom / per_slot))


def _score_slots(score: float, threshold: float) -> int:
    return int(math.floor((score - threshold) / 0.8))


def _worst_scenario_id(scenarios: list[dict[str, Any]]) -> str:
    if not scenarios:
        return "n/a"
    return str(min(scenarios, key=lambda item: as_int(item.get("projected_maintainability"))).get("id", "n/a"))


def _risk_level(score: int, module_loc: int, p90_ce: float) -> str:
    if score >= 78 and module_loc <= _BUDGETS["max_module_yellow"] and p90_ce <= _BUDGETS["p90_ce_yellow"]:
        return "low"
    if score >= 68 and module_loc <= _BUDGETS["max_module_red"] and p90_ce <= _BUDGETS["p90_ce_red"]:
        return "moderate"
    return "high"


def _clamp_score(value: float) -> int:
    return int(round(max(0.0, min(100.0, value))))


def _get(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current
