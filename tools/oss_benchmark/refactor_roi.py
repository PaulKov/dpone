"""Refactor ROI roadmap and quality-economics scoring."""

from __future__ import annotations

import re
from typing import Any

from tools.oss_benchmark.payload_utils import as_int, get_value, project_name, project_slug

_QUADRANTS = (
    "high-impact / low-effort",
    "high-impact / high-effort",
    "low-impact / low-effort",
    "defer",
)


def build_refactor_roi_roadmap(payload: dict[str, Any]) -> dict[str, Any]:
    """Rank benchmark remediation items by impact, effort and debt reduction."""

    summary = _project_debt_summary(payload)
    items = _ranked_items(payload, summary)
    quadrants = {name: [] for name in _QUADRANTS}
    for rank, item in enumerate(items, start=1):
        item["rank"] = rank
        quadrants.setdefault(item["quadrant"], []).append(rank)
    return {
        "schema_version": 1,
        "methodology": (
            "ROI is a deterministic quality-economics proxy: impact, effort feasibility, "
            "debt points and dpone actionability. It ranks refactoring candidates; it is not a delivery estimate."
        ),
        "summary": summary,
        "items": items[:20],
        "quadrants": quadrants,
    }


def _project_debt_summary(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    complexity = (payload.get("complexity_boundary") or {}).get("summary") or {}
    projects = {project_slug(project): project for project in payload.get("projects", [])}
    summary: dict[str, dict[str, Any]] = {}
    for slug, item in complexity.items():
        architecture_risk = as_int(get_value(projects.get(slug, {}), "architecture_risk", "score"))
        debt_points = max(0, 100 - as_int(item.get("overall_score")))
        debt_points += as_int(item.get("god_unit_count")) * 4
        debt_points += as_int(item.get("boundary_violation_count")) * 8
        debt_points += min(30, architecture_risk // 2)
        debt_points = min(999, debt_points)
        driver = _top_debt_driver(item, architecture_risk)
        summary[slug] = {
            "project": slug,
            "name": item.get("name") or project_name(projects.get(slug, {})) or slug,
            "debt_points": debt_points,
            "status": "active-debt" if debt_points >= 60 else "watch" if debt_points >= 30 else "controlled",
            "top_debt_driver": driver,
            "quick_win_count": 0,
            "strategic_refactor_count": 0,
        }
    return summary


def _ranked_items(payload: dict[str, Any], summary: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = _candidate_risks(payload)
    items = [_score_candidate(candidate, summary) for candidate in candidates]
    items = _deduplicate_items(items)
    items.sort(key=lambda item: (-as_int(item["roi_score"]), _priority_rank(item.get("priority")), item["module"]))
    _update_project_counts(items, summary)
    return items


def _candidate_risks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    risks: list[dict[str, Any]] = []
    complexity = payload.get("complexity_boundary") or {}
    risks.extend(_normalize_risk(item, source="complexity_boundary") for item in complexity.get("risk_register") or [])
    for slug, project in (complexity.get("summary") or {}).items():
        for item in project.get("risk_register") or []:
            normalized = _normalize_risk(item, source="complexity_boundary")
            normalized.setdefault("project", slug)
            risks.append(normalized)
    risks.extend(_architecture_hotspot_risks(payload))
    risks.extend(_remediation_risks(payload))
    return [item for item in risks if item.get("project") and item.get("module")]


def _normalize_risk(item: dict[str, Any], *, source: str) -> dict[str, Any]:
    return {
        "priority": str(item.get("priority") or "P2"),
        "project": str(item.get("project") or "dpone"),
        "module": str(item.get("module") or item.get("path") or ""),
        "reason": str(item.get("reason") or item.get("title") or item.get("message") or "Quality risk detected."),
        "recommendation": str(item.get("recommendation") or "Refactor behind a smaller contract."),
        "source": source,
    }


def _architecture_hotspot_risks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    risks: list[dict[str, Any]] = []
    for project in payload.get("projects", []):
        slug = project_slug(project)
        for hotspot in (project.get("architecture_risk") or {}).get("hotspots") or []:
            risks.append(
                {
                    "priority": "P1" if hotspot.get("severity") in {"critical", "high"} else "P2",
                    "project": slug,
                    "module": str(hotspot.get("label") or ""),
                    "reason": f"{hotspot.get('kind', 'architecture')} risk is {hotspot.get('value', 'n/a')} {hotspot.get('unit', '')}.",
                    "recommendation": "Reduce architecture pressure behind a narrower boundary.",
                    "source": "architecture_risk",
                }
            )
    return risks


def _remediation_risks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    risks: list[dict[str, Any]] = []
    for item in (payload.get("remediation_backlog") or {}).get("items") or []:
        module = str(item.get("module") or item.get("path") or item.get("title") or "")
        risks.append(
            {
                "priority": str(item.get("priority") or "P2"),
                "project": str(item.get("project") or "dpone"),
                "module": module,
                "reason": str(item.get("evidence") or item.get("title") or "Remediation backlog item."),
                "recommendation": str(item.get("recommendation") or "Resolve generated remediation backlog item."),
                "source": "remediation_backlog",
            }
        )
    return risks


def _score_candidate(candidate: dict[str, Any], summary: dict[str, dict[str, Any]]) -> dict[str, Any]:
    module = str(candidate["module"])
    reason = str(candidate["reason"])
    complexity = _extract_number(reason)
    debt = _candidate_debt_points(candidate, summary, complexity)
    impact = _impact_score(candidate, debt, complexity)
    effort = _effort_score(module, complexity)
    roi = int(round((impact * 0.52) + (effort * 0.28) + (min(100, debt * 2) * 0.20)))
    quadrant = _quadrant(impact, effort)
    return {
        **candidate,
        "impact_score": impact,
        "effort_score": effort,
        "debt_points": debt,
        "roi_score": min(100, roi),
        "quadrant": quadrant,
        "target_architecture": _target_architecture(module),
        "recommended_action": candidate.get("recommendation") or _default_action(module),
    }


def _candidate_debt_points(candidate: dict[str, Any], summary: dict[str, dict[str, Any]], complexity: int) -> int:
    project_debt = as_int((summary.get(str(candidate.get("project"))) or {}).get("debt_points"))
    debt = max(8, min(40, project_debt // 4))
    if complexity:
        debt += min(30, complexity // 3)
    if candidate.get("source") == "architecture_risk":
        debt += 8
    if "implementation import" in str(candidate.get("reason", "")).lower():
        debt += 10
    return min(100, debt)


def _impact_score(candidate: dict[str, Any], debt: int, complexity: int) -> int:
    priority_base = {"P0": 100, "P1": 88, "P2": 70, "P3": 52}.get(str(candidate.get("priority")), 60)
    actionability = 12 if candidate.get("project") == "dpone" else -12
    complexity_bonus = min(10, complexity // 8) if complexity else 0
    return max(0, min(100, priority_base + actionability + complexity_bonus + min(8, debt // 12)))


def _effort_score(module: str, complexity: int) -> int:
    if "commands/" in module or ".commands." in module:
        base = 82
    elif "cli_render" in module or "render" in module:
        base = 78
    elif "runtime/sinks" in module or ".runtime.sinks." in module:
        base = 68
    elif "manifest" in module or "dag" in module:
        base = 64
    else:
        base = 58
    return max(25, base - min(28, complexity // 5 if complexity else 0))


def _target_architecture(module: str) -> str:
    if "commands/" in module or ".commands." in module:
        return "Command -> Application service -> Renderer"
    if "runtime/sinks" in module or ".runtime.sinks." in module:
        return "Sink -> SinkProtocol -> StrategyFactory"
    if "cli_render" in module or "render" in module:
        return "Renderer -> View model -> Formatter"
    if "manifest" in module:
        return "Manifest service -> Rule object -> Report renderer"
    if "dag" in module:
        return "DAG query service -> DTO -> CLI renderer"
    return "Facade -> Service -> Port"


def _default_action(module: str) -> str:
    return f"Move `{module}` behavior behind the recommended target architecture."


def _top_debt_driver(item: dict[str, Any], architecture_risk: int) -> str:
    drivers = {
        "complexity": as_int(item.get("god_unit_count")) * 8 + max(0, as_int(item.get("max_complexity")) - 20),
        "boundary": as_int(item.get("boundary_violation_count")) * 12,
        "architecture": architecture_risk,
    }
    return max(drivers.items(), key=lambda pair: pair[1])[0]


def _quadrant(impact: int, effort: int) -> str:
    if impact >= 75 and effort >= 60:
        return "high-impact / low-effort"
    if impact >= 75:
        return "high-impact / high-effort"
    if effort >= 60:
        return "low-impact / low-effort"
    return "defer"


def _extract_number(text: str) -> int:
    numbers = [int(match) for match in re.findall(r"\b\d+\b", text)]
    return max(numbers, default=0)


def _deduplicate_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    unique: list[dict[str, Any]] = []
    for item in items:
        key = (str(item.get("project")), str(item.get("module")), str(item.get("source")))
        if key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


def _update_project_counts(items: list[dict[str, Any]], summary: dict[str, dict[str, Any]]) -> None:
    for item in items:
        project = summary.get(str(item.get("project")))
        if not project:
            continue
        if item["quadrant"] == "high-impact / low-effort":
            project["quick_win_count"] += 1
        elif item["quadrant"] == "high-impact / high-effort":
            project["strategic_refactor_count"] += 1


def _priority_rank(priority: Any) -> int:
    return {"P0": 0, "P1": 1, "P2": 2, "P3": 3}.get(str(priority), 9)
