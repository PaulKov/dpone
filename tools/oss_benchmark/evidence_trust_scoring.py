"""Confidence scoring for benchmark evidence trust."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import get_value, project_name, project_slug


def confidence_for_project(project: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Score how audit-ready one project's static evidence is."""

    groups = project.get("metric_groups") or {}
    fresh, stale, unavailable = _status_counts(groups)
    total = max(1, fresh + stale + unavailable)
    score = 52 + int(round((fresh / total) * 38))
    score -= stale * 7
    score -= unavailable * 16
    if get_value(project, "spec", "kind") == "local-framework":
        score += 5
    if get_value(project, "loc_without_tests", "total_sloc") is not None:
        score += 3
    if get_value(project, "quality", "solid") is not None:
        score += 2
    if payload and (payload.get("quality_gates") or {}).get("status") == "passed" and project_slug(project) == "dpone":
        score += 2
    score = max(0, min(100, score))
    return {
        "name": project_name(project),
        "confidence_score": score,
        "band": confidence_band(score),
        "fresh_groups": fresh,
        "stale_groups": stale,
        "unavailable_groups": unavailable,
        "freshness": _project_status(fresh, stale, unavailable),
    }


def confidence_band(score: int) -> str:
    if score >= 90:
        return "audit-ready"
    if score >= 75:
        return "high"
    if score >= 55:
        return "medium"
    return "low"


def _status_counts(groups: dict[str, Any]) -> tuple[int, int, int]:
    fresh = stale = unavailable = 0
    for group in groups.values():
        status = str((group or {}).get("status") or "fresh")
        if status == "stale":
            stale += 1
        elif status == "unavailable":
            unavailable += 1
        else:
            fresh += 1
    return fresh, stale, unavailable


def _project_status(fresh: int, stale: int, unavailable: int) -> str:
    if unavailable:
        return "unavailable"
    if stale:
        return "stale"
    if fresh:
        return "fresh"
    return "unavailable"
