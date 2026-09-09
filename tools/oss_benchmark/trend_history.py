"""Historical trend and architecture-delta helpers for OSS benchmark evidence."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import as_float, find_project, get_value, project_name, project_slug
from tools.oss_benchmark.state import project_freshness_status


def update_history_payload(
    payload: dict[str, Any],
    *,
    previous_history: dict[str, Any] | None,
    max_entries: int = 30,
) -> dict[str, Any]:
    """Append the current benchmark snapshot to durable trend history."""

    entries = list((previous_history or {}).get("entries", []))
    current_entry = build_history_entry(payload)
    entries = [entry for entry in entries if entry.get("generated_at") != current_entry["generated_at"]]
    entries.append(current_entry)
    entries = entries[-max_entries:]
    return {
        "schema_version": 2,
        "latest_deltas": latest_history_deltas(entries),
        "entries": entries,
    }


def build_history_entry(payload: dict[str, Any]) -> dict[str, Any]:
    """Build one compact history entry from the merged benchmark payload."""

    run_context = payload.get("run_context", {})
    projects: dict[str, dict[str, Any]] = {}
    for project in payload.get("projects", []):
        slug = project_slug(project)
        if not slug or project.get("unavailable"):
            continue
        index = project.get("industrial_maintainability") or {}
        quality = project.get("quality") or {}
        projects[slug] = {
            "name": project_name(project),
            "score": index.get("score"),
            "band": index.get("band"),
            "solid": quality.get("solid"),
            "clean_oop": quality.get("clean_oop"),
            "freshness": project_freshness_status(project),
            "max_ce": get_value(project, "coupling", "max_ce"),
            "max_ce_module": get_value(project, "coupling", "max_ce_module"),
            "avg_ce": get_value(project, "coupling", "avg_ce"),
            "p90_ce": get_value(project, "coupling", "p90_ce"),
            "largest_module_loc": get_value(project, "loc_without_tests", "max_lines"),
            "test_footprint_ratio": get_value(project, "coverage_confidence", "test_footprint_ratio"),
        }
    return {
        "generated_at": run_context.get("generated_at") or payload.get("generated_at"),
        "updated_by": run_context.get("updated_by") or "local",
        "run_url": run_context.get("run_url") or "",
        "projects": projects,
    }


def latest_history_deltas(entries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Return project-level deltas from the two latest history entries."""

    if len(entries) < 2:
        return {}
    previous = entries[-2].get("projects", {})
    current = entries[-1].get("projects", {})
    deltas: dict[str, dict[str, Any]] = {}
    for slug, project in current.items():
        previous_project = previous.get(slug)
        if not previous_project:
            continue
        project_deltas: dict[str, Any] = {
            "score": _number_delta(project.get("score"), previous_project.get("score")),
            "solid": _number_delta(project.get("solid"), previous_project.get("solid")),
            "clean_oop": _number_delta(project.get("clean_oop"), previous_project.get("clean_oop")),
            "previous_band": previous_project.get("band"),
            "current_band": project.get("band"),
            "previous_max_ce_module": previous_project.get("max_ce_module"),
            "current_max_ce_module": project.get("max_ce_module"),
        }
        for key in ("max_ce", "avg_ce", "p90_ce", "largest_module_loc", "test_footprint_ratio"):
            delta = _number_delta_if_present(project.get(key), previous_project.get(key))
            if delta is not None:
                project_deltas[key] = delta
        deltas[slug] = project_deltas
    return deltas


def build_architecture_delta(
    payload: dict[str, Any],
    *,
    previous_payload: dict[str, Any] | None,
    project_slug_value: str = "dpone",
) -> dict[str, Any]:
    """Compare current architecture-governance metrics with previous evidence."""

    current = find_project(list(payload.get("projects", [])), project_slug_value)
    previous = find_project(list((previous_payload or {}).get("projects", [])), project_slug_value)
    if not current or not previous:
        return {
            "schema_version": 1,
            "project": project_slug_value,
            "status": "no_previous_evidence",
            "items": [],
        }
    items = [
        _metric_delta(
            "max_ce",
            "Max fan-out",
            get_value(previous, "coupling", "max_ce"),
            get_value(current, "coupling", "max_ce"),
            lower_is_better=True,
            previous_label=get_value(previous, "coupling", "max_ce_module"),
            current_label=get_value(current, "coupling", "max_ce_module"),
        ),
        _metric_delta(
            "p90_ce",
            "P90 fan-out",
            get_value(previous, "coupling", "p90_ce"),
            get_value(current, "coupling", "p90_ce"),
            lower_is_better=True,
        ),
        _metric_delta(
            "avg_ce",
            "Average fan-out",
            get_value(previous, "coupling", "avg_ce"),
            get_value(current, "coupling", "avg_ce"),
            lower_is_better=True,
        ),
        _metric_delta(
            "largest_module_loc",
            "Largest module LOC",
            get_value(previous, "loc_without_tests", "max_lines"),
            get_value(current, "loc_without_tests", "max_lines"),
            lower_is_better=True,
        ),
        _metric_delta(
            "test_footprint_ratio",
            "Static test footprint",
            get_value(previous, "coverage_confidence", "test_footprint_ratio"),
            get_value(current, "coverage_confidence", "test_footprint_ratio"),
            lower_is_better=False,
        ),
    ]
    statuses = {item["status"] for item in items}
    status = "regressed" if "regressed" in statuses else "improved" if "improved" in statuses else "unchanged"
    return {
        "schema_version": 1,
        "project": project_slug_value,
        "status": status,
        "items": items,
    }


def _metric_delta(
    metric_id: str,
    label: str,
    previous: Any,
    current: Any,
    *,
    lower_is_better: bool,
    previous_label: Any = None,
    current_label: Any = None,
) -> dict[str, Any]:
    previous_value = as_float(previous)
    current_value = as_float(current)
    delta = round(current_value - previous_value, 4)
    if delta == 0:
        status = "unchanged"
    elif (delta < 0 and lower_is_better) or (delta > 0 and not lower_is_better):
        status = "improved"
    else:
        status = "regressed"
    return {
        "id": metric_id,
        "label": label,
        "previous": previous_value,
        "current": current_value,
        "delta": delta,
        "direction": "lower_is_better" if lower_is_better else "higher_is_better",
        "status": status,
        "previous_label": previous_label,
        "current_label": current_label,
    }


def _number_delta(current: Any, previous: Any) -> int | float:
    current_value = as_float(current)
    previous_value = as_float(previous)
    delta = round(current_value - previous_value, 4)
    if delta.is_integer():
        return int(delta)
    return delta


def _number_delta_if_present(current: Any, previous: Any) -> int | float | None:
    if current is None or previous is None:
        return None
    return _number_delta(current, previous)


__all__ = [
    "build_architecture_delta",
    "build_history_entry",
    "latest_history_deltas",
    "update_history_payload",
]
