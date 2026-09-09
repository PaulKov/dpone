"""State merging and freshness metadata for OSS benchmark evidence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

METRIC_GROUPS = (
    "loc_sloc",
    "top_modules",
    "coupling_cohesion",
    "solid_clean_oop",
    "visuals",
)

PROJECT_ORDER = ("dpone", "airbyte", "dlt", "pentaho-kettle", "apache-hop", "sling")

PROJECT_STUBS: dict[str, dict[str, str]] = {
    "dpone": {"name": "dpone", "slug": "dpone", "kind": "local-framework"},
    "airbyte": {"name": "Airbyte", "slug": "airbyte", "kind": "oss-core"},
    "dlt": {"name": "dlt", "slug": "dlt", "kind": "oss-core"},
    "pentaho-kettle": {"name": "Pentaho Kettle", "slug": "pentaho-kettle", "kind": "oss-core"},
    "apache-hop": {"name": "Apache Hop", "slug": "apache-hop", "kind": "oss-core"},
    "sling": {"name": "Sling", "slug": "sling", "kind": "oss-core"},
}


@dataclass(frozen=True)
class RunContext:
    """CI/local invocation metadata rendered into the public benchmark header."""

    generated_at: str
    updated_by: str
    workflow_name: str
    run_id: str
    run_url: str
    git_sha: str
    branch: str


def run_context_to_jsonable(context: RunContext) -> dict[str, str]:
    return asdict(context)


def fresh_metric_groups(updated_at: str) -> dict[str, dict[str, str | int | None]]:
    return {
        group: {
            "status": "fresh",
            "last_updated_at": updated_at,
            "refresh_attempted_at": updated_at,
            "last_error": None,
            "stale_age_days": 0,
        }
        for group in METRIC_GROUPS
    }


def merge_benchmark_payload(
    refreshed_payload: dict[str, Any],
    *,
    previous_payload: dict[str, Any] | None,
    requested_slugs: set[str],
    attempted_at: str,
    failures: dict[str, str],
    allow_stale: bool,
) -> dict[str, Any]:
    """Merge fresh project evidence with previous evidence after partial failures.

    A failed metric refresh must never erase the previous published value. When
    previous evidence exists, the whole project metric bundle is retained and
    each group is marked stale. If no previous evidence exists, an unavailable
    placeholder is emitted so the document can be honest without inventing
    numbers.
    """

    previous_projects = _projects_by_slug(previous_payload)
    refreshed_projects = _projects_by_slug(refreshed_payload)
    final_projects: dict[str, dict[str, Any]] = {}
    previous_generated_at = str((previous_payload or {}).get("generated_at") or attempted_at)

    for slug, project in previous_projects.items():
        if slug not in requested_slugs:
            final_projects[slug] = _with_missing_freshness(project, previous_generated_at)

    for slug, project in refreshed_projects.items():
        final_projects[slug] = _with_missing_freshness(project, attempted_at)

    for slug, error in failures.items():
        previous_project = previous_projects.get(slug)
        if previous_project and allow_stale:
            final_projects[slug] = _mark_project_stale(
                _with_missing_freshness(previous_project, previous_generated_at),
                attempted_at=attempted_at,
                error=error,
            )
        else:
            final_projects[slug] = _unavailable_project(slug, attempted_at=attempted_at, error=error)

    ordered_projects = sorted(final_projects.values(), key=lambda item: _project_sort_key(_project_slug(item)))
    merged = dict(refreshed_payload)
    merged["projects"] = ordered_projects
    merged["freshness_summary"] = freshness_summary(ordered_projects)
    merged["score_deltas"] = score_deltas(ordered_projects, previous_projects)
    return merged


def freshness_summary(projects: list[dict[str, Any]]) -> dict[str, int]:
    summary = {"fresh": 0, "stale": 0, "unavailable": 0}
    for project in projects:
        status = project_freshness_status(project)
        summary[status] = summary.get(status, 0) + 1
    return summary


def project_freshness_status(project: dict[str, Any]) -> str:
    statuses = {group.get("status", "fresh") for group in project.get("metric_groups", {}).values()}
    if "unavailable" in statuses:
        return "unavailable"
    if "stale" in statuses:
        return "stale"
    return "fresh"


def project_freshness_label(project: dict[str, Any]) -> str:
    status = project_freshness_status(project)
    groups = project.get("metric_groups", {})
    updated_values = [group.get("last_updated_at") for group in groups.values() if group.get("last_updated_at")]
    attempted_values = [
        group.get("refresh_attempted_at") for group in groups.values() if group.get("refresh_attempted_at")
    ]
    last_updated = min(updated_values) if updated_values else "never"
    attempted = max(attempted_values) if attempted_values else "not attempted"
    if status == "fresh":
        return f"fresh; updated {last_updated}"
    if status == "stale":
        return f"stale; last updated {last_updated}; attempted {attempted}"
    return f"unavailable; attempted {attempted}"


def score_deltas(
    projects: list[dict[str, Any]],
    previous_projects: dict[str, dict[str, Any]],
) -> dict[str, dict[str, float]]:
    deltas: dict[str, dict[str, float]] = {}
    for project in projects:
        slug = _project_slug(project)
        previous = previous_projects.get(slug)
        if not previous or project_freshness_status(project) != "fresh":
            continue
        current_quality = project.get("quality", {})
        previous_quality = previous.get("quality", {})
        solid_delta = _as_float(current_quality.get("solid")) - _as_float(previous_quality.get("solid"))
        clean_delta = _as_float(current_quality.get("clean_oop")) - _as_float(previous_quality.get("clean_oop"))
        if solid_delta or clean_delta:
            deltas[slug] = {"solid": round(solid_delta, 2), "clean_oop": round(clean_delta, 2)}
    return deltas


def _projects_by_slug(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not payload:
        return {}
    projects: dict[str, dict[str, Any]] = {}
    for project in payload.get("projects", []):
        slug = _project_slug(project)
        if slug:
            projects[slug] = project
    return projects


def _with_missing_freshness(project: dict[str, Any], attempted_at: str) -> dict[str, Any]:
    updated = dict(project)
    existing = updated.get("metric_groups") or {}
    groups = fresh_metric_groups(attempted_at)
    groups.update(existing)
    updated["metric_groups"] = groups
    updated.pop("unavailable", None)
    return updated


def _mark_project_stale(project: dict[str, Any], *, attempted_at: str, error: str) -> dict[str, Any]:
    stale_project = dict(project)
    groups = stale_project.get("metric_groups") or fresh_metric_groups(
        str(stale_project.get("generated_at") or attempted_at)
    )
    stale_project["metric_groups"] = {
        group: _mark_group_stale(groups.get(group, {}), attempted_at=attempted_at, error=error)
        for group in METRIC_GROUPS
    }
    stale_project.pop("unavailable", None)
    return stale_project


def _mark_group_stale(group: dict[str, Any], *, attempted_at: str, error: str) -> dict[str, Any]:
    previous_last_updated = group.get("last_updated_at") or group.get("refresh_attempted_at")
    return {
        "status": "stale",
        "last_updated_at": previous_last_updated,
        "refresh_attempted_at": attempted_at,
        "last_error": error,
        "stale_age_days": _stale_age_days(previous_last_updated, attempted_at),
    }


def _unavailable_project(slug: str, *, attempted_at: str, error: str) -> dict[str, Any]:
    spec = PROJECT_STUBS.get(slug, {"name": slug, "slug": slug, "kind": "unknown"})
    return {
        "spec": spec,
        "unavailable": True,
        "metric_groups": {
            group: {
                "status": "unavailable",
                "last_updated_at": None,
                "refresh_attempted_at": attempted_at,
                "last_error": error,
                "stale_age_days": None,
            }
            for group in METRIC_GROUPS
        },
    }


def _project_slug(project: dict[str, Any]) -> str:
    return str(project.get("spec", {}).get("slug", ""))


def _project_sort_key(slug: str) -> tuple[int, str]:
    try:
        return (PROJECT_ORDER.index(slug), slug)
    except ValueError:
        return (len(PROJECT_ORDER), slug)


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _stale_age_days(last_updated_at: Any, attempted_at: str) -> int | None:
    if not last_updated_at:
        return None
    try:
        last = datetime.fromisoformat(str(last_updated_at).replace("Z", "+00:00"))
        attempted = datetime.fromisoformat(str(attempted_at).replace("Z", "+00:00"))
    except ValueError:
        return None
    return max(0, int((attempted - last).total_seconds() // 86_400))
