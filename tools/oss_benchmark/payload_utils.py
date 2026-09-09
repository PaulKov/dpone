"""Shared payload helpers for benchmark scoring and rendering."""

from __future__ import annotations

import re
from dataclasses import asdict
from typing import Any

from tools.oss_benchmark.models import ProjectMetrics


def find_project(projects: list[dict[str, Any]], slug: str) -> dict[str, Any]:
    for project in projects:
        if project_slug(project) == slug:
            return project
    return {}


def project_slug(project: dict[str, Any]) -> str:
    return str(get_value(project, "spec", "slug", default=""))


def project_name(project: dict[str, Any]) -> str:
    return str(get_value(project, "spec", "name", default="n/a"))


def project_commit(project: dict[str, Any]) -> str:
    branch = get_value(project, "spec", "branch", default="")
    commit = get_value(project, "spec", "commit", default="")
    if not branch and not commit:
        return "`n/a`"
    return f"`{branch}@{commit}`"


def dirty_label(project: dict[str, Any]) -> str:
    return " dirty" if project.get("dirty") else ""


def anchor(text: str) -> str:
    return re.sub(r"[^a-z0-9 -]", "", text.lower()).replace(" ", "-")


def get_value(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def format_int(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "n/a"


def format_float(value: Any, *, digits: int) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"


def format_score(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.1f}/5"
    except (TypeError, ValueError):
        return "n/a"


def format_delta(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if number == 0:
        return "0"
    return f"{number:+g}"


def format_hotspot(item: dict[str, Any]) -> str:
    value = item.get("value", "n/a")
    if isinstance(value, float):
        value = f"{value:.3f}"
    return f"{item.get('severity', 'risk')} {item.get('kind', 'hotspot')} `{item.get('label', 'n/a')}` ({value} {item.get('unit', '')})"


def format_gate_value(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value) if value is not None else "n/a"


def format_percent(value: Any) -> str:
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "n/a"


def test_loc(project: dict[str, Any]) -> int | None:
    return numeric_delta(
        get_value(project, "loc_with_tests", "total_lines"), get_value(project, "loc_without_tests", "total_lines")
    )


def test_sloc(project: dict[str, Any]) -> int | None:
    return numeric_delta(
        get_value(project, "loc_with_tests", "total_sloc"), get_value(project, "loc_without_tests", "total_sloc")
    )


def numeric_delta(total: Any, production: Any) -> int | None:
    try:
        return max(0, int(total) - int(production))
    except (TypeError, ValueError):
        return None


def test_footprint(project: dict[str, Any]) -> str:
    project_test_sloc = test_sloc(project)
    production_sloc = get_value(project, "loc_without_tests", "total_sloc")
    try:
        denominator = int(production_sloc)
    except (TypeError, ValueError):
        return "n/a"
    if project_test_sloc is None or denominator <= 0:
        return "n/a"
    return f"{(project_test_sloc / denominator) * 100:.1f}%"


def as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def metric_to_renderable(metric: ProjectMetrics) -> dict[str, Any]:
    payload = asdict(metric)
    payload["spec"]["path"] = str(metric.spec.path)
    return payload
