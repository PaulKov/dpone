"""Semantic maintainability deep scan for OSS benchmark evidence."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tools.oss_benchmark.collectors import is_test_file
from tools.oss_benchmark.config import IGNORED_DIRS, SOURCE_SUFFIXES
from tools.oss_benchmark.payload_utils import as_float, as_int, get_value, project_name, project_slug
from tools.oss_benchmark.semantic_hotspots import (
    GOD_CLASS_LOC,
    GOD_FUNCTION_LOC,
    HIGH_BRANCH_COUNT,
    analyze_semantic_file,
    measure_generic_semantics,
    measure_python_semantics,
    responsibility_tags,
)

__all__ = [
    "analyze_semantic_file",
    "analyze_semantic_maintainability",
    "measure_generic_semantics",
    "measure_python_semantics",
    "project_semantic_maintainability",
]

_GOD_MODULE_LOC = 600
_SEMANTIC_IGNORED_DIRS = IGNORED_DIRS | {"docs", "doc", "examples", "example", "samples", "tutorials"}


def analyze_semantic_maintainability(projects: list[dict[str, Any]]) -> dict[str, Any]:
    """Build project-level semantic maintainability evidence."""

    summary: dict[str, Any] = {}
    risks: list[dict[str, Any]] = []
    for project in projects:
        slug = project_slug(project)
        if not slug or project.get("unavailable"):
            continue
        item = project_semantic_maintainability(project)
        summary[slug] = item
        risks.extend(item.get("risk_register", []))
    risks.sort(key=lambda item: (_priority_rank(item.get("priority")), item.get("project", ""), item.get("module", "")))
    return {
        "schema_version": 1,
        "methodology": (
            "Static semantic maintainability proxies for god modules/classes/functions, "
            "SOLID/DI contracts, DRY/KISS responsibility spread and direct implementation coupling."
        ),
        "thresholds": {
            "god_module_loc": _GOD_MODULE_LOC,
            "god_class_loc": GOD_CLASS_LOC,
            "god_function_loc": GOD_FUNCTION_LOC,
            "high_branch_count": HIGH_BRANCH_COUNT,
        },
        "summary": summary,
        "risk_register": risks[:20],
    }


def project_semantic_maintainability(project: dict[str, Any]) -> dict[str, Any]:
    """Analyze one project using source files plus existing benchmark payloads."""

    root = _project_root(project)
    scans = [analyze_semantic_file(path, root) for path in _iter_source_files(root)] if root and root.exists() else []
    module_objects = _god_modules(project)
    file_objects = [item for scan in scans for item in scan.get("top_god_objects", [])]
    top_objects = sorted(module_objects + file_objects, key=lambda item: (-as_int(item.get("value")), item["module"]))[
        :15
    ]
    direct_impls = [item for scan in scans for item in scan.get("direct_implementation_imports", [])]
    source_count = max(1, len(scans) or as_int(get_value(project, "loc_without_tests", "files")))
    interface_density = sum(as_int(scan.get("interface_hint_count")) for scan in scans) / source_count
    constructor_signal = sum(as_int(scan.get("constructor_dependency_count")) for scan in scans)
    responsibility_spread = _responsibility_spread(project, scans)
    god_module_count = len(module_objects)
    god_class_count = sum(1 for item in file_objects if item.get("kind") == "class")
    god_function_count = sum(1 for item in file_objects if item.get("kind") == "function")
    branch_hotspots = sum(1 for scan in scans if as_int(scan.get("branch_count")) >= HIGH_BRANCH_COUNT)
    boundary_score = _boundary_score(len(direct_impls), project)
    scores = {
        "god_object_score": _god_object_score(god_module_count, god_class_count, god_function_count, source_count),
        "solid_di_score": _solid_di_score(project, interface_density, constructor_signal, len(direct_impls)),
        "dry_kiss_score": _dry_kiss_score(responsibility_spread, branch_hotspots, god_function_count, source_count),
        "boundary_score": boundary_score,
    }
    overall = round(
        scores["god_object_score"] * 0.30
        + scores["solid_di_score"] * 0.30
        + scores["dry_kiss_score"] * 0.20
        + scores["boundary_score"] * 0.20
    )
    return {
        "name": project_name(project),
        "status": _score_status(overall),
        "overall_score": int(overall),
        **scores,
        "god_module_count": god_module_count,
        "god_class_count": god_class_count,
        "god_function_count": god_function_count,
        "interface_density": round(interface_density, 4),
        "constructor_dependency_count": constructor_signal,
        "direct_implementation_imports": len(direct_impls),
        "responsibility_spread": responsibility_spread,
        "branch_hotspot_count": branch_hotspots,
        "top_god_objects": top_objects,
        "solid_di_findings": _solid_di_findings(interface_density, constructor_signal, direct_impls),
        "dry_kiss_findings": _dry_kiss_findings(responsibility_spread, branch_hotspots, god_function_count),
        "boundary_findings": direct_impls[:10],
        "risk_register": _risk_register(project, top_objects, direct_impls, branch_hotspots, scores),
    }


def _iter_source_files(root: Path | None) -> tuple[Path, ...]:
    if root is None or not root.exists():
        return ()
    files: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in SOURCE_SUFFIXES:
            continue
        rel = path.relative_to(root)
        if is_test_file(rel) or _skip_path(rel):
            continue
        files.append(path)
    return tuple(sorted(files, key=lambda item: item.relative_to(root).as_posix()))


def _skip_path(path: Path) -> bool:
    parts = tuple(part.lower() for part in path.parts)
    return bool(set(parts) & _SEMANTIC_IGNORED_DIRS)


def _god_modules(project: dict[str, Any]) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    for item in get_value(project, "top_loc_without_tests", default=[]) or []:
        module_path = str(item.get("path") or "n/a")
        if _skip_path(Path(module_path)):
            continue
        lines = as_int(item.get("lines"))
        if lines >= _GOD_MODULE_LOC:
            objects.append(
                {
                    "kind": "module",
                    "module": module_path,
                    "name": "module",
                    "value": lines,
                    "unit": "LOC",
                }
            )
    return objects


def _responsibility_spread(project: dict[str, Any], scans: list[dict[str, Any]]) -> int:
    tags = {tag for scan in scans for tag in scan.get("responsibility_tags", [])}
    for item in get_value(project, "top_loc_without_tests", default=[]) or []:
        tags.update(responsibility_tags(str(item.get("path") or "")))
    return len(tags)


def _god_object_score(modules: int, classes: int, functions: int, source_count: int) -> int:
    object_pressure = (classes + functions) / max(1, source_count)
    penalty = modules * 4 + object_pressure * 80.0
    return int(round(max(0.0, 100.0 - min(90.0, penalty))))


def _solid_di_score(project: dict[str, Any], density: float, constructor_signal: int, direct_impls: int) -> int:
    quality = (
        as_float(get_value(project, "quality", "solid")) + as_float(get_value(project, "quality", "clean_oop"))
    ) / 2
    density_score = min(40.0, (density / 0.06) * 40.0)
    constructor_score = min(20.0, constructor_signal * 4.0)
    quality_score = min(40.0, quality * 8.0)
    return int(round(max(0.0, density_score + constructor_score + quality_score - direct_impls * 5.0)))


def _dry_kiss_score(spread: int, branch_hotspots: int, god_functions: int, source_count: int) -> int:
    branch_pressure = branch_hotspots / max(1, source_count)
    function_pressure = god_functions / max(1, source_count)
    penalty = max(0, spread - 6) * 3 + branch_pressure * 120 + function_pressure * 80
    return int(round(max(0.0, 100.0 - min(85.0, penalty))))


def _boundary_score(direct_impls: int, project: dict[str, Any]) -> int:
    cross_slice = as_float(get_value(project, "coupling", "cross_slice_ratio"))
    penalty = direct_impls * 5 + max(0.0, cross_slice - 0.35) * 80.0
    return int(round(max(0.0, 100.0 - penalty)))


def _solid_di_findings(
    density: float, constructor_signal: int, direct_impls: list[dict[str, Any]]
) -> list[dict[str, str]]:
    findings = [
        {
            "principle": "DI",
            "message": f"Constructor dependency signals detected: {constructor_signal}.",
        },
        {
            "principle": "SOLID",
            "message": f"Interface/protocol density is {density:.3f}.",
        },
    ]
    if direct_impls:
        findings.append(
            {"principle": "SOLID/DI", "message": "Direct implementation imports require port/factory review."}
        )
    return findings


def _dry_kiss_findings(spread: int, branch_hotspots: int, god_functions: int) -> list[dict[str, str]]:
    return [
        {"principle": "DRY/KISS", "message": f"Responsibility spread covers {spread} architectural tags."},
        {"principle": "KISS", "message": f"Branch hotspots: {branch_hotspots}; god functions: {god_functions}."},
    ]


def _risk_register(
    project: dict[str, Any],
    top_objects: list[dict[str, Any]],
    direct_impls: list[dict[str, Any]],
    branch_hotspots: int,
    scores: dict[str, int],
) -> list[dict[str, Any]]:
    slug = project_slug(project)
    risks: list[dict[str, Any]] = []
    for item in top_objects[:8]:
        risks.append(
            {
                "priority": "P1" if as_int(item.get("value")) >= 2 * GOD_FUNCTION_LOC else "P2",
                "project": slug,
                "module": item.get("module", "n/a"),
                "reason": f"{item.get('kind', 'object')} `{item.get('name', 'n/a')}` is {item.get('value')} {item.get('unit', 'LOC')}.",
                "recommendation": "Split responsibility behind a smaller service, strategy, renderer, or port.",
            }
        )
    for finding in direct_impls[:8]:
        risks.append(
            {
                "priority": "P2",
                "project": slug,
                "module": finding.get("path", "n/a"),
                "reason": finding.get("message", "Direct implementation import detected."),
                "recommendation": "Route dependency through a thin interface, factory, or DI composition root.",
            }
        )
    if branch_hotspots and scores.get("dry_kiss_score", 100) < 85:
        risks.append(
            {
                "priority": "P2",
                "project": slug,
                "module": "semantic branch hotspots",
                "reason": f"{branch_hotspots} files exceed branch-count comfort thresholds.",
                "recommendation": "Extract branching into strategies or declarative rule tables.",
            }
        )
    return risks


def _project_root(project: dict[str, Any]) -> Path | None:
    raw = get_value(project, "spec", "path")
    return Path(str(raw)) if raw else None


def _score_status(score: int) -> str:
    if score >= 90:
        return "excellent"
    if score >= 75:
        return "strong"
    if score >= 55:
        return "watch"
    return "risk"


def _priority_rank(priority: Any) -> int:
    return {"P0": 0, "P1": 1, "P2": 2, "P3": 3}.get(str(priority), 4)
