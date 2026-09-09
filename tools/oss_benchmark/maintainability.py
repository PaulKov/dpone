"""Maintainability enrichment for benchmark project payloads."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tools.oss_benchmark.collectors import read_text
from tools.oss_benchmark.payload_utils import (
    as_float,
    as_int,
    format_float,
    format_int,
    format_percent,
    get_value,
    test_footprint,
    test_sloc,
)
from tools.oss_benchmark.stable_contracts import stable_contract_for
from tools.oss_benchmark.state import project_freshness_label, project_freshness_status


def enrich_payload_with_maintainability(payload: dict[str, Any]) -> dict[str, Any]:
    """Recompute maintainability overlays after stale/fresh payload merging."""

    updated = dict(payload)
    updated["projects"] = [
        project
        if project.get("unavailable")
        else {
            **project,
            "coverage_confidence": compute_coverage_confidence(project),
            "architecture_risk": compute_architecture_risk(project),
            "industrial_maintainability": compute_industrial_maintainability_index(project),
        }
        for project in updated.get("projects", [])
    ]
    return updated


def detect_ci_evidence(path: Path) -> dict[str, Any]:
    """Detect lightweight CI and coverage signals in a repository."""

    signals: list[str] = []
    workflow_dir = path / ".github" / "workflows"
    workflow_files = sorted(workflow_dir.glob("*.yml")) + sorted(workflow_dir.glob("*.yaml"))
    for workflow in workflow_files[:12]:
        text = read_text(workflow)
        lowered = text.lower()
        if "pytest" in lowered:
            signals.append("pytest")
        if "gradle" in lowered or "mvn test" in lowered or "maven" in lowered:
            signals.append("jvm tests")
        if "npm test" in lowered or "pnpm test" in lowered or "yarn test" in lowered:
            signals.append("js tests")
        if "coverage" in lowered or "--cov" in lowered or "jacoco" in lowered:
            signals.append("coverage workflow")

    for name in ("pyproject.toml", "pytest.ini", "tox.ini", "package.json", "build.gradle", "pom.xml"):
        candidate = path / name
        if not candidate.exists():
            continue
        text = read_text(candidate).lower()
        if "coverage" in text or "pytest-cov" in text or "jacoco" in text or "--cov" in text:
            signals.append(f"{name} coverage")
        elif "pytest" in text or "test" in text:
            signals.append(f"{name} tests")

    unique = tuple(dict.fromkeys(signals))
    return {
        "has_ci": bool(workflow_files),
        "has_coverage_config": any("coverage" in signal or "cov" in signal or "jacoco" in signal for signal in unique),
        "workflow_files": tuple(file.relative_to(path).as_posix() for file in workflow_files[:12]),
        "signals": unique,
    }


def compute_coverage_confidence(project: dict[str, Any]) -> dict[str, Any]:
    """Score test-footprint and CI evidence confidence for one project."""

    ci = project.get("ci_evidence") or {}
    production_files = as_int(get_value(project, "loc_without_tests", "files"))
    total_files = as_int(get_value(project, "loc_with_tests", "files"))
    test_files = max(0, total_files - production_files)
    test_file_ratio = (test_files / production_files) if production_files else 0.0
    footprint_sloc = test_sloc(project) or 0
    production_sloc = as_int(get_value(project, "loc_without_tests", "total_sloc"))
    footprint = (footprint_sloc / production_sloc) if production_sloc else 0.0
    components = {
        "test_footprint": min(35.0, 35.0 * (footprint / 0.35)) if footprint < 0.35 else 35.0,
        "test_file_ratio": min(20.0, 20.0 * (test_file_ratio / 0.25)) if test_file_ratio < 0.25 else 20.0,
        "ci_evidence": 25.0 if ci.get("has_ci") else 0.0,
        "coverage_config": 20.0 if ci.get("has_coverage_config") else 0.0,
    }
    score = int(round(sum(components.values())))
    return {
        "score": score,
        "confidence": "high" if score >= 75 else "medium" if score >= 45 else "low",
        "runtime_coverage": "configured"
        if ci.get("has_coverage_config")
        else "ci-only"
        if ci.get("has_ci")
        else "not detected",
        "test_footprint_ratio": round(footprint, 4),
        "test_file_ratio": round(test_file_ratio, 4),
        "components": {key: round(value, 1) for key, value in components.items()},
        "evidence": (
            f"test footprint {format_percent(footprint)}",
            f"{test_files:,} test files over {production_files:,} production files",
            "CI workflow evidence found" if ci.get("has_ci") else "no CI workflow evidence detected",
            "coverage signal found" if ci.get("has_coverage_config") else "no runtime coverage signal detected",
        ),
    }


def compute_architecture_risk(project: dict[str, Any]) -> dict[str, Any]:
    """Build hotspot risk evidence from module size and import coupling."""

    hotspots: list[dict[str, Any]] = []
    top_module = (get_value(project, "top_loc_without_tests", default=[]) or [{}])[0]
    _append_hotspot(
        hotspots,
        kind="module_size",
        label=str(top_module.get("path") or "largest production module"),
        value=as_int(get_value(project, "loc_without_tests", "max_lines")),
        severity_points=_risk_points(
            as_int(get_value(project, "loc_without_tests", "max_lines")), medium=600, high=1200, critical=2000
        ),
        unit="LOC",
    )
    _append_hotspot(
        hotspots,
        kind="fan_out",
        label=str(
            get_value(project, "coupling", "max_ce_module", default="max fan-out module") or "max fan-out module"
        ),
        value=as_int(get_value(project, "coupling", "max_ce")),
        severity_points=_risk_points(
            as_int(get_value(project, "coupling", "max_ce")), medium=20, high=60, critical=120
        ),
        unit="Ce",
    )
    fan_in_label, max_ca = _unapproved_fan_in_candidate(project)
    _append_hotspot(
        hotspots,
        kind="fan_in",
        label=fan_in_label,
        value=max_ca,
        severity_points=_risk_points(max_ca, medium=40, high=90, critical=160),
        unit="Ca",
    )
    cohesion = as_float(get_value(project, "coupling", "cohesion_ratio"))
    _append_hotspot(
        hotspots,
        kind="cohesion",
        label="cross-slice dependency pressure",
        value=round(cohesion, 3),
        severity_points=_inverse_risk_points(cohesion, medium=0.45, high=0.35, critical=0.25),
        unit="ratio",
    )
    clustering = as_float(get_value(project, "coupling", "avg_clustering"))
    _append_hotspot(
        hotspots,
        kind="clustering",
        label="dependency triangle pressure",
        value=round(clustering, 3),
        severity_points=_risk_points(clustering, medium=0.18, high=0.25, critical=0.35),
        unit="coefficient",
    )
    score = min(100, sum(item["points"] for item in hotspots))
    level = "low" if score < 25 else "medium" if score < 50 else "high" if score < 75 else "critical"
    return {
        "score": score,
        "level": level,
        "hotspots": hotspots,
        "approved_fan_in_contracts": _approved_contracts(project),
    }


def compute_industrial_maintainability_index(project: dict[str, Any]) -> dict[str, Any]:
    """Compute the 0-100 executive maintainability score."""

    components = {
        "quality": _component_quality(project),
        "module_size": _component_module_size(project),
        "coupling": _component_coupling(project),
        "cohesion": _component_cohesion(project),
        "test_footprint": _component_test_footprint(project),
        "freshness": _component_freshness(project),
    }
    score = round(sum(components.values()))
    band = "excellent" if score >= 85 else "strong" if score >= 70 else "watch" if score >= 50 else "risk"
    return {
        "score": int(max(0, min(100, score))),
        "band": band,
        "components": {name: round(value, 1) for name, value in components.items()},
        "evidence": (
            f"SOLID/Clean OOP component {components['quality']:.1f}/35",
            f"largest production module {format_int(get_value(project, 'loc_without_tests', 'max_lines'))} LOC",
            f"avg Ce {format_float(get_value(project, 'coupling', 'avg_ce'), digits=2)}, P90 Ce {format_float(get_value(project, 'coupling', 'p90_ce'), digits=0)}",
            f"cohesion {format_float(get_value(project, 'coupling', 'cohesion_ratio'), digits=3)}, clustering {format_float(get_value(project, 'coupling', 'avg_clustering'), digits=3)}",
            f"test footprint {test_footprint(project)}",
            project_freshness_label(project),
        ),
    }


def _approved_contracts(project: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        contract.to_jsonable(fan_in=fan_in)
        for module, fan_in in _fan_in_candidates(project)
        if (contract := stable_contract_for(module))
    ]


def _unapproved_fan_in_candidate(project: dict[str, Any]) -> tuple[str, int]:
    for module, fan_in in _fan_in_candidates(project):
        if stable_contract_for(module) is None:
            return module, fan_in
    return str(get_value(project, "coupling", "max_ca_module", default="max fan-in module")), 0


def _fan_in_candidates(project: dict[str, Any]) -> list[tuple[str, int]]:
    raw_items = get_value(project, "coupling", "top_in", default=[]) or []
    candidates: list[tuple[str, int]] = []
    for item in raw_items:
        if isinstance(item, dict):
            module = str(item.get("module") or item.get("name") or "")
            fan_in = as_int(item.get("fan_in") or item.get("value") or item.get("count"))
        else:
            try:
                module = str(item[0])
                fan_in = as_int(item[1])
            except (IndexError, TypeError):
                continue
        if module:
            candidates.append((module, fan_in))
    return candidates or [
        (
            str(get_value(project, "coupling", "max_ca_module", default="max fan-in module")),
            as_int(get_value(project, "coupling", "max_ca")),
        )
    ]


def _append_hotspot(
    hotspots: list[dict[str, Any]], *, kind: str, label: str, value: int | float, severity_points: int, unit: str
) -> None:
    if severity_points <= 0:
        return
    severity = "critical" if severity_points >= 25 else "high" if severity_points >= 15 else "medium"
    hotspots.append(
        {"kind": kind, "label": label, "value": value, "unit": unit, "severity": severity, "points": severity_points}
    )


def _risk_points(value: int | float, *, medium: float, high: float, critical: float) -> int:
    if value >= critical:
        return 25
    if value >= high:
        return 15
    if value >= medium:
        return 8
    return 0


def _inverse_risk_points(value: int | float, *, medium: float, high: float, critical: float) -> int:
    if value <= critical:
        return 25
    if value <= high:
        return 15
    if value <= medium:
        return 8
    return 0


def _component_quality(project: dict[str, Any]) -> float:
    return (
        (as_float(get_value(project, "quality", "solid")) + as_float(get_value(project, "quality", "clean_oop"))) / 10.0
    ) * 35.0


def _component_module_size(project: dict[str, Any]) -> float:
    max_sloc = as_int(get_value(project, "loc_without_tests", "max_sloc"))
    if max_sloc <= 400:
        return 20.0
    if max_sloc >= 1200:
        return 0.0
    return 20.0 * (1.0 - ((max_sloc - 400) / 800.0))


def _component_coupling(project: dict[str, Any]) -> float:
    avg_score = _descending_score(as_float(get_value(project, "coupling", "avg_ce")), green=6.0, red=16.0)
    p90_score = _descending_score(as_float(get_value(project, "coupling", "p90_ce")), green=12.0, red=30.0)
    return ((avg_score * 0.55) + (p90_score * 0.45)) * 15.0


def _component_cohesion(project: dict[str, Any]) -> float:
    cohesion_score = _ascending_score(as_float(get_value(project, "coupling", "cohesion_ratio")), red=0.25, green=0.60)
    clustering_score = _descending_score(
        as_float(get_value(project, "coupling", "avg_clustering")), green=0.18, red=0.35
    )
    return ((cohesion_score * 0.6) + (clustering_score * 0.4)) * 15.0


def _component_test_footprint(project: dict[str, Any]) -> float:
    footprint_sloc = test_sloc(project)
    production_sloc = as_int(get_value(project, "loc_without_tests", "total_sloc"))
    if footprint_sloc is None or production_sloc <= 0:
        return 0.0
    footprint = footprint_sloc / production_sloc
    if 0.35 <= footprint <= 1.25:
        return 10.0
    return 10.0 * (footprint / 0.35) if footprint < 0.35 else max(7.0, 10.0 - min(3.0, (footprint - 1.25) * 5.0))


def _component_freshness(project: dict[str, Any]) -> float:
    return (
        5.0
        if project_freshness_status(project) == "fresh"
        else 2.5
        if project_freshness_status(project) == "stale"
        else 0.0
    )


def _ascending_score(value: float, *, red: float, green: float) -> float:
    if value >= green:
        return 1.0
    if value <= red:
        return 0.0
    return (value - red) / (green - red)


def _descending_score(value: float, *, green: float, red: float) -> float:
    if value <= green:
        return 1.0
    if value >= red:
        return 0.0
    return 1.0 - ((value - green) / (red - green))
