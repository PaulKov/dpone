"""Independent analyzer cross-validation for benchmark audit evidence."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from tools.oss_benchmark.config import ROOT
from tools.oss_benchmark.payload_utils import as_float, as_int, project_name, project_slug

_ANALYZERS = {
    "tokei": {"kind": "loc_sloc", "command": "tokei --output json {path}"},
    "cloc": {"kind": "loc_sloc", "command": "cloc --json {path}"},
    "radon": {"kind": "complexity", "command": "radon cc -j {path}"},
    "lizard": {"kind": "complexity", "command": "lizard {path}"},
}


def build_independent_validation(payload: dict[str, Any]) -> dict[str, Any]:
    """Build an external-analyzer audit pack without fabricating unavailable metrics."""

    projects = [project for project in payload.get("projects", []) if not project.get("unavailable")]
    generated_at = str(payload.get("generated_at") or (payload.get("run_context") or {}).get("generated_at") or "")
    raw_results = list(payload.get("external_analyzer_results") or []) or _discovered_results(projects, generated_at)
    commands = [_command_entry(result, generated_at) for result in raw_results]
    by_project = _results_by_project(raw_results)
    summary: dict[str, dict[str, Any]] = {}
    loc_checks: dict[str, list[dict[str, Any]]] = {}
    complexity_checks: dict[str, list[dict[str, Any]]] = {}
    coverage: dict[str, dict[str, Any]] = {}
    warnings: list[dict[str, Any]] = []

    for project in projects:
        slug = project_slug(project)
        entries = by_project.get(slug, [])
        loc_checks[slug] = _loc_sloc_checks(project, entries)
        complexity_checks[slug] = _complexity_checks(entries)
        coverage[slug] = _coverage(entries)
        summary[slug] = _summary(project, loc_checks[slug], complexity_checks[slug], coverage[slug])
        warnings.extend(_warnings(slug, loc_checks[slug], complexity_checks[slug]))

    return {
        "schema_version": 1,
        "methodology": {
            "purpose": "Cross-check benchmark metrics against independent analyzer outputs when CI provides them.",
            "tools": sorted(_ANALYZERS),
            "availability_policy": "Unavailable analyzers are rendered explicitly; values are never fabricated.",
        },
        "summary": summary,
        "analyzer_commands": commands,
        "loc_sloc_cross_checks": loc_checks,
        "complexity_cross_checks": complexity_checks,
        "analyzer_coverage": coverage,
        "warnings": warnings,
    }


def _discovered_results(projects: list[dict[str, Any]], generated_at: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for project in projects:
        slug = project_slug(project)
        path = _project_path(project)
        for tool, spec in _ANALYZERS.items():
            available = shutil.which(tool)
            results.append(
                {
                    "tool": tool,
                    "project": slug,
                    "status": "available" if available else "unavailable",
                    "command": spec["command"].format(path=path),
                    "version": None,
                    "exit_code": None,
                    "generated_at": generated_at,
                    "error": None if available else f"{tool} not installed or not injected by CI",
                    "metrics": {},
                }
            )
    return results


def _project_path(project: dict[str, Any]) -> str:
    raw_path = str((project.get("spec") or {}).get("path") or ".")
    path = Path(raw_path)
    try:
        relative = path.resolve().relative_to(ROOT.resolve())
    except (OSError, ValueError):
        return f"$WORKSPACE/{path.name}" if path.is_absolute() else path.as_posix()
    relative_path = relative.as_posix()
    return "$WORKSPACE" if relative_path == "." else f"$WORKSPACE/{relative_path}"


def _command_entry(result: dict[str, Any], generated_at: str) -> dict[str, Any]:
    tool = str(result.get("tool", "unknown"))
    return {
        "tool": tool,
        "kind": _ANALYZERS.get(tool, {}).get("kind", "unknown"),
        "project": str(result.get("project") or "unknown"),
        "status": str(result.get("status") or "unavailable"),
        "command": str(result.get("command") or _ANALYZERS.get(tool, {}).get("command", tool)),
        "version": result.get("version"),
        "exit_code": result.get("exit_code"),
        "generated_at": str(result.get("generated_at") or generated_at),
        "last_updated_at": result.get("last_updated_at"),
        "refresh_attempted_at": result.get("refresh_attempted_at"),
        "error": result.get("error"),
    }


def _results_by_project(results: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for result in results:
        grouped.setdefault(str(result.get("project") or "unknown"), []).append(result)
    return grouped


def _loc_sloc_checks(project: dict[str, Any], results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    benchmark_sloc = as_int(_get(project, "loc_with_tests", "total_sloc"))
    benchmark_lines = as_int(_get(project, "loc_with_tests", "total_lines"))
    for result in results:
        if _ANALYZERS.get(str(result.get("tool")), {}).get("kind") != "loc_sloc":
            continue
        metrics = result.get("metrics") or {}
        external_sloc = as_int(metrics.get("total_sloc") or metrics.get("code"))
        external_lines = as_int(metrics.get("total_lines") or metrics.get("lines"))
        delta_percent = _delta_percent(external_sloc, benchmark_sloc)
        status = _loc_status(result, external_sloc, delta_percent)
        checks.append(
            {
                "tool": result.get("tool"),
                "status": status,
                "benchmark_sloc": benchmark_sloc,
                "external_sloc": external_sloc or None,
                "benchmark_lines": benchmark_lines,
                "external_lines": external_lines or None,
                "delta_percent": delta_percent,
                "error": result.get("error"),
            }
        )
    return checks


def _complexity_checks(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for result in results:
        if _ANALYZERS.get(str(result.get("tool")), {}).get("kind") != "complexity":
            continue
        metrics = result.get("metrics") or {}
        avg_complexity = as_float(metrics.get("avg_complexity"))
        max_complexity = as_float(metrics.get("max_complexity"))
        checks.append(
            {
                "tool": result.get("tool"),
                "status": _complexity_status(result, avg_complexity, max_complexity),
                "external_avg_complexity": avg_complexity or None,
                "external_max_complexity": max_complexity or None,
                "files": as_int(metrics.get("files")) or None,
                "error": result.get("error"),
            }
        )
    return checks


def _coverage(results: list[dict[str, Any]]) -> dict[str, Any]:
    validated = sorted(
        str(result.get("tool")) for result in results if result.get("status") == "fresh" and result.get("metrics")
    )
    stale = sorted(str(result.get("tool")) for result in results if result.get("status") == "stale")
    unavailable = sorted(str(result.get("tool")) for result in results if result.get("status") == "unavailable")
    if len(validated) >= 3:
        status = "audit-ready"
    elif len(validated) >= 2:
        status = "high"
    elif validated:
        status = "medium"
    elif stale:
        status = "stale"
    else:
        status = "unavailable"
    return {
        "validated_tool_count": len(validated),
        "validated_tools": validated,
        "stale_tools": stale,
        "unavailable_tools": unavailable,
        "coverage_status": status,
    }


def _summary(
    project: dict[str, Any],
    loc_checks: list[dict[str, Any]],
    complexity_checks: list[dict[str, Any]],
    coverage: dict[str, Any],
) -> dict[str, Any]:
    loc_status = _rollup_status(loc_checks)
    complexity_status = _rollup_status(complexity_checks)
    unavailable = len(coverage.get("unavailable_tools") or [])
    stale = len(coverage.get("stale_tools") or [])
    score = 50 + _status_points(loc_status, 22) + _status_points(complexity_status, 18)
    score += min(12, as_int(coverage.get("validated_tool_count")) * 4)
    score -= unavailable * 2
    score -= stale
    score = max(0, min(100, score))
    return {
        "name": project_name(project),
        "confidence_score": score,
        "validation_band": _band(score),
        "loc_sloc_status": loc_status,
        "complexity_status": complexity_status,
        "unavailable_analyzers": unavailable,
        "stale_analyzers": stale,
    }


def _warnings(
    slug: str,
    loc_checks: list[dict[str, Any]],
    complexity_checks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for check in [*loc_checks, *complexity_checks]:
        if check.get("status") not in {"warning", "failed"}:
            continue
        warnings.append(
            {
                "id": f"{check.get('tool')}_{check.get('status')}",
                "project": slug,
                "label": f"{check.get('tool')} validation",
                "severity": "warning",
                "message": f"{check.get('tool')} returned {check.get('status')} in independent validation.",
            }
        )
    return warnings


def _loc_status(result: dict[str, Any], external_sloc: int, delta_percent: float | None) -> str:
    if result.get("status") == "stale" and external_sloc:
        return "stale"
    if result.get("status") != "fresh" or not external_sloc or delta_percent is None:
        return "unavailable"
    if delta_percent <= 3:
        return "passed"
    if delta_percent <= 8:
        return "warning"
    return "failed"


def _complexity_status(result: dict[str, Any], avg_complexity: float, max_complexity: float) -> str:
    if result.get("status") == "stale" and avg_complexity:
        return "stale"
    if result.get("status") == "fresh" and as_int((result.get("metrics") or {}).get("files")) == 0:
        return "not_applicable"
    if result.get("status") != "fresh" or not avg_complexity:
        return "unavailable"
    if avg_complexity <= 8 and max_complexity <= 35:
        return "passed"
    if avg_complexity <= 12 and max_complexity <= 55:
        return "warning"
    return "failed"


def _rollup_status(checks: list[dict[str, Any]]) -> str:
    statuses = {str(check.get("status")) for check in checks}
    if "failed" in statuses:
        return "failed"
    if "warning" in statuses:
        return "warning"
    if "passed" in statuses:
        return "passed"
    if "stale" in statuses:
        return "stale"
    if statuses == {"not_applicable"}:
        return "not_applicable"
    return "unavailable"


def _status_points(status: str, full_points: int) -> int:
    if status == "passed":
        return full_points
    if status == "warning":
        return int(full_points * 0.45)
    if status == "stale":
        return int(full_points * 0.3)
    return 0


def _delta_percent(external: int, benchmark: int) -> float | None:
    if not external or not benchmark:
        return None
    return round(abs(external - benchmark) / benchmark * 100, 2)


def _band(score: int) -> str:
    if score >= 90:
        return "audit-ready"
    if score >= 75:
        return "high"
    if score >= 55:
        return "medium"
    return "low"


def _get(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current
