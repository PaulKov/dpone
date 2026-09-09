"""Candidate-vs-baseline quality delta for benchmark PR governance."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import as_float, as_int, find_project, get_value

DEFAULT_QUALITY_BUDGETS: dict[str, Any] = {
    "max_module_loc": 600,
    "watch_module_loc": 400,
    "max_fan_out": 30,
    "p90_fan_out": 12,
    "cohesion": 0.55,
    "test_footprint_tolerance": 0.02,
}


def build_candidate_quality_delta(
    payload: dict[str, Any],
    *,
    baseline_payload: dict[str, Any] | None,
    project_slug: str = "dpone",
    budgets: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare candidate dpone evidence with an accepted baseline evidence payload."""

    if baseline_payload is None:
        return _empty_delta(project_slug, status="no_baseline")
    candidate = find_project(list(payload.get("projects", [])), project_slug)
    baseline = find_project(list(baseline_payload.get("projects", [])), project_slug)
    if not candidate or not baseline:
        return _empty_delta(project_slug, status="no_baseline")

    active_budgets = {**DEFAULT_QUALITY_BUDGETS, **(budgets or {})}
    quality_budgets = _evaluate_quality_budgets(candidate, baseline, active_budgets)
    failed_budgets = [budget for budget in quality_budgets if budget["status"] == "failed"]
    changed_modules = _changed_modules(candidate, baseline, active_budgets)
    return {
        "schema_version": 1,
        "project": project_slug,
        "status": "failed" if failed_budgets else "passed",
        "baseline_source": _baseline_source(baseline_payload),
        "quality_budgets": quality_budgets,
        "failed_budgets": failed_budgets,
        "changed_modules": changed_modules,
    }


def _empty_delta(project_slug: str, *, status: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "project": project_slug,
        "status": status,
        "baseline_source": "",
        "quality_budgets": [],
        "failed_budgets": [],
        "changed_modules": [],
    }


def _evaluate_quality_budgets(
    candidate: dict[str, Any],
    baseline: dict[str, Any],
    budgets: dict[str, Any],
) -> list[dict[str, Any]]:
    baseline_test_footprint = as_float(get_value(baseline, "coverage_confidence", "test_footprint_ratio"))
    min_test_footprint = max(0.0, baseline_test_footprint - as_float(budgets["test_footprint_tolerance"]))
    checks = [
        _budget_check(
            "max_module_loc",
            "Largest production module LOC",
            get_value(candidate, "loc_without_tests", "max_lines"),
            "<=",
            budgets["max_module_loc"],
        ),
        _budget_check(
            "p90_fan_out",
            "P90 fan-out",
            get_value(candidate, "coupling", "p90_ce"),
            "<=",
            budgets["p90_fan_out"],
        ),
        _budget_check(
            "max_fan_out",
            "Maximum fan-out",
            get_value(candidate, "coupling", "max_ce"),
            "<=",
            budgets["max_fan_out"],
        ),
        _budget_check(
            "cohesion",
            "Cohesion ratio",
            get_value(candidate, "coupling", "cohesion_ratio"),
            ">=",
            budgets["cohesion"],
        ),
        _budget_check(
            "test_footprint",
            "Static test footprint",
            get_value(candidate, "coverage_confidence", "test_footprint_ratio"),
            ">=",
            min_test_footprint,
        ),
    ]
    return checks


def _budget_check(check_id: str, label: str, actual: Any, operator: str, threshold: Any) -> dict[str, Any]:
    passed = _compare(actual, operator, threshold)
    return {
        "id": check_id,
        "label": label,
        "actual": as_float(actual),
        "operator": operator,
        "threshold": as_float(threshold),
        "status": "passed" if passed else "failed",
    }


def _compare(actual: Any, operator: str, threshold: Any) -> bool:
    actual_value = as_float(actual)
    threshold_value = as_float(threshold)
    if operator == "<=":
        return actual_value <= threshold_value
    if operator == ">=":
        return actual_value >= threshold_value
    raise ValueError(f"Unsupported budget operator: {operator}")


def _changed_modules(
    candidate: dict[str, Any],
    baseline: dict[str, Any],
    budgets: dict[str, Any],
) -> list[dict[str, Any]]:
    baseline_modules = _module_index(get_value(baseline, "top_loc_without_tests", default=[]))
    candidate_modules = _module_index(get_value(candidate, "top_loc_without_tests", default=[]))
    fan_out_baseline = _fan_out_index(get_value(baseline, "coupling", "top_out", default=[]))
    fan_out_candidate = _fan_out_index(get_value(candidate, "coupling", "top_out", default=[]))
    modules: list[dict[str, Any]] = []
    for path, current in sorted(candidate_modules.items()):
        previous = baseline_modules.get(path)
        loc_delta = as_int(current.get("lines")) - as_int((previous or {}).get("lines"))
        sloc_delta = as_int(current.get("sloc")) - as_int((previous or {}).get("sloc"))
        module_name = _path_to_module(path)
        fan_out_delta = fan_out_candidate.get(module_name, 0) - fan_out_baseline.get(module_name, 0)
        status = "new" if previous is None else "changed" if loc_delta or sloc_delta or fan_out_delta else "unchanged"
        if status == "unchanged" and as_int(current.get("lines")) < as_int(budgets["watch_module_loc"]):
            continue
        modules.append(
            {
                "path": path,
                "status": status,
                "loc": as_int(current.get("lines")),
                "sloc": as_int(current.get("sloc")),
                "loc_delta": loc_delta,
                "sloc_delta": sloc_delta,
                "fan_out": fan_out_candidate.get(module_name, 0),
                "fan_out_delta": fan_out_delta,
                "risk": _module_risk(as_int(current.get("lines")), fan_out_candidate.get(module_name, 0), budgets),
            }
        )
    modules.sort(key=lambda item: (_risk_rank(item["risk"]), -abs(item["loc_delta"]), item["path"]))
    return modules[:10]


def _module_index(items: Any) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for item in items or []:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or "")
        if path:
            indexed[path] = item
    return indexed


def _fan_out_index(items: Any) -> dict[str, int]:
    indexed: dict[str, int] = {}
    for item in items or []:
        if isinstance(item, dict):
            module = str(item.get("module") or item.get("name") or "")
            value = as_int(item.get("fan_out") or item.get("value") or item.get("count"))
        else:
            try:
                module = str(item[0])
                value = as_int(item[1])
            except (IndexError, TypeError):
                continue
        if module:
            indexed[module] = value
    return indexed


def _path_to_module(path: str) -> str:
    normalized = path.removesuffix(".py").replace("/", ".")
    return normalized.removeprefix("src.")


def _module_risk(loc: int, fan_out: int, budgets: dict[str, Any]) -> str:
    if loc > as_int(budgets["max_module_loc"]) or fan_out > as_int(budgets["max_fan_out"]):
        return "fail"
    if loc >= as_int(budgets["watch_module_loc"]) or fan_out >= 15:
        return "watch"
    return "ok"


def _risk_rank(risk: str) -> int:
    return {"fail": 0, "watch": 1, "ok": 2}.get(risk, 3)


def _baseline_source(payload: dict[str, Any]) -> str:
    run_context = payload.get("run_context") or {}
    generated_at = run_context.get("generated_at") or payload.get("generated_at") or ""
    git_sha = run_context.get("git_sha") or ""
    if generated_at and git_sha:
        return f"{generated_at}@{git_sha}"
    return str(generated_at or git_sha)
