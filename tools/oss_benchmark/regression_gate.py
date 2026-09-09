"""PR regression gate aggregation for benchmark refreshes."""

from __future__ import annotations

from typing import Any


def build_pr_regression_gate(payload: dict[str, Any]) -> dict[str, Any]:
    """Build a compact PR gate from benchmark, candidate and trend evidence."""

    checks: list[dict[str, Any]] = []
    _append_failed_quality_gates(checks, payload.get("quality_gates") or {})
    _append_candidate_budget_failures(checks, payload.get("candidate_quality_delta") or {})
    _append_regression_warnings(checks, payload.get("regression_summary") or {})
    _append_scale_readiness_warnings(checks, payload.get("scale_readiness") or {})
    _append_trend_signals(checks, ((payload.get("trend_summary") or {}).get("dpone") or {}))

    blockers = sum(1 for check in checks if check.get("severity") == "blocker")
    warnings = sum(1 for check in checks if check.get("severity") == "warning")
    improvements = sum(1 for check in checks if check.get("status") == "improved")
    status = "failed" if blockers else "warning" if warnings else "passed"
    return {
        "schema_version": 1,
        "status": status,
        "summary": {
            "blockers": blockers,
            "warnings": warnings,
            "improvements": improvements,
        },
        "checks": checks,
    }


def _append_failed_quality_gates(checks: list[dict[str, Any]], gates: dict[str, Any]) -> None:
    for check in gates.get("failed_checks") or []:
        checks.append(
            {
                "id": f"quality_gate:{check.get('id', 'unknown')}",
                "label": check.get("label", "Quality gate"),
                "status": "failed",
                "severity": "blocker" if check.get("severity") == "blocker" else "warning",
                "previous": None,
                "current": check.get("actual"),
                "delta": None,
                "source": "quality_gates",
            }
        )


def _append_candidate_budget_failures(checks: list[dict[str, Any]], candidate_delta: dict[str, Any]) -> None:
    for budget in candidate_delta.get("failed_budgets") or []:
        checks.append(
            {
                "id": f"candidate:{budget.get('id', budget.get('label', 'budget'))}",
                "label": budget.get("label", "Candidate quality budget"),
                "status": "failed",
                "severity": "blocker",
                "previous": None,
                "current": budget.get("actual"),
                "delta": None,
                "source": "candidate_quality_delta",
            }
        )


def _append_regression_warnings(checks: list[dict[str, Any]], regression_summary: dict[str, Any]) -> None:
    for change in regression_summary.get("regressions") or []:
        checks.append(
            {
                "id": str(change.get("id", "regression")),
                "label": change.get("label", "Regression"),
                "status": "regressed",
                "severity": "warning",
                "previous": change.get("previous"),
                "current": change.get("current"),
                "delta": change.get("delta"),
                "source": "regression_summary",
            }
        )


def _append_scale_readiness_warnings(checks: list[dict[str, Any]], scale_readiness: dict[str, Any]) -> None:
    for warning in scale_readiness.get("warnings") or []:
        checks.append(
            {
                "id": f"scale_readiness:{warning.get('id', 'warning')}",
                "label": warning.get("label", "Scale readiness"),
                "status": "warning",
                "severity": "warning",
                "previous": None,
                "current": warning.get("current"),
                "delta": None,
                "source": "scale_readiness",
            }
        )


def _append_trend_signals(checks: list[dict[str, Any]], trend: dict[str, Any]) -> None:
    trend_specs = (
        ("score", "Industrial Maintainability Index", True),
        ("max_ce", "Max fan-out", False),
        ("p90_ce", "P90 fan-out", False),
        ("largest_module_loc", "Largest module LOC", False),
        ("test_footprint_ratio", "Static test footprint", True),
    )
    existing_ids = {str(check.get("id")) for check in checks}
    for metric_id, label, higher_is_better in trend_specs:
        if metric_id not in trend:
            continue
        delta = _as_number(trend.get(metric_id))
        if delta == 0:
            continue
        improved = (delta > 0 and higher_is_better) or (delta < 0 and not higher_is_better)
        if not improved and metric_id in existing_ids:
            continue
        checks.append(
            {
                "id": metric_id,
                "label": label,
                "status": "improved" if improved else "regressed",
                "severity": "info" if improved else "warning",
                "previous": None,
                "current": None,
                "delta": delta,
                "source": "trend_summary",
            }
        )


def _as_number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


__all__ = ["build_pr_regression_gate"]
