"""Forecast and report rendering for data product cost governance."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness import data_product_cost_support as support


class CostForecastEvaluator:
    """Produces plan-only trend signals from latest and historical cost evaluations."""

    def forecast(self, *, evaluation: Mapping[str, Any], history: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if evaluation.get("status") == "disabled":
            return _forecast_payload(evaluation, (), "disabled", (), ())
        points = tuple(item for item in support.mappings((history or {}).get("evaluations")) if item)
        latest = float(_mapping(evaluation.get("estimated_costs")).get("per_run_cost") or 0)
        historical = [
            float(_mapping(item.get("estimated_costs")).get("per_run_cost") or 0)
            for item in points
            if isinstance(item.get("estimated_costs"), Mapping)
        ]
        warnings = []
        if historical and latest > (sum(historical) / len(historical)) * 1.5:
            warnings.append("data_product_cost.forecast_run_cost_spike")
        return _forecast_payload(evaluation, points, "warning" if warnings else "ready", (), warnings)


class CostGovernanceRenderer:
    """Renders deterministic cost governance reports."""

    def report(self, *, gate: Mapping[str, Any], forecast: Mapping[str, Any] | None = None) -> dict[str, Any]:
        status = "blocked" if gate.get("status") == "blocked" else "warning" if gate.get("warnings") else "allowed"
        payload = {
            "schema_version": support.COST_REPORT_SCHEMA,
            "status": status,
            "product": _mapping(gate.get("product")),
            "product_id": gate.get("product_id"),
            "cost_gate_id": gate.get("cost_gate_id"),
            "cost_forecast_id": (forecast or {}).get("cost_forecast_id"),
            "estimated_costs": dict(_mapping(gate.get("estimated_costs"))),
            "capacity": dict(_mapping(gate.get("capacity"))),
            "blockers": list(support.strings(gate.get("blockers"))),
            "warnings": [*support.strings(gate.get("warnings")), *support.strings((forecast or {}).get("warnings"))],
        }
        payload["markdown"] = _report_markdown(payload)
        return support.payload_id(payload, "cost_report_id")


def _forecast_payload(
    evaluation: Mapping[str, Any],
    points: Sequence[Mapping[str, Any]],
    status: str,
    blockers: Sequence[str],
    warnings: Sequence[str],
) -> dict[str, Any]:
    payload = {
        "schema_version": support.COST_FORECAST_SCHEMA,
        "status": status,
        "product": _mapping(evaluation.get("product")),
        "product_id": evaluation.get("product_id"),
        "cost_evaluation_id": evaluation.get("cost_evaluation_id"),
        "summary": {"history_points": len(points)},
        "latest": dict(_mapping(evaluation.get("estimated_costs"))),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "markdown": _forecast_markdown(evaluation, points, warnings),
    }
    return support.payload_id(payload, "cost_forecast_id")


def _report_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Data Product Cost Governance Report",
        "",
        f"- status: {payload.get('status')}",
        f"- product: {payload.get('product_id') or ''}",
        f"- cost_gate_id: {payload.get('cost_gate_id') or ''}",
        "",
        "## Estimated Costs",
    ]
    lines.extend(f"- {key}: {value}" for key, value in _mapping(payload.get("estimated_costs")).items())
    if payload.get("blockers"):
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- {item}" for item in payload["blockers"])
    if payload.get("warnings"):
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {item}" for item in payload["warnings"])
    return "\n".join(lines) + "\n"


def _forecast_markdown(
    evaluation: Mapping[str, Any], points: Sequence[Mapping[str, Any]], warnings: Sequence[str]
) -> str:
    lines = [
        "# Data Product Cost Forecast",
        "",
        f"- status: {'warning' if warnings else 'ready'}",
        f"- product: {evaluation.get('product_id') or ''}",
        f"- history_points: {len(points)}",
    ]
    if warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {item}" for item in warnings)
    return "\n".join(lines) + "\n"


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


__all__ = ["CostForecastEvaluator", "CostGovernanceRenderer"]
