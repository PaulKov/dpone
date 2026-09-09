"""Schema-version constants for data product cost governance artifacts."""

from __future__ import annotations

COST_PLAN_SCHEMA = "dpone.data_product_cost_plan.v1"
COST_EVALUATION_SCHEMA = "dpone.data_product_cost_evaluation.v1"
COST_GATE_SCHEMA = "dpone.data_product_cost_gate.v1"
COST_FORECAST_SCHEMA = "dpone.data_product_cost_forecast.v1"
COST_REPORT_SCHEMA = "dpone.data_product_cost_report.v1"

__all__ = [
    "COST_EVALUATION_SCHEMA",
    "COST_FORECAST_SCHEMA",
    "COST_GATE_SCHEMA",
    "COST_PLAN_SCHEMA",
    "COST_REPORT_SCHEMA",
]
