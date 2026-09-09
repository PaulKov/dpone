"""Schema constants for data product progressive delivery artifacts."""

ROLLOUT_PLAN_SCHEMA = "dpone.data_product_rollout_plan.v1"
SHADOW_VALIDATION_SCHEMA = "dpone.data_product_shadow_validation.v1"
RING_GATE_SCHEMA = "dpone.data_product_ring_gate.v1"
ROLLOUT_PROMOTION_SCHEMA = "dpone.data_product_rollout_promotion.v1"
ROLLOUT_REPORT_SCHEMA = "dpone.data_product_rollout_report.v1"

__all__ = [
    "RING_GATE_SCHEMA",
    "ROLLOUT_PLAN_SCHEMA",
    "ROLLOUT_PROMOTION_SCHEMA",
    "ROLLOUT_REPORT_SCHEMA",
    "SHADOW_VALIDATION_SCHEMA",
]
