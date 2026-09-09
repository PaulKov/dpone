"""Cost-governance bundle artifact relationships."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol


class BundleArtifact(Protocol):
    payload: dict[str, Any]


COST_GOVERNANCE_STATUS_CHECKS = (
    ("data_product_cost_gate", {"allowed", "warning"}, "migration_bundle.data_product_cost_gate_blocked"),
    ("data_product_cost_forecast", {"ready", "warning"}, "migration_bundle.data_product_cost_forecast_blocked"),
    ("data_product_cost_report", {"allowed", "warning"}, "migration_bundle.data_product_cost_report_blocked"),
)

COST_GOVERNANCE_PACK_BOUND = {
    "data_product_cost_gate": "migration_bundle.data_product_cost_gate_pack_id_mismatch",
    "data_product_cost_forecast": "migration_bundle.data_product_cost_forecast_pack_id_mismatch",
    "data_product_cost_report": "migration_bundle.data_product_cost_report_pack_id_mismatch",
}


def cost_governance_summary_ids(artifacts: Mapping[str, BundleArtifact]) -> dict[str, Any]:
    return {
        "data_product_cost_gate_id": _payload_value(artifacts.get("data_product_cost_gate"), "cost_gate_id"),
        "data_product_cost_forecast_id": _payload_value(
            artifacts.get("data_product_cost_forecast"), "cost_forecast_id"
        ),
        "data_product_cost_report_id": _payload_value(artifacts.get("data_product_cost_report"), "cost_report_id"),
    }


def _payload_value(artifact: BundleArtifact | None, key: str) -> Any:
    return artifact.payload.get(key) if artifact else None


__all__ = [
    "COST_GOVERNANCE_PACK_BOUND",
    "COST_GOVERNANCE_STATUS_CHECKS",
    "cost_governance_summary_ids",
]
