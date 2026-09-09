"""Rollout bundle relationship constants and summary IDs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol


class BundleArtifact(Protocol):
    payload: dict[str, Any]


ROLLOUT_STATUS_CHECKS = (
    ("data_product_ring_gate", {"allowed", "warning"}, "migration_bundle.data_product_ring_gate_blocked"),
    (
        "data_product_shadow_validation",
        {"passed", "warning", "disabled"},
        "migration_bundle.data_product_shadow_validation_blocked",
    ),
    (
        "data_product_rollout_promotion",
        {"promoted"},
        "migration_bundle.data_product_rollout_promotion_blocked",
    ),
    (
        "data_product_rollout_report",
        {"promoted", "warning"},
        "migration_bundle.data_product_rollout_report_blocked",
    ),
)

ROLLOUT_PACK_BOUND = {
    "data_product_ring_gate": "migration_bundle.data_product_ring_gate_pack_id_mismatch",
    "data_product_shadow_validation": "migration_bundle.data_product_shadow_validation_pack_id_mismatch",
    "data_product_rollout_promotion": "migration_bundle.data_product_rollout_promotion_pack_id_mismatch",
    "data_product_rollout_report": "migration_bundle.data_product_rollout_report_pack_id_mismatch",
}


def rollout_summary_ids(artifacts: Mapping[str, BundleArtifact]) -> dict[str, Any]:
    return {
        "data_product_ring_gate_id": _payload_value(artifacts.get("data_product_ring_gate"), "ring_gate_id"),
        "data_product_shadow_validation_id": _payload_value(
            artifacts.get("data_product_shadow_validation"), "shadow_validation_id"
        ),
        "data_product_rollout_promotion_id": _payload_value(
            artifacts.get("data_product_rollout_promotion"), "rollout_promotion_id"
        ),
        "data_product_rollout_report_id": _payload_value(
            artifacts.get("data_product_rollout_report"), "rollout_report_id"
        ),
    }


def _payload_value(artifact: BundleArtifact | None, key: str) -> Any:
    return artifact.payload.get(key) if artifact else None


__all__ = ["ROLLOUT_PACK_BOUND", "ROLLOUT_STATUS_CHECKS", "rollout_summary_ids"]
