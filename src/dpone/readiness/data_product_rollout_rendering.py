"""Rendering helpers for data product rollout evidence."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness import data_product_rollout_support as support


class RolloutRenderer:
    """Renders operator-friendly rollout reports."""

    def report(self, *, promotion: Mapping[str, Any]) -> dict[str, Any]:
        status = str(promotion.get("status") or "unknown")
        payload = {
            "schema_version": support.ROLLOUT_REPORT_SCHEMA,
            "status": status,
            "product": support.mapping(promotion.get("product")),
            "product_id": promotion.get("product_id"),
            "rollout_promotion_id": promotion.get("rollout_promotion_id"),
            "ring_gate_id": promotion.get("ring_gate_id"),
            "from_ring": promotion.get("from_ring"),
            "to_ring": promotion.get("to_ring"),
            "decision": promotion.get("decision"),
            "blockers": list(support.strings(promotion.get("blockers"))),
            "warnings": list(support.strings(promotion.get("warnings"))),
        }
        payload["markdown"] = _markdown(payload)
        return support.payload_id(payload, "rollout_report_id")


def _markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Data Product Rollout Report",
        "",
        f"- status: {payload.get('status')}",
        f"- product: {payload.get('product_id') or ''}",
        f"- from_ring: {payload.get('from_ring') or ''}",
        f"- to_ring: {payload.get('to_ring') or ''}",
        f"- decision: {payload.get('decision') or ''}",
    ]
    if payload.get("blockers"):
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- {item}" for item in payload["blockers"])
    if payload.get("warnings"):
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {item}" for item in payload["warnings"])
    return "\n".join(lines) + "\n"


__all__ = ["RolloutRenderer"]
