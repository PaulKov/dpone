"""Renderers for data product access governance artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

ACCESS_REPORT_SCHEMA = "dpone.data_product_access_report.v1"


class AccessGovernanceRenderer:
    """Renders deterministic access governance reports."""

    def report(self, *, gate: Mapping[str, Any]) -> dict[str, Any]:
        status = str(gate.get("status") or "unknown")
        payload: dict[str, Any] = {
            "schema_version": ACCESS_REPORT_SCHEMA,
            "status": "blocked" if status == "blocked" else "warning" if status == "warning" else "allowed",
            "product": dict(gate.get("product", {})) if isinstance(gate.get("product"), Mapping) else {},
            "product_id": gate.get("product_id"),
            "access_gate_id": gate.get("access_gate_id"),
            "entitlement_plan_id": gate.get("entitlement_plan_id"),
            "privacy_impact_id": gate.get("privacy_impact_id"),
            "summary": dict(gate.get("summary", {})) if isinstance(gate.get("summary"), Mapping) else {},
            "blockers": list(gate.get("blockers", [])),
            "warnings": list(gate.get("warnings", [])),
        }
        payload["markdown"] = _markdown(payload)
        payload["access_report_id"] = stable_fingerprint(payload)
        return payload


def _markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Data Product Access Governance Report",
        "",
        f"- status: {payload.get('status')}",
        f"- product: {payload.get('product_id')}",
        f"- access_gate_id: {payload.get('access_gate_id')}",
        "",
        "## Blockers",
    ]
    blockers = list(payload.get("blockers", []))
    lines.extend(f"- {item}" for item in blockers) if blockers else lines.append("- none")
    lines.extend(["", "## Warnings"])
    warnings = list(payload.get("warnings", []))
    lines.extend(f"- {item}" for item in warnings) if warnings else lines.append("- none")
    return "\n".join(lines) + "\n"


__all__ = ["ACCESS_REPORT_SCHEMA", "AccessGovernanceRenderer"]
