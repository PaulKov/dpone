"""Approval gate for schema impact plans."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from dpone.readiness.migration_control import MigrationPack
from dpone.readiness.schema_impact_models import SCHEMA_IMPACT_GATE_SCHEMA


class SchemaImpactGate:
    def evaluate(
        self,
        *,
        pack: MigrationPack,
        impact_plan: Mapping[str, Any],
        approval: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        required = tuple(str(item) for item in impact_plan.get("required_approvals", []) if item)
        mode = str(impact_plan.get("mode", "gate"))
        blockers = list(_staleness_blockers(pack, impact_plan, approval))
        if mode == "gate":
            blockers.extend(_approval_blockers(required, approval))
        status = "blocked" if blockers else ("observed" if mode == "observe" else "allowed")
        return {
            "schema_version": SCHEMA_IMPACT_GATE_SCHEMA,
            "command": "impact_gate",
            "status": status,
            "pack_id": pack.pack_id,
            "impact_plan_id": impact_plan.get("impact_plan_id"),
            "required_approvals": list(required),
            "blockers": blockers,
            "warnings": list(impact_plan.get("warnings", [])),
            "impacted_consumers": list(impact_plan.get("impacted_consumers", [])),
        }


def _staleness_blockers(
    pack: MigrationPack,
    impact_plan: Mapping[str, Any],
    approval: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    blockers: list[str] = []
    if impact_plan.get("pack_id") != pack.pack_id:
        blockers.append("schema_impact.pack_id_mismatch")
    if not approval:
        return tuple(blockers)
    if approval.get("pack_id") != pack.pack_id:
        blockers.append("schema_impact.approval_pack_id_mismatch")
    if approval.get("impact_plan_id") != impact_plan.get("impact_plan_id"):
        blockers.append("schema_impact.approval_impact_plan_id_mismatch")
    expires_at = approval.get("expires_at")
    if expires_at and _expired(str(expires_at)):
        blockers.append("schema_impact.approval_expired")
    return tuple(blockers)


def _approval_blockers(required: tuple[str, ...], approval: Mapping[str, Any] | None) -> tuple[str, ...]:
    approved = set()
    if approval and isinstance(approval.get("approved_risks"), list):
        approved = {str(item) for item in approval["approved_risks"]}
    return tuple(f"schema_impact.approval_required:{risk}" for risk in required if risk not in approved)


def _expired(value: str) -> bool:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized) < datetime.now(UTC)


__all__ = ["SchemaImpactGate"]
