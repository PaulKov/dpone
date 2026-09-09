from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness import data_product_audit_retention_support as support
from dpone.readiness.migration_control import stable_fingerprint

LEGAL_HOLD_SCHEMA = "dpone.data_product_legal_hold.v1"


class LegalHoldService:
    """Applies legal-hold evidence to one audit archive run."""

    def apply(
        self,
        *,
        archive_run: Mapping[str, Any],
        reason: str,
        authority_gate: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        blockers: list[str] = []
        if not reason.strip():
            blockers.append("data_product_audit_retention.legal_hold_reason_required")
        if authority_gate and authority_gate.get("status") not in {"allowed", "warning"}:
            blockers.append("data_product_audit_retention.authority_gate_blocked")
        if authority_gate is None:
            blockers.append("data_product_audit_retention.authority_gate_required")
        status = "blocked" if blockers else "held"
        payload: dict[str, Any] = {
            "schema_version": LEGAL_HOLD_SCHEMA,
            "status": status,
            "product": dict(archive_run.get("product", {})) if isinstance(archive_run.get("product"), Mapping) else {},
            "product_id": support.product_id(archive_run),
            "audit_archive_run_id": archive_run.get("audit_archive_run_id"),
            "archive_uri": archive_run.get("archive_uri"),
            "reason": reason,
            "authority_gate_id": authority_gate.get("authority_gate_id")
            if isinstance(authority_gate, Mapping)
            else None,
            "blockers": blockers,
            "warnings": [],
        }
        payload["legal_hold_id"] = stable_fingerprint(payload)
        return payload
