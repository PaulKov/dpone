"""Render data product compliance audit packages."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

AUDIT_PACKAGE_SCHEMA = "dpone.data_product_audit_package.v1"


class AuditPackageRenderer:
    """Builds deterministic audit-ready JSON and Markdown packages."""

    def render(self, *, gate: Mapping[str, Any]) -> dict[str, Any]:
        markdown = _markdown(gate)
        payload: dict[str, Any] = {
            "schema_version": AUDIT_PACKAGE_SCHEMA,
            "status": gate.get("status"),
            "product": dict(gate.get("product", {})) if isinstance(gate.get("product"), Mapping) else {},
            "product_id": gate.get("product_id"),
            "profile": gate.get("profile"),
            "compliance_gate_id": gate.get("compliance_gate_id"),
            "compliance_evaluation_id": gate.get("compliance_evaluation_id"),
            "compliance_plan_id": gate.get("compliance_plan_id"),
            "pack_id": gate.get("pack_id"),
            "bundle_id": gate.get("bundle_id"),
            "controls": [dict(control) for control in gate.get("controls", []) if isinstance(control, Mapping)],
            "evidence_refs": [dict(ref) for ref in gate.get("evidence_refs", []) if isinstance(ref, Mapping)],
            "blockers": list(gate.get("blockers", [])),
            "warnings": list(gate.get("warnings", [])),
            "markdown": markdown,
        }
        payload["audit_package_id"] = stable_fingerprint(payload)
        return payload


def _markdown(gate: Mapping[str, Any]) -> str:
    product = gate.get("product") if isinstance(gate.get("product"), Mapping) else {}
    lines = [
        "# Data Product Audit Evidence Package",
        "",
        f"- status: {gate.get('status')}",
        f"- profile: {gate.get('profile')}",
        f"- product: {product.get('id') or gate.get('product_id') or ''}",
        f"- owner: {product.get('owner') or ''}",
        f"- compliance_gate_id: {gate.get('compliance_gate_id')}",
        "",
        "## Control Matrix",
        "",
        "| Framework | Control | Severity | Status | Evidence |",
        "| --- | --- | --- | --- | --- |",
    ]
    for control in gate.get("controls", []):
        if not isinstance(control, Mapping):
            continue
        evidence = ", ".join(
            str(ref.get("kind")) for ref in control.get("evidence_refs", []) if isinstance(ref, Mapping)
        )
        lines.append(
            "| {framework} | {control} | {severity} | {status} | {evidence} |".format(
                framework=control.get("framework_id", ""),
                control=control.get("control_id", ""),
                severity=control.get("severity", ""),
                status=control.get("status", ""),
                evidence=evidence,
            )
        )
    blockers = list(gate.get("blockers", []))
    lines.extend(["", "## Blockers", ""])
    if blockers:
        lines.extend(f"- {item}" for item in blockers)
    else:
        lines.append("- none")
    warnings = list(gate.get("warnings", []))
    lines.extend(["", "## Warnings", ""])
    if warnings:
        lines.extend(f"- {item}" for item in warnings)
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


__all__ = ["AUDIT_PACKAGE_SCHEMA", "AuditPackageRenderer"]
