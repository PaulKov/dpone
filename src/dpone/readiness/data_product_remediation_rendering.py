"""Renderers for data product remediation runbooks and reports."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness import data_product_remediation_support as support


class RemediationRenderer:
    """Render operator-friendly remediation artifacts without side effects."""

    def runbook(self, *, plan: Mapping[str, Any]) -> dict[str, Any]:
        blockers = list(support.strings(plan.get("blockers")))
        payload = {
            "schema_version": support.RUNBOOK_SCHEMA,
            "status": "blocked" if blockers else "rendered",
            "product": _product(plan),
            "product_id": plan.get("product_id"),
            "pack_id": plan.get("pack_id"),
            "bundle_id": plan.get("bundle_id"),
            "remediation_plan_id": plan.get("remediation_plan_id"),
            "actions": [dict(action) for action in support.mappings(plan.get("actions"))],
            "blockers": blockers,
            "warnings": list(support.strings(plan.get("warnings"))),
        }
        payload["markdown"] = _runbook_markdown(payload)
        return support.payload_id(payload, "remediation_runbook_id")

    def report(self, *, gate: Mapping[str, Any], closeout: Mapping[str, Any] | None = None) -> dict[str, Any]:
        closeout = closeout or {}
        blockers = [*support.strings(gate.get("blockers")), *support.strings(closeout.get("blockers"))]
        warnings = [*support.strings(gate.get("warnings")), *support.strings(closeout.get("warnings"))]
        status = support.status(blockers, warnings, "allowed")
        payload = {
            "schema_version": support.REPORT_SCHEMA,
            "status": status,
            "product": _product(gate) or _product(closeout),
            "product_id": gate.get("product_id") or closeout.get("product_id"),
            "pack_id": gate.get("pack_id") or closeout.get("pack_id"),
            "bundle_id": gate.get("bundle_id") or closeout.get("bundle_id"),
            "remediation_gate_id": gate.get("remediation_gate_id"),
            "remediation_closeout_id": closeout.get("remediation_closeout_id"),
            "blockers": support.dedupe(blockers),
            "warnings": support.dedupe(warnings),
        }
        payload["markdown"] = _report_markdown(payload, gate, closeout)
        return support.payload_id(payload, "remediation_report_id")


def _runbook_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Data Product Remediation Runbook",
        "",
        f"- product: {payload.get('product_id') or 'unknown'}",
        f"- status: {payload.get('status')}",
        f"- actions: {len(support.mappings(payload.get('actions')))}",
        "",
    ]
    actions = support.mappings(payload.get("actions"))
    if not actions:
        lines.append("No remediation actions are required.")
        return "\n".join(lines) + "\n"
    lines.append("## Actions")
    for index, action in enumerate(actions, start=1):
        lines.extend(
            [
                "",
                f"{index}. {action.get('domain')} - {action.get('repair_class')}",
                f"   - owner: {action.get('owner') or 'unassigned'}",
                f"   - source: {action.get('source_code')}",
            ]
        )
        for command in support.strings(action.get("commands")):
            lines.append(f"   - command: `{command}`")
        expected = ", ".join(str(item.get("kind")) for item in support.mappings(action.get("expected_evidence")))
        lines.append(f"   - expected evidence: {expected}")
    return "\n".join(lines) + "\n"


def _report_markdown(payload: Mapping[str, Any], gate: Mapping[str, Any], closeout: Mapping[str, Any]) -> str:
    lines = [
        "# Data Product Remediation Report",
        "",
        f"- product: {payload.get('product_id') or 'unknown'}",
        f"- status: {payload.get('status')}",
        f"- remediation_gate_id: {gate.get('remediation_gate_id') or 'n/a'}",
        f"- remediation_closeout_id: {closeout.get('remediation_closeout_id') or 'n/a'}",
        "",
    ]
    if payload.get("blockers"):
        lines.append("## Blockers")
        lines.extend(f"- {item}" for item in support.strings(payload.get("blockers")))
        lines.append("")
    if payload.get("warnings"):
        lines.append("## Warnings")
        lines.extend(f"- {item}" for item in support.strings(payload.get("warnings")))
        lines.append("")
    lines.append("## Next Action")
    if payload.get("status") == "blocked":
        lines.append("- Attach fresh expected evidence and rerun remediation closeout.")
    else:
        lines.append("- Record remediation closeout in the evidence registry.")
    return "\n".join(lines) + "\n"


def _product(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = payload.get("product")
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = ["RemediationRenderer"]
