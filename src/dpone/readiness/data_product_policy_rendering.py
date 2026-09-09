"""Renderers for data product policy artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

POLICY_REPORT_SCHEMA = "dpone.data_product_policy_report.v1"


class PolicyReportRenderer:
    """Builds deterministic policy reports for release and compliance review."""

    def report(self, *, gate: Mapping[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": POLICY_REPORT_SCHEMA,
            "status": gate.get("status"),
            "policy_gate_id": gate.get("policy_gate_id"),
            "product_id": gate.get("product_id"),
            "profile": gate.get("profile"),
            "blockers": list(gate.get("blockers", [])) if isinstance(gate.get("blockers"), list) else [],
            "warnings": list(gate.get("warnings", [])) if isinstance(gate.get("warnings"), list) else [],
            "markdown": _markdown(gate),
        }
        payload["policy_report_id"] = stable_fingerprint(payload)
        return payload


def _markdown(gate: Mapping[str, Any]) -> str:
    lines = [
        "# Data Product Policy Report",
        "",
        f"- status: `{gate.get('status')}`",
        f"- product: `{gate.get('product_id')}`",
        f"- profile: `{gate.get('profile')}`",
        f"- policy gate: `{gate.get('policy_gate_id')}`",
        "",
        "## Rule Results",
        "",
        "| Rule | Status | Severity |",
        "| --- | --- | --- |",
    ]
    for rule in gate.get("rules", []) if isinstance(gate.get("rules"), list) else ():
        if isinstance(rule, Mapping):
            lines.append(f"| `{rule.get('id')}` | `{rule.get('status')}` | `{rule.get('severity')}` |")
    _append_list(lines, "Blockers", gate.get("blockers"))
    _append_list(lines, "Warnings", gate.get("warnings"))
    _append_list(lines, "Waived Rules", gate.get("waived_rules"))
    return "\n".join(lines) + "\n"


def _append_list(lines: list[str], title: str, values: Any) -> None:
    if not isinstance(values, list) or not values:
        return
    lines.extend(("", f"## {title}", ""))
    lines.extend(f"- `{item}`" for item in values)


__all__ = ["POLICY_REPORT_SCHEMA", "PolicyReportRenderer"]
