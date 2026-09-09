"""Renderers for data product authority artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

AUTHORITY_REPORT_SCHEMA = "dpone.data_product_authority_report.v1"


class AuthorityReportRenderer:
    """Build deterministic authority reports for release/compliance review."""

    def report(self, *, gate: Mapping[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": AUTHORITY_REPORT_SCHEMA,
            "status": gate.get("status"),
            "authority_gate_id": gate.get("authority_gate_id"),
            "profile": gate.get("profile"),
            "authority_check_id": gate.get("authority_check_id"),
            "approval_quorum_id": gate.get("approval_quorum_id"),
            "evidence_signature_ids": list(gate.get("evidence_signature_ids", []))
            if isinstance(gate.get("evidence_signature_ids"), list)
            else [],
            "blockers": list(gate.get("blockers", [])) if isinstance(gate.get("blockers"), list) else [],
            "warnings": list(gate.get("warnings", [])) if isinstance(gate.get("warnings"), list) else [],
            "markdown": _markdown(gate),
        }
        payload["authority_report_id"] = stable_fingerprint(payload)
        return payload


def _markdown(gate: Mapping[str, Any]) -> str:
    lines = [
        "# Data Product Authority Report",
        "",
        f"- status: `{gate.get('status')}`",
        f"- profile: `{gate.get('profile')}`",
        f"- authority gate: `{gate.get('authority_gate_id')}`",
        f"- authority check: `{gate.get('authority_check_id')}`",
        f"- approval quorum: `{gate.get('approval_quorum_id')}`",
    ]
    _append_list(lines, "Evidence Signatures", gate.get("evidence_signature_ids"))
    _append_list(lines, "Blockers", gate.get("blockers"))
    _append_list(lines, "Warnings", gate.get("warnings"))
    return "\n".join(lines) + "\n"


def _append_list(lines: list[str], title: str, values: Any) -> None:
    if not isinstance(values, list) or not values:
        return
    lines.extend(("", f"## {title}", ""))
    lines.extend(f"- `{item}`" for item in values)


__all__ = ["AUTHORITY_REPORT_SCHEMA", "AuthorityReportRenderer"]
