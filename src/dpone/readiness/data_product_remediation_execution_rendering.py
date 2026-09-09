"""Renderers for remediation execution artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness import data_product_remediation_execution_support as support


class RemediationExecutionRenderer:
    """Render operator-friendly remediation execution reports."""

    def report(self, *, certificate: Mapping[str, Any]) -> dict[str, Any]:
        status = str(certificate.get("status") or "blocked")
        payload = {
            "schema_version": support.REPORT_SCHEMA,
            "status": status,
            "product": _product(certificate),
            "product_id": certificate.get("product_id"),
            "pack_id": certificate.get("pack_id"),
            "bundle_id": certificate.get("bundle_id"),
            "remediation_execution_certificate_id": certificate.get("remediation_execution_certificate_id"),
            "blockers": list(support.strings(certificate.get("blockers"))),
            "warnings": list(support.strings(certificate.get("warnings"))),
        }
        payload["markdown"] = _markdown(payload)
        return support.payload_id(payload, "remediation_execution_report_id")


def _markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Data Product Remediation Execution Report",
        "",
        f"- product: {payload.get('product_id') or 'unknown'}",
        f"- status: {payload.get('status')}",
        f"- certificate: {payload.get('remediation_execution_certificate_id') or 'n/a'}",
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
    if payload.get("status") == "certified":
        lines.append("- Record the certificate in the evidence registry and continue release closeout.")
    else:
        lines.append("- Resolve execution blockers and rerun certification with fresh evidence.")
    return "\n".join(lines) + "\n"


def _product(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = payload.get("product")
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = ["RemediationExecutionRenderer"]
