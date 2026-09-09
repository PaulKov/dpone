"""Renderers for data product access enforcement artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

REPORT_SCHEMA = "dpone.data_product_access_enforcement_report.v1"


class AccessEnforcementRenderer:
    """Renders human-readable access enforcement reports."""

    def report(self, *, certificate: Mapping[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": REPORT_SCHEMA,
            "status": certificate.get("status"),
            "access_enforcement_certificate_id": certificate.get("access_enforcement_certificate_id"),
            "access_report_id": stable_fingerprint(certificate),
            "blockers": list(certificate.get("blockers", [])),
            "warnings": list(certificate.get("warnings", [])),
        }
        payload["markdown"] = _markdown(certificate)
        return payload


def _markdown(certificate: Mapping[str, Any]) -> str:
    lines = [
        "# Data Product Access Enforcement Report",
        "",
        f"- status: {certificate.get('status')}",
        f"- certificate_id: {certificate.get('access_enforcement_certificate_id')}",
        f"- run_id: {certificate.get('access_enforcement_run_id')}",
        f"- drift_report_id: {certificate.get('access_drift_report_id')}",
        "",
        "## Blockers",
        *[f"- {item}" for item in certificate.get("blockers", [])],
        "",
        "## Warnings",
        *[f"- {item}" for item in certificate.get("warnings", [])],
    ]
    return "\n".join(lines).rstrip() + "\n"


__all__ = ["AccessEnforcementRenderer", "REPORT_SCHEMA"]
