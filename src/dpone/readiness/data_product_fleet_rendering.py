"""Renderers and dry-run delivery receipts for data product fleet evidence."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

FLEET_REPORT_SCHEMA = "dpone.data_product_fleet_report.v1"
RELIABILITY_EXPORT_SCHEMA = "dpone.data_product_reliability_export.v1"
ROUTE_DELIVERY_RECEIPT_SCHEMA = "dpone.data_product_route_delivery_receipt.v1"


class ReliabilityExportRenderer:
    """Renders fleet reports and offline export artifacts."""

    def report(self, *, evaluation: Mapping[str, Any]) -> dict[str, Any]:
        payload = {
            "schema_version": FLEET_REPORT_SCHEMA,
            "status": evaluation.get("status"),
            "fleet_evaluation_id": evaluation.get("fleet_evaluation_id"),
            "markdown": _markdown(evaluation),
            "blockers": list(evaluation.get("blockers", [])),
            "warnings": list(evaluation.get("warnings", [])),
        }
        payload["fleet_report_id"] = stable_fingerprint(payload)
        return payload

    def render(self, *, evaluation: Mapping[str, Any], target: str) -> dict[str, Any]:
        target = str(target)
        content, payload = _export_payload(evaluation, target)
        result: dict[str, Any] = {
            "schema_version": RELIABILITY_EXPORT_SCHEMA,
            "status": "rendered",
            "target": target,
            "fleet_evaluation_id": evaluation.get("fleet_evaluation_id"),
            "content": content,
            "payload": payload,
            "blockers": [],
            "warnings": [],
        }
        result["export_id"] = stable_fingerprint(result)
        return result


class IncidentRouteDryRunEvaluator:
    """Validates route payloads and emits dry-run delivery receipts."""

    def evaluate(self, *, payload: Mapping[str, Any], provider: str) -> dict[str, Any]:
        blockers = _route_blockers(payload, provider)
        result: dict[str, Any] = {
            "schema_version": ROUTE_DELIVERY_RECEIPT_SCHEMA,
            "status": "blocked" if blockers else "dry_run",
            "provider": provider,
            "payload_id": payload.get("route_payload_id"),
            "incident_id": payload.get("incident_id"),
            "network_writes": [],
            "blockers": blockers,
            "warnings": [],
            "recommendations": ["Attach this dry-run receipt to incident route evidence."],
        }
        result["route_delivery_receipt_id"] = stable_fingerprint(result)
        return result


def _export_payload(evaluation: Mapping[str, Any], target: str) -> tuple[str, dict[str, Any]]:
    products = [dict(item) for item in evaluation.get("products", []) if isinstance(item, Mapping)]
    if target == "prometheus":
        lines = ["# TYPE dpone_data_product_health_status gauge"]
        for product in products:
            frozen = 1 if product.get("status") == "frozen" else 0
            labels = f'product="{product.get("id")}",owner="{product.get("owner")}",tier="{product.get("tier")}"'
            lines.append(f"dpone_data_product_health_status{{{labels}}} {0 if frozen else 1}")
            lines.append(f"dpone_data_product_release_frozen{{{labels}}} {frozen}")
        return "\n".join(lines) + "\n", {"metrics": lines}
    if target == "opentelemetry":
        payload = {"events": [{"name": "dpone.data_product.health", "attributes": product} for product in products]}
        return "", payload
    if target == "openlineage":
        return "", {"facets": {"dataProductReliability": {"status": evaluation.get("status"), "products": products}}}
    if target == "datahub":
        return "", {"entityType": "dataProduct", "status": evaluation.get("status"), "products": products}
    return "", {"evaluation": dict(evaluation)}


def _markdown(evaluation: Mapping[str, Any]) -> str:
    summary = _mapping(evaluation.get("summary"))
    lines = [
        "# Data Product Fleet Reliability",
        "",
        f"- status: {evaluation.get('status')}",
        f"- products: {summary.get('products', 0)}",
        f"- healthy: {summary.get('healthy', 0)}",
        f"- degraded: {summary.get('degraded', 0)}",
        f"- incident_active: {summary.get('incident_active', 0)}",
        f"- frozen: {summary.get('frozen', 0)}",
        "",
        "## Release Freeze Reasons",
        "",
    ]
    reasons = list(evaluation.get("release_freeze_reasons", []))
    lines.extend(f"{index}. {reason}" for index, reason in enumerate(reasons or ["No active release freeze."], start=1))
    lines.extend(
        [
            "",
            "## Owner Focus",
            "",
            "| Owner | Products | Frozen | Incidents | Action |",
            "| --- | ---: | ---: | ---: | --- |",
        ]
    )
    for owner, stats in sorted(_mapping(evaluation.get("by_owner")).items()):
        if isinstance(stats, Mapping):
            action = "Resolve blockers and rerun fleet gate" if stats.get("frozen") else "Continue"
            lines.append(
                f"| {owner} | {stats.get('products', 0)} | {stats.get('frozen', 0)} | "
                f"{stats.get('incident_active', 0)} | {action} |"
            )
    return "\n".join(lines) + "\n"


def _route_blockers(payload: Mapping[str, Any], provider: str) -> list[str]:
    allowed = {"slack", "jira", "pagerduty", "webhook"}
    blockers: list[str] = []
    if provider not in allowed:
        blockers.append(f"data_product_route_delivery.unsupported_provider:{provider}")
    if payload.get("schema_version") != "dpone.data_product_incident_route_payload.v1":
        blockers.append("data_product_route_delivery.schema_invalid")
    if not payload.get("route_payload_id"):
        blockers.append("data_product_route_delivery.payload_id_missing")
    if not payload.get("incident_id"):
        blockers.append("data_product_route_delivery.incident_id_missing")
    if payload.get("provider") not in {None, provider}:
        blockers.append("data_product_route_delivery.provider_mismatch")
    return blockers


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = [
    "FLEET_REPORT_SCHEMA",
    "RELIABILITY_EXPORT_SCHEMA",
    "ROUTE_DELIVERY_RECEIPT_SCHEMA",
    "IncidentRouteDryRunEvaluator",
    "ReliabilityExportRenderer",
]
