"""Renderers for Data Product Trust Center artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness import data_product_trust_support as support


class TrustReportRenderer:
    """Render deterministic Trust Center reports."""

    def report(self, *, snapshot: Mapping[str, Any], gate: Mapping[str, Any] | None = None) -> dict[str, Any]:
        status = str((gate or {}).get("status") or snapshot.get("status") or "unknown")
        payload = {
            "schema_version": support.REPORT_SCHEMA,
            "status": status,
            "product": _mapping(snapshot.get("product")),
            "product_id": snapshot.get("product_id"),
            "trust_snapshot_id": snapshot.get("trust_snapshot_id"),
            "trust_gate_id": (gate or {}).get("trust_gate_id"),
            "trust_score": snapshot.get("trust_score"),
            "domains": dict(_mapping(snapshot.get("domains"))),
            "blockers": list(support.strings((gate or snapshot).get("blockers"))),
            "warnings": list(support.strings((gate or snapshot).get("warnings"))),
        }
        payload["markdown"] = _markdown(payload)
        return support.payload_id(payload, "trust_report_id")


class TrustExportRenderer:
    """Render local interop payloads without network writes."""

    def export(self, *, snapshot: Mapping[str, Any], target: str = "json") -> dict[str, Any]:
        payload = {
            "schema_version": support.EXPORT_SCHEMA,
            "status": "rendered",
            "target": target,
            "product": _mapping(snapshot.get("product")),
            "product_id": snapshot.get("product_id"),
            "trust_snapshot_id": snapshot.get("trust_snapshot_id"),
            "trust_score": snapshot.get("trust_score"),
            "network_writes": [],
            "payload": _target_payload(snapshot, target),
            "blockers": [],
            "warnings": []
            if target in {"json", "datahub", "openlineage", "opa"}
            else [f"data_product_trust.export_unknown_target:{target}"],
        }
        return support.payload_id(payload, "trust_export_id")


def _target_payload(snapshot: Mapping[str, Any], target: str) -> dict[str, Any]:
    domains = _mapping(snapshot.get("domains"))
    base = {
        "product_id": snapshot.get("product_id"),
        "trust_snapshot_id": snapshot.get("trust_snapshot_id"),
        "trust_score": snapshot.get("trust_score"),
        "domains": domains,
    }
    if target == "opa":
        return {"decision": "trust_snapshot", "input": base}
    if target == "openlineage":
        return {"facets": {"dponeTrust": base}}
    if target == "datahub":
        return {"metadataChangeProposal": {"entityType": "dataset", "aspectName": "dponeTrust", "aspect": base}}
    return base


def _markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Data Product Trust Center",
        "",
        f"- status: {payload.get('status')}",
        f"- product: {payload.get('product_id') or ''}",
        f"- trust_score: {payload.get('trust_score')}",
        "",
        "## Domain Matrix",
        "",
        "| Domain | Status | Evidence |",
        "| --- | --- | ---: |",
    ]
    for domain, state in sorted(_mapping(payload.get("domains")).items()):
        refs = state.get("evidence_refs") if isinstance(state, Mapping) else []
        lines.append(f"| {domain} | {state.get('status') if isinstance(state, Mapping) else ''} | {len(refs or [])} |")
    if payload.get("blockers"):
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- {item}" for item in payload["blockers"])
    if payload.get("warnings"):
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {item}" for item in payload["warnings"])
    return "\n".join(lines) + "\n"


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


__all__ = ["TrustExportRenderer", "TrustReportRenderer"]
