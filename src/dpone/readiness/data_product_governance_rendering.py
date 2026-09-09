"""Provider-neutral governance export payload renderers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.readiness.data_product_governance_constants import SUPPORTED_GOVERNANCE_PROVIDERS
from dpone.readiness.migration_control import stable_fingerprint

GOVERNANCE_PAYLOAD_SCHEMA = "dpone.data_product_governance_export_payload.v1"


class GovernancePayloadRenderer:
    """Renders catalog/observability payloads without performing network writes."""

    def render(self, *, plan: Mapping[str, Any], provider: str) -> dict[str, Any]:
        normalized = provider.lower().strip()
        blockers = _provider_blockers(plan, normalized)
        payload: dict[str, Any] = {
            "schema_version": GOVERNANCE_PAYLOAD_SCHEMA,
            "status": "blocked" if blockers else "rendered",
            "provider": normalized,
            "product_id": _product_id(plan),
            "pack_id": plan.get("pack_id"),
            "bundle_id": plan.get("bundle_id"),
            "plan_id": plan.get("governance_export_plan_id"),
            "payload": {} if blockers else _provider_payload(plan, normalized),
            "evidence_refs": list(plan.get("evidence_refs", [])),
            "blockers": blockers,
            "warnings": list(plan.get("warnings", [])),
        }
        payload["payload_sha256"] = stable_fingerprint(payload["payload"])
        payload["payload_id"] = stable_fingerprint(
            {
                "provider": normalized,
                "product_id": payload["product_id"],
                "plan_id": payload["plan_id"],
                "payload_sha256": payload["payload_sha256"],
            }
        )
        return payload


def _provider_blockers(plan: Mapping[str, Any], provider: str) -> list[str]:
    blockers = [str(item) for item in plan.get("blockers", []) if str(item)]
    if provider not in SUPPORTED_GOVERNANCE_PROVIDERS:
        blockers.append(f"data_product_governance.unsupported_provider:{provider}")
    if plan.get("status") == "disabled":
        blockers.append("data_product_governance.plan_disabled")
    return list(dict.fromkeys(blockers))


def _provider_payload(plan: Mapping[str, Any], provider: str) -> dict[str, Any]:
    product = _product(plan)
    evidence = list(plan.get("evidence_refs", []))
    if provider == "datahub":
        return {
            "entityType": "dataProduct",
            "urn": f"urn:li:dataProduct:{product.get('id')}",
            "aspects": {"dponeGovernanceEvidence": _common_payload(plan, evidence)},
        }
    if provider == "openmetadata":
        return {
            "entityType": "table",
            "fullyQualifiedName": product.get("id"),
            "extension": {"dponeGovernanceEvidence": _common_payload(plan, evidence)},
        }
    if provider == "openlineage":
        return {
            "eventType": "COMPLETE",
            "job": {"namespace": "dpone", "name": f"governance_export.{product.get('id')}"},
            "run": {"runId": str(plan.get("governance_export_plan_id"))},
            "outputs": [{"namespace": "dpone", "name": str(product.get("id")), "facets": _lineage_facets(plan)}],
        }
    if provider == "opa":
        return {
            "decision_id": str(plan.get("governance_export_plan_id")),
            "labels": {"product_id": str(product.get("id")), "source": "dpone"},
            "input": _common_payload(plan, evidence),
            "result": {"status": plan.get("status")},
        }
    return {"product": product, "governance": _common_payload(plan, evidence)}


def _lineage_facets(plan: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "dpone_governance": {
            "_producer": "https://dpone.dev",
            "_schemaURL": "https://dpone.dev/schemas/data-product/governance-export-payload",
            "status": plan.get("status"),
            "evidenceRefs": list(plan.get("evidence_refs", [])),
        }
    }


def _common_payload(plan: Mapping[str, Any], evidence: list[Any]) -> dict[str, Any]:
    return {
        "product": _product(plan),
        "contract": plan.get("contract", {}),
        "status": plan.get("status"),
        "profile": plan.get("profile"),
        "evidenceRefs": evidence,
        "registrySummary": plan.get("registry_summary", {}),
    }


def _product(plan: Mapping[str, Any]) -> dict[str, Any]:
    raw = plan.get("product")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _product_id(plan: Mapping[str, Any]) -> str | None:
    product = _product(plan)
    return str(product.get("id")) if product.get("id") else None


__all__ = ["GOVERNANCE_PAYLOAD_SCHEMA", "GovernancePayloadRenderer"]
