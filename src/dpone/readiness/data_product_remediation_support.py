"""Shared contracts and helpers for data product remediation evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.readiness import data_product_compliance_support as base
from dpone.readiness.data_product_cost_policy import apply_profile, status
from dpone.readiness.data_product_trust_support import artifact_kind, domain_for_kind
from dpone.readiness.migration_control import stable_fingerprint

PLAN_SCHEMA = "dpone.data_product_remediation_plan.v1"
RUNBOOK_SCHEMA = "dpone.data_product_remediation_runbook.v1"
GATE_SCHEMA = "dpone.data_product_remediation_gate.v1"
CLOSEOUT_SCHEMA = "dpone.data_product_remediation_closeout.v1"
REPORT_SCHEMA = "dpone.data_product_remediation_report.v1"

DOMAIN_DEFAULT_KIND = {
    "contract": "schema_contract_gate",
    "quality": "data_product_assertion_gate",
    "reliability": "data_product_slo_gate",
    "governance": "data_product_policy_gate",
    "compliance": "data_product_compliance_gate",
    "access": "data_product_access_gate",
    "connection_security": "data_product_connection_rotation_gate",
    "cost": "data_product_cost_gate",
    "rollout": "data_product_ring_gate",
    "trust": "data_product_trust_gate",
}

DOMAIN_COMMANDS = {
    "contract": "dpone schema contract gate --manifest <manifest> --format json",
    "quality": "dpone data product assertions evaluate --plan <assertion-plan> --format json",
    "reliability": "dpone data product slo evaluate --plan <slo-plan> --format json",
    "governance": "dpone data product policy gate --evaluation <policy-evaluation> --format json",
    "compliance": "dpone data product compliance controls gate --evaluation <compliance-evaluation> --format json",
    "access": "dpone data product access gate --entitlement-plan <entitlement-plan> --format json",
    "connection_security": "dpone data product connection posture evaluate --inventory <inventory> --format json",
    "cost": "dpone data product cost evaluate --plan <cost-plan> --format json",
    "rollout": "dpone data product rollout ring gate --plan <rollout-plan> --format json",
    "trust": "dpone data product trust gate --snapshot <trust-snapshot> --format json",
}


@dataclass(frozen=True, slots=True)
class DataProductRemediationOptions:
    enabled: bool
    mode: str
    profile: str
    stale_evidence_policy: str
    require_trust_gate: bool
    closeout_requires_fresh_evidence: bool
    product: Mapping[str, Any]

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> DataProductRemediationOptions:
        product = base.product(manifest)
        raw = _mapping(product.get("remediation"))
        return cls(
            enabled=base.bool_value(raw.get("enabled"), False),
            mode=str(raw.get("mode") or "gate"),
            profile=str(raw.get("profile") or "prod_strict"),
            stale_evidence_policy=str(raw.get("stale_evidence_policy") or "block"),
            require_trust_gate=base.bool_value(raw.get("require_trust_gate"), True),
            closeout_requires_fresh_evidence=base.bool_value(raw.get("closeout_requires_fresh_evidence"), True),
            product=base.product_ref(product),
        )


def evidence_ref(payload: Mapping[str, Any], fallback_product: Mapping[str, Any]) -> dict[str, Any]:
    kind = artifact_kind(payload)
    product = _mapping(payload.get("product")) or fallback_product
    return {
        key: value
        for key, value in {
            "artifact_kind": kind,
            "schema_version": payload.get("schema_version"),
            "status": payload.get("status"),
            "evidence_id": _evidence_id(payload),
            "product_id": base.product_id(payload) or fallback_product.get("id"),
            "owner": product.get("owner") or fallback_product.get("owner"),
            "domain": domain_for_kind(kind),
            "blockers": list(strings(payload.get("blockers"))),
            "warnings": list(strings(payload.get("warnings"))),
        }.items()
        if value is not None
    }


def expected_statuses(kind: str) -> list[str]:
    if kind == "data_product_remediation_closeout":
        return ["allowed", "warning"]
    if kind.endswith("_report"):
        return ["allowed", "warning", "rendered"]
    if kind.endswith("_gate"):
        return ["allowed", "warning", "waived"]
    return ["allowed", "warning", "ready", "passed", "certified", "verified", "rendered"]


def payload_id(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    result = dict(payload)
    result[key] = stable_fingerprint(result)
    return result


def dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if str(value)))


def mappings(raw: Any) -> tuple[Mapping[str, Any], ...]:
    return base.mappings(raw)


def strings(raw: Any) -> tuple[str, ...]:
    return base.strings(raw)


def product_ref(raw: Mapping[str, Any]) -> dict[str, Any]:
    return base.product_ref(raw)


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


def _evidence_id(payload: Mapping[str, Any]) -> str | None:
    ignored = {"product_id", "pack_id", "bundle_id"}
    for key, value in payload.items():
        if str(key).endswith("_id") and key not in ignored and value:
            return str(value)
    return base.evidence_id(payload)


__all__ = [
    "CLOSEOUT_SCHEMA",
    "DOMAIN_COMMANDS",
    "DOMAIN_DEFAULT_KIND",
    "GATE_SCHEMA",
    "PLAN_SCHEMA",
    "REPORT_SCHEMA",
    "RUNBOOK_SCHEMA",
    "DataProductRemediationOptions",
    "apply_profile",
    "dedupe",
    "evidence_ref",
    "expected_statuses",
    "mappings",
    "payload_id",
    "product_ref",
    "status",
    "strings",
]
