"""Shared helpers for data product Trust Center artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.readiness import data_product_compliance_support as base
from dpone.readiness.data_product_cost_policy import apply_profile, status
from dpone.readiness.migration_control import stable_fingerprint

INDEX_SCHEMA = "dpone.data_product_evidence_lake_index.v1"
QUERY_SCHEMA = "dpone.data_product_trust_query_result.v1"
SNAPSHOT_SCHEMA = "dpone.data_product_trust_snapshot.v1"
GATE_SCHEMA = "dpone.data_product_trust_gate.v1"
REPORT_SCHEMA = "dpone.data_product_trust_report.v1"
EXPORT_SCHEMA = "dpone.data_product_trust_export.v1"

DEFAULT_REQUIRED_DOMAINS = ("contract", "quality", "governance", "compliance", "access", "cost", "rollout")
DEFAULT_SCORE_MINIMUMS = {"advisory": 0.0, "stage": 0.7, "prod_strict": 0.85, "regulated": 0.95}

KIND_DOMAINS = {
    "schema_contract_gate": "contract",
    "schema_contract_consumer_gate": "contract",
    "data_product_assertion_gate": "quality",
    "data_product_slo_gate": "reliability",
    "data_product_error_budget_gate": "reliability",
    "data_product_policy_gate": "governance",
    "data_product_authority_gate": "governance",
    "data_product_compliance_gate": "compliance",
    "data_product_access_gate": "access",
    "data_product_privacy_impact_assessment": "access",
    "data_product_connection_rotation_gate": "connection_security",
    "data_product_secret_posture_evaluation": "connection_security",
    "data_product_cost_gate": "cost",
    "data_product_ring_gate": "rollout",
    "data_product_rollout_promotion": "rollout",
    "data_product_remediation_gate": "remediation",
    "data_product_remediation_runbook": "remediation",
    "data_product_remediation_closeout": "remediation",
    "data_product_remediation_report": "remediation",
    "data_product_remediation_execution_plan": "remediation",
    "data_product_remediation_execution_run": "remediation",
    "data_product_remediation_execution_certificate": "remediation",
    "data_product_remediation_execution_report": "remediation",
}


@dataclass(frozen=True, slots=True)
class TrustCenterOptions:
    enabled: bool
    mode: str
    profile: str
    stale_evidence_policy: str
    stale_after_seconds: int
    required_domains: tuple[str, ...]
    include_artifacts: tuple[str, ...]
    score_minimums: Mapping[str, float]
    score_weights: Mapping[str, float]
    product: Mapping[str, Any]

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> TrustCenterOptions:
        product = base.product(manifest)
        raw = _mapping(product.get("trust_center"))
        trust_score = _mapping(raw.get("trust_score"))
        return cls(
            enabled=base.bool_value(raw.get("enabled"), False),
            mode=str(raw.get("mode") or "gate"),
            profile=str(raw.get("profile") or "prod_strict"),
            stale_evidence_policy=str(raw.get("stale_evidence_policy") or "block"),
            stale_after_seconds=int(raw.get("stale_after_seconds") or 86400),
            required_domains=tuple(base.strings(raw.get("required_domains"))) or DEFAULT_REQUIRED_DOMAINS,
            include_artifacts=tuple(base.strings(raw.get("include_artifacts"))),
            score_minimums=_float_mapping(_mapping(trust_score.get("minimum"))) or DEFAULT_SCORE_MINIMUMS,
            score_weights=_float_mapping(_mapping(trust_score.get("weights"))),
            product=base.product_ref(product),
        )

    def threshold(self, profile: str) -> float:
        return float(self.score_minimums.get(profile, self.score_minimums.get(self.profile, 0.0)))


def payload_id(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    result = dict(payload)
    result[key] = stable_fingerprint(result)
    return result


def artifact_kind(payload: Mapping[str, Any]) -> str:
    explicit = payload.get("artifact_kind") or payload.get("kind")
    if explicit:
        return str(explicit)
    schema = str(payload.get("schema_version") or "")
    if schema.startswith("dpone.") and schema.endswith(".v1"):
        return schema.removeprefix("dpone.").removesuffix(".v1")
    for key in payload:
        if str(key).endswith("_id"):
            return str(key).removesuffix("_id")
    return "unknown"


def product_id(payload: Mapping[str, Any], fallback: str | None = None) -> str | None:
    return base.product_id(payload) or fallback


def evidence_id(payload: Mapping[str, Any]) -> str | None:
    return base.evidence_id(payload)


def strings(raw: Any) -> tuple[str, ...]:
    return base.strings(raw)


def mappings(raw: Any) -> tuple[Mapping[str, Any], ...]:
    return base.mappings(raw)


def domain_for_kind(kind: str) -> str:
    return KIND_DOMAINS.get(kind, "unknown")


def dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if str(value)))


def _float_mapping(raw: Mapping[str, Any]) -> dict[str, float]:
    result: dict[str, float] = {}
    for key, value in raw.items():
        try:
            result[str(key)] = float(value)
        except (TypeError, ValueError):
            continue
    return result


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


__all__ = [
    "EXPORT_SCHEMA",
    "GATE_SCHEMA",
    "INDEX_SCHEMA",
    "QUERY_SCHEMA",
    "REPORT_SCHEMA",
    "SNAPSHOT_SCHEMA",
    "TrustCenterOptions",
    "apply_profile",
    "artifact_kind",
    "dedupe",
    "domain_for_kind",
    "evidence_id",
    "mappings",
    "payload_id",
    "product_id",
    "status",
    "strings",
]
