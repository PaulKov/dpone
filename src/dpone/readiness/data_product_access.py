"""Provider-neutral access classification, entitlement and privacy gates."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from dpone.readiness import data_product_access_support as access_support
from dpone.readiness import data_product_compliance_support as support
from dpone.readiness.migration_control import stable_fingerprint

ACCESS_CLASSIFICATION_SCHEMA = "dpone.data_product_access_classification.v1"
ENTITLEMENT_PLAN_SCHEMA = "dpone.data_product_entitlement_plan.v1"
PRIVACY_IMPACT_SCHEMA = "dpone.data_product_privacy_impact_assessment.v1"
ACCESS_GATE_SCHEMA = "dpone.data_product_access_gate.v1"


@dataclass(frozen=True, slots=True)
class DataProductAccessOptions:
    enabled: bool
    mode: str
    profile: str
    unknown_consumer: str
    stale_evidence_policy: str
    default_class: str
    classification: Mapping[str, Any]
    entitlements: tuple[Mapping[str, Any], ...]
    privacy: Mapping[str, Any]

    @classmethod
    def from_product(cls, product: Mapping[str, Any]) -> DataProductAccessOptions:
        raw = product.get("access_governance")
        options = raw if isinstance(raw, Mapping) else {}
        classification = _mapping(options.get("classification"))
        return cls(
            enabled=support.bool_value(options.get("enabled"), False),
            mode=str(options.get("mode") or "gate"),
            profile=str(options.get("profile") or "prod_strict"),
            unknown_consumer=str(options.get("unknown_consumer") or "warn"),
            stale_evidence_policy=str(options.get("stale_evidence_policy") or "block"),
            default_class=str(classification.get("default") or "internal"),
            classification=classification,
            entitlements=support.mappings(options.get("entitlements")),
            privacy=_mapping(options.get("privacy")),
        )


class AccessClassificationBuilder:
    """Builds normalized dataset/column classifications."""

    def build(
        self,
        *,
        manifest: Mapping[str, Any],
        schema_contract: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        product = support.product(manifest)
        options = DataProductAccessOptions.from_product(product)
        if not options.enabled:
            return access_support.classification_payload(
                schema=ACCESS_CLASSIFICATION_SCHEMA,
                product=product,
                options=options,
                columns=(),
                status="disabled",
            )
        columns = tuple(
            access_support.column_classification(name, options)
            for name in sorted(
                access_support.column_names(manifest=manifest, schema_contract=schema_contract or {}, options=options)
            )
        )
        return access_support.classification_payload(
            schema=ACCESS_CLASSIFICATION_SCHEMA,
            product=product,
            options=options,
            columns=columns,
            status="classified",
        )


class EntitlementPlanBuilder:
    """Joins classifications, consumer reads and declared entitlements."""

    def build(
        self,
        *,
        manifest: Mapping[str, Any],
        classification: Mapping[str, Any],
        consumer_matrix: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        product = support.product(manifest)
        options = DataProductAccessOptions.from_product(product)
        if not options.enabled or classification.get("status") == "disabled":
            return access_support.entitlement_payload(
                schema=ENTITLEMENT_PLAN_SCHEMA,
                product=product,
                classification=classification,
                consumer_matrix=consumer_matrix,
                decisions=(),
                status="disabled",
                blockers=(),
            )
        class_map = access_support.classification_map(classification)
        entitlements = tuple(access_support.normalize_entitlement(item) for item in options.entitlements)
        decisions = tuple(
            access_support.decision(consumer, column, class_map, entitlements, options)
            for consumer, column in access_support.consumer_reads(consumer_matrix, entitlements)
        )
        blockers = tuple(item for decision in decisions for item in decision.get("blockers", []))
        warnings = tuple(item for decision in decisions for item in decision.get("warnings", []))
        status = "blocked" if blockers else "warning" if warnings else "ready"
        return access_support.entitlement_payload(
            schema=ENTITLEMENT_PLAN_SCHEMA,
            product=product,
            classification=classification,
            consumer_matrix=consumer_matrix,
            decisions=decisions,
            status=status,
            blockers=blockers,
            warnings=warnings,
        )


class PrivacyImpactAssessor:
    """Evaluates purpose, lawful-basis and authority evidence for sensitive access."""

    def assess(
        self,
        *,
        manifest: Mapping[str, Any],
        entitlement_plan: Mapping[str, Any],
        authority_gate: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        product = support.product(manifest)
        options = DataProductAccessOptions.from_product(product)
        if not options.enabled or entitlement_plan.get("status") == "disabled":
            return access_support.privacy_payload(
                schema=PRIVACY_IMPACT_SCHEMA,
                product=product,
                entitlement_plan=entitlement_plan,
                authority_gate=authority_gate,
                sensitive=(),
                status="disabled",
                blockers=(),
                warnings=(),
            )
        blockers: list[str] = []
        warnings: list[str] = []
        sensitive = [item for item in support.mappings(entitlement_plan.get("decisions")) if item.get("sensitive")]
        for decision in sensitive:
            blockers.extend(access_support.purpose_blockers(decision))
            blockers.extend(access_support.lawful_basis_blockers(decision, options))
            blockers.extend(access_support.approval_blockers(decision, options, authority_gate))
        if authority_gate:
            warnings.extend(str(item) for item in authority_gate.get("warnings", []) if str(item))
        status = "blocked" if blockers else "warning" if warnings else "assessed"
        return access_support.privacy_payload(
            schema=PRIVACY_IMPACT_SCHEMA,
            product=product,
            entitlement_plan=entitlement_plan,
            authority_gate=authority_gate,
            sensitive=sensitive,
            status=status,
            blockers=blockers,
            warnings=warnings,
        )


class AccessGateEvaluator:
    """Profile-aware final access governance decision."""

    def evaluate(
        self,
        *,
        entitlement_plan: Mapping[str, Any],
        privacy_impact: Mapping[str, Any],
        profile: str = "prod_strict",
    ) -> dict[str, Any]:
        blockers = [
            *support.strings(entitlement_plan.get("blockers")),
            *support.strings(privacy_impact.get("blockers")),
        ]
        warnings = [
            *support.strings(entitlement_plan.get("warnings")),
            *support.strings(privacy_impact.get("warnings")),
        ]
        blockers = access_support.profile_blockers(blockers, profile)
        if profile == "advisory":
            warnings = [*warnings, *blockers]
            blockers = []
        product = access_support.product_from_payload(entitlement_plan) or access_support.product_from_payload(
            privacy_impact
        )
        if profile == "regulated" and product and not product.get("owner"):
            blockers.append("data_product_access.product_owner_missing")
        status = "blocked" if blockers else "warning" if warnings else "allowed"
        payload: dict[str, Any] = {
            "schema_version": ACCESS_GATE_SCHEMA,
            "status": status,
            "profile": profile,
            "product": product,
            "product_id": product.get("id") if product else None,
            "entitlement_plan_id": entitlement_plan.get("entitlement_plan_id"),
            "privacy_impact_id": privacy_impact.get("privacy_impact_id"),
            "pack_id": entitlement_plan.get("pack_id") or privacy_impact.get("pack_id"),
            "bundle_id": entitlement_plan.get("bundle_id") or privacy_impact.get("bundle_id"),
            "summary": access_support.gate_summary(entitlement_plan, privacy_impact),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": access_support.recommendations(status),
        }
        payload["access_gate_id"] = stable_fingerprint(payload)
        return payload


class AccessGovernanceRenderer:
    """Compatibility wrapper for dedicated rendering module."""

    def report(self, *, gate: Mapping[str, Any]) -> dict[str, Any]:
        renderer = import_module("dpone.readiness.data_product_access_rendering")
        return renderer.AccessGovernanceRenderer().report(gate=gate)


def _mapping(raw: Any) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = [
    "ACCESS_CLASSIFICATION_SCHEMA",
    "ACCESS_GATE_SCHEMA",
    "ENTITLEMENT_PLAN_SCHEMA",
    "PRIVACY_IMPACT_SCHEMA",
    "AccessClassificationBuilder",
    "AccessGateEvaluator",
    "AccessGovernanceRenderer",
    "DataProductAccessOptions",
    "EntitlementPlanBuilder",
    "PrivacyImpactAssessor",
]
