"""Data product evidence helpers for the migration evidence registry."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def data_product_blockers(
    *,
    bundle: Mapping[str, Any],
    slo_gate: Mapping[str, Any] | None,
    incident_report: Mapping[str, Any] | None,
    error_budget_gate: Mapping[str, Any] | None = None,
    incident_lifecycle: Mapping[str, Any] | None = None,
    release_closeout_gate: Mapping[str, Any] | None = None,
    fleet_gate: Mapping[str, Any] | None = None,
    fleet_report: Mapping[str, Any] | None = None,
    reliability_export: Mapping[str, Any] | None = None,
    route_delivery_receipt: Mapping[str, Any] | None = None,
    assertion_gate: Mapping[str, Any] | None = None,
    assertion_report: Mapping[str, Any] | None = None,
    policy_gate: Mapping[str, Any] | None = None,
    policy_report: Mapping[str, Any] | None = None,
    waiver: Mapping[str, Any] | None = None,
    authority_gate: Mapping[str, Any] | None = None,
    approval_quorum: Mapping[str, Any] | None = None,
    evidence_signature: Mapping[str, Any] | None = None,
    governance_export_payload: Mapping[str, Any] | None = None,
    governance_publish_receipt: Mapping[str, Any] | None = None,
    governance_publish_verification: Mapping[str, Any] | None = None,
    compliance_gate: Mapping[str, Any] | None = None,
    audit_package: Mapping[str, Any] | None = None,
    audit_archive_verification: Mapping[str, Any] | None = None,
    audit_retention_plan: Mapping[str, Any] | None = None,
    legal_hold: Mapping[str, Any] | None = None,
    access_classification: Mapping[str, Any] | None = None,
    entitlement_plan: Mapping[str, Any] | None = None,
    privacy_impact_assessment: Mapping[str, Any] | None = None,
    access_gate: Mapping[str, Any] | None = None,
    access_report: Mapping[str, Any] | None = None,
    access_enforcement_certificate: Mapping[str, Any] | None = None,
    access_drift_report: Mapping[str, Any] | None = None,
    extensions: Mapping[str, Mapping[str, Any] | None] | None = None,
) -> tuple[str, ...]:
    blockers: list[str] = []
    for kind, payload, allowed in (
        ("data_product_slo_gate", slo_gate, {"allowed", "warning"}),
        ("data_product_incident_report", incident_report, {"healthy", "resolved", "warning"}),
        ("data_product_error_budget_gate", error_budget_gate, {"allowed", "warning"}),
        ("data_product_incident_lifecycle", incident_lifecycle, {"healthy", "acknowledged", "resolved", "warning"}),
        ("data_product_release_closeout_gate", release_closeout_gate, {"allowed", "warning"}),
        ("data_product_fleet_gate", fleet_gate, {"allowed", "warning"}),
        ("data_product_fleet_report", fleet_report, {"healthy", "degraded", "warning", "frozen"}),
        ("data_product_reliability_export", reliability_export, {"rendered"}),
        ("data_product_route_delivery_receipt", route_delivery_receipt, {"dry_run"}),
        ("data_product_assertion_gate", assertion_gate, {"allowed", "warning"}),
        ("data_product_assertion_report", assertion_report, {"passed", "warning"}),
        ("data_product_policy_gate", policy_gate, {"allowed", "warning", "waived"}),
        ("data_product_policy_report", policy_report, {"allowed", "warning", "waived"}),
        ("data_product_waiver", waiver, {"approved"}),
        ("data_product_authority_gate", authority_gate, {"allowed", "warning"}),
        ("data_product_approval_quorum", approval_quorum, {"allowed", "warning"}),
        ("data_product_evidence_signature", evidence_signature, {"signed", "verified", "warning"}),
        ("data_product_governance_export_payload", governance_export_payload, {"rendered"}),
        ("data_product_governance_publish_receipt", governance_publish_receipt, {"dry_run", "published"}),
        ("data_product_governance_publish_verification", governance_publish_verification, {"verified", "warning"}),
        ("data_product_compliance_gate", compliance_gate, {"allowed", "warning"}),
        ("data_product_audit_package", audit_package, {"allowed", "warning"}),
        ("data_product_audit_archive_verification", audit_archive_verification, {"verified", "warning"}),
        ("data_product_audit_retention_plan", audit_retention_plan, {"ready", "warning"}),
        ("data_product_legal_hold", legal_hold, {"held"}),
        ("data_product_access_classification", access_classification, {"classified", "disabled"}),
        ("data_product_entitlement_plan", entitlement_plan, {"ready", "warning", "disabled"}),
        ("data_product_privacy_impact_assessment", privacy_impact_assessment, {"assessed", "warning", "disabled"}),
        ("data_product_access_gate", access_gate, {"allowed", "warning"}),
        ("data_product_access_report", access_report, {"allowed", "warning"}),
        ("data_product_access_enforcement_certificate", access_enforcement_certificate, {"certified", "warning"}),
        ("data_product_access_drift_report", access_drift_report, {"clean", "warning"}),
        *_extension_blocker_specs(extensions),
    ):
        if payload is not None:
            blockers.extend(_pack_bundle_blockers(kind, bundle, payload))
            if payload.get("status") not in allowed:
                blockers.append(f"evidence_registry.{kind}_blocked")
            blockers.extend(str(item) for item in payload.get("blockers", []) if str(item))
    return tuple(blockers)


def data_product_refs(
    *,
    slo_gate: Mapping[str, Any] | None,
    incident_report: Mapping[str, Any] | None,
    error_budget_gate: Mapping[str, Any] | None = None,
    incident_lifecycle: Mapping[str, Any] | None = None,
    release_closeout_gate: Mapping[str, Any] | None = None,
    fleet_gate: Mapping[str, Any] | None = None,
    fleet_report: Mapping[str, Any] | None = None,
    reliability_export: Mapping[str, Any] | None = None,
    route_delivery_receipt: Mapping[str, Any] | None = None,
    assertion_gate: Mapping[str, Any] | None = None,
    assertion_report: Mapping[str, Any] | None = None,
    policy_gate: Mapping[str, Any] | None = None,
    policy_report: Mapping[str, Any] | None = None,
    waiver: Mapping[str, Any] | None = None,
    authority_gate: Mapping[str, Any] | None = None,
    approval_quorum: Mapping[str, Any] | None = None,
    evidence_signature: Mapping[str, Any] | None = None,
    governance_export_payload: Mapping[str, Any] | None = None,
    governance_publish_receipt: Mapping[str, Any] | None = None,
    governance_publish_verification: Mapping[str, Any] | None = None,
    compliance_gate: Mapping[str, Any] | None = None,
    audit_package: Mapping[str, Any] | None = None,
    audit_archive_verification: Mapping[str, Any] | None = None,
    audit_retention_plan: Mapping[str, Any] | None = None,
    legal_hold: Mapping[str, Any] | None = None,
    access_classification: Mapping[str, Any] | None = None,
    entitlement_plan: Mapping[str, Any] | None = None,
    privacy_impact_assessment: Mapping[str, Any] | None = None,
    access_gate: Mapping[str, Any] | None = None,
    access_report: Mapping[str, Any] | None = None,
    access_enforcement_certificate: Mapping[str, Any] | None = None,
    access_drift_report: Mapping[str, Any] | None = None,
    extensions: Mapping[str, Mapping[str, Any] | None] | None = None,
) -> tuple[dict[str, Any], ...]:
    return tuple(
        item
        for item in (
            _ref("data_product_slo_gate", slo_gate, "slo_gate_id"),
            _ref("data_product_incident_report", incident_report, "incident_report_id"),
            _ref("data_product_error_budget_gate", error_budget_gate, "error_budget_gate_id"),
            _ref("data_product_incident_lifecycle", incident_lifecycle, "incident_id"),
            _ref("data_product_release_closeout_gate", release_closeout_gate, "release_closeout_gate_id"),
            _ref("data_product_fleet_gate", fleet_gate, "fleet_gate_id"),
            _ref("data_product_fleet_report", fleet_report, "fleet_report_id"),
            _ref("data_product_reliability_export", reliability_export, "export_id"),
            _ref("data_product_route_delivery_receipt", route_delivery_receipt, "route_delivery_receipt_id"),
            _ref("data_product_assertion_gate", assertion_gate, "assertion_gate_id"),
            _ref("data_product_assertion_report", assertion_report, "assertion_report_id"),
            _ref("data_product_policy_gate", policy_gate, "policy_gate_id"),
            _ref("data_product_policy_report", policy_report, "policy_report_id"),
            _ref("data_product_waiver", waiver, "waiver_id"),
            _ref("data_product_authority_gate", authority_gate, "authority_gate_id"),
            _ref("data_product_approval_quorum", approval_quorum, "approval_quorum_id"),
            _ref("data_product_evidence_signature", evidence_signature, "evidence_signature_id"),
            _ref("data_product_governance_export_payload", governance_export_payload, "payload_id"),
            _ref("data_product_governance_publish_receipt", governance_publish_receipt, "publish_receipt_id"),
            _ref(
                "data_product_governance_publish_verification",
                governance_publish_verification,
                "publish_verification_id",
            ),
            _ref("data_product_compliance_gate", compliance_gate, "compliance_gate_id"),
            _ref("data_product_audit_package", audit_package, "audit_package_id"),
            _ref(
                "data_product_audit_archive_verification",
                audit_archive_verification,
                "audit_archive_verification_id",
            ),
            _ref("data_product_audit_retention_plan", audit_retention_plan, "audit_retention_plan_id"),
            _ref("data_product_legal_hold", legal_hold, "legal_hold_id"),
            _ref("data_product_access_classification", access_classification, "access_classification_id"),
            _ref("data_product_entitlement_plan", entitlement_plan, "entitlement_plan_id"),
            _ref("data_product_privacy_impact_assessment", privacy_impact_assessment, "privacy_impact_id"),
            _ref("data_product_access_gate", access_gate, "access_gate_id"),
            _ref("data_product_access_report", access_report, "access_report_id"),
            _ref(
                "data_product_access_enforcement_certificate",
                access_enforcement_certificate,
                "access_enforcement_certificate_id",
            ),
            _ref("data_product_access_drift_report", access_drift_report, "access_drift_report_id"),
            *(_ref(kind, payload, id_key) for kind, payload, id_key in _extension_ref_specs(extensions)),
        )
        if item
    )


def _pack_bundle_blockers(kind: str, bundle: Mapping[str, Any], payload: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    if payload.get("pack_id") not in {None, bundle.get("pack_id")}:
        blockers.append(f"evidence_registry.{kind}_pack_id_mismatch")
    if payload.get("bundle_id") not in {None, bundle.get("bundle_id")}:
        blockers.append(f"evidence_registry.{kind}_bundle_id_mismatch")
    return blockers


def _ref(kind: str, payload: Mapping[str, Any] | None, id_key: str) -> dict[str, Any]:
    if not payload:
        return {}
    return {
        key: value
        for key, value in {
            "kind": kind,
            "schema_version": payload.get("schema_version"),
            "evidence_id": payload.get(id_key),
        }.items()
        if value is not None
    }


_EXTENSION_SPECS: dict[str, tuple[set[str], str]] = {
    "data_product_cost_gate": ({"allowed", "warning"}, "cost_gate_id"),
    "data_product_cost_forecast": ({"ready", "warning"}, "cost_forecast_id"),
    "data_product_cost_report": ({"allowed", "warning"}, "cost_report_id"),
    "data_product_ring_gate": ({"allowed", "warning"}, "ring_gate_id"),
    "data_product_shadow_validation": ({"passed", "warning", "disabled"}, "shadow_validation_id"),
    "data_product_rollout_promotion": ({"promoted"}, "rollout_promotion_id"),
    "data_product_rollout_report": ({"promoted", "warning"}, "rollout_report_id"),
    "data_product_trust_gate": ({"allowed", "warning"}, "trust_gate_id"),
    "data_product_trust_report": ({"allowed", "warning"}, "trust_report_id"),
    "data_product_trust_export": ({"rendered"}, "trust_export_id"),
    "data_product_remediation_gate": ({"allowed", "warning"}, "remediation_gate_id"),
    "data_product_remediation_runbook": ({"rendered", "warning"}, "remediation_runbook_id"),
    "data_product_remediation_closeout": ({"allowed", "warning"}, "remediation_closeout_id"),
    "data_product_remediation_report": ({"allowed", "warning"}, "remediation_report_id"),
    "data_product_remediation_execution_plan": (
        {"ready", "warning", "disabled"},
        "remediation_execution_plan_id",
    ),
    "data_product_remediation_execution_run": (
        {"executed", "dry_run", "disabled"},
        "remediation_execution_run_id",
    ),
    "data_product_remediation_execution_certificate": (
        {"certified", "warning"},
        "remediation_execution_certificate_id",
    ),
    "data_product_remediation_execution_report": (
        {"certified", "warning"},
        "remediation_execution_report_id",
    ),
}


def known_data_product_extensions(payloads: Mapping[str, Any]) -> dict[str, Mapping[str, Any] | None]:
    unknown = sorted(set(payloads) - set(_EXTENSION_SPECS))
    if unknown:
        raise TypeError("unexpected data product artifact keyword(s): " + ", ".join(unknown))
    return {
        key: value if isinstance(value, Mapping) else None for key, value in payloads.items() if key in _EXTENSION_SPECS
    }


def data_product_extension_values(
    extensions: Mapping[str, Mapping[str, Any] | None] | None,
) -> tuple[Mapping[str, Any], ...]:
    return tuple(payload for payload in (extensions or {}).values() if payload is not None)


def _extension_blocker_specs(
    extensions: Mapping[str, Mapping[str, Any] | None] | None,
) -> tuple[tuple[str, Mapping[str, Any] | None, set[str]], ...]:
    return tuple((kind, payload, _EXTENSION_SPECS[kind][0]) for kind, payload in (extensions or {}).items())


def _extension_ref_specs(
    extensions: Mapping[str, Mapping[str, Any] | None] | None,
) -> tuple[tuple[str, Mapping[str, Any] | None, str], ...]:
    return tuple((kind, payload, _EXTENSION_SPECS[kind][1]) for kind, payload in (extensions or {}).items())


__all__ = [
    "data_product_blockers",
    "data_product_extension_values",
    "data_product_refs",
    "known_data_product_extensions",
]
