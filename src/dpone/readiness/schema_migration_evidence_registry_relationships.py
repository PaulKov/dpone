"""Relationship checks and artifact refs for migration evidence registry records."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from dpone.readiness.schema_migration_evidence_registry_constants import STAGES
from dpone.readiness.schema_migration_evidence_registry_data_product import (
    data_product_blockers,
    data_product_refs,
)


@dataclass(frozen=True, slots=True)
class MigrationEvidenceArtifactRef:
    kind: str
    schema_version: str | None = None
    path: str | None = None
    sha256: str | None = None
    pack_id: str | None = None
    evidence_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value is not None}


def relationship_blockers(
    *,
    bundle: Mapping[str, Any],
    verification: Mapping[str, Any],
    gate: Mapping[str, Any] | None,
    trust: Mapping[str, Any] | None,
    diff: Mapping[str, Any] | None,
    contract_gate: Mapping[str, Any] | None,
    consumer_gate: Mapping[str, Any] | None,
    consumer_certification: Mapping[str, Any] | None,
    compatibility_view_gate: Mapping[str, Any] | None,
    contract_retirement_gate: Mapping[str, Any] | None,
    data_product_slo_gate: Mapping[str, Any] | None,
    data_product_incident_report: Mapping[str, Any] | None,
    data_product_error_budget_gate: Mapping[str, Any] | None,
    data_product_incident_lifecycle: Mapping[str, Any] | None,
    data_product_release_closeout_gate: Mapping[str, Any] | None,
    data_product_fleet_gate: Mapping[str, Any] | None,
    data_product_fleet_report: Mapping[str, Any] | None,
    data_product_reliability_export: Mapping[str, Any] | None,
    data_product_route_delivery_receipt: Mapping[str, Any] | None,
    data_product_assertion_gate: Mapping[str, Any] | None,
    data_product_assertion_report: Mapping[str, Any] | None,
    data_product_policy_gate: Mapping[str, Any] | None,
    data_product_policy_report: Mapping[str, Any] | None,
    data_product_waiver: Mapping[str, Any] | None,
    data_product_authority_gate: Mapping[str, Any] | None,
    data_product_approval_quorum: Mapping[str, Any] | None,
    data_product_evidence_signature: Mapping[str, Any] | None,
    data_product_governance_export_payload: Mapping[str, Any] | None,
    data_product_governance_publish_receipt: Mapping[str, Any] | None,
    data_product_governance_publish_verification: Mapping[str, Any] | None,
    data_product_compliance_gate: Mapping[str, Any] | None,
    data_product_audit_package: Mapping[str, Any] | None,
    data_product_audit_archive_verification: Mapping[str, Any] | None,
    data_product_audit_retention_plan: Mapping[str, Any] | None,
    data_product_legal_hold: Mapping[str, Any] | None,
    data_product_access_classification: Mapping[str, Any] | None,
    data_product_entitlement_plan: Mapping[str, Any] | None,
    data_product_privacy_impact_assessment: Mapping[str, Any] | None,
    data_product_access_gate: Mapping[str, Any] | None,
    data_product_access_report: Mapping[str, Any] | None,
    data_product_access_enforcement_certificate: Mapping[str, Any] | None,
    data_product_access_drift_report: Mapping[str, Any] | None,
    data_product_extensions: Mapping[str, Mapping[str, Any] | None] | None = None,
    post_apply_certificate: Mapping[str, Any] | None,
    watch_certificate: Mapping[str, Any] | None,
    remediation_certificate: Mapping[str, Any] | None,
    backup_certificate: Mapping[str, Any] | None,
    recovery_point: Mapping[str, Any] | None,
    recovery_chain_verification: Mapping[str, Any] | None,
    stage: str,
) -> tuple[str, ...]:
    blockers: list[str] = []
    if stage not in STAGES:
        blockers.append(f"evidence_registry.stage_invalid:{stage}")
    if verification.get("status") == "blocked":
        blockers.append("evidence_registry.bundle_verification_blocked")
    blockers.extend(str(item) for item in verification.get("blockers", []) if str(item))
    blockers.extend(_pack_bundle_blockers(bundle, gate, "gate"))
    blockers.extend(_pack_bundle_blockers(bundle, trust, "trust"))
    blockers.extend(_diff_blockers(bundle, diff))
    blockers.extend(_gate_blockers(bundle, contract_gate, "contract_gate"))
    blockers.extend(_gate_blockers(bundle, consumer_gate, "consumer_gate"))
    blockers.extend(_status_blockers(bundle, consumer_certification, "consumer_certification", "certified"))
    blockers.extend(_gate_blockers(bundle, compatibility_view_gate, "compatibility_view_gate"))
    blockers.extend(_gate_blockers(bundle, contract_retirement_gate, "contract_retirement_gate"))
    blockers.extend(
        data_product_blockers(
            bundle=bundle,
            slo_gate=data_product_slo_gate,
            incident_report=data_product_incident_report,
            error_budget_gate=data_product_error_budget_gate,
            incident_lifecycle=data_product_incident_lifecycle,
            release_closeout_gate=data_product_release_closeout_gate,
            fleet_gate=data_product_fleet_gate,
            fleet_report=data_product_fleet_report,
            reliability_export=data_product_reliability_export,
            route_delivery_receipt=data_product_route_delivery_receipt,
            assertion_gate=data_product_assertion_gate,
            assertion_report=data_product_assertion_report,
            policy_gate=data_product_policy_gate,
            policy_report=data_product_policy_report,
            waiver=data_product_waiver,
            authority_gate=data_product_authority_gate,
            approval_quorum=data_product_approval_quorum,
            evidence_signature=data_product_evidence_signature,
            governance_export_payload=data_product_governance_export_payload,
            governance_publish_receipt=data_product_governance_publish_receipt,
            governance_publish_verification=data_product_governance_publish_verification,
            compliance_gate=data_product_compliance_gate,
            audit_package=data_product_audit_package,
            audit_archive_verification=data_product_audit_archive_verification,
            audit_retention_plan=data_product_audit_retention_plan,
            legal_hold=data_product_legal_hold,
            access_classification=data_product_access_classification,
            entitlement_plan=data_product_entitlement_plan,
            privacy_impact_assessment=data_product_privacy_impact_assessment,
            access_gate=data_product_access_gate,
            access_report=data_product_access_report,
            access_enforcement_certificate=data_product_access_enforcement_certificate,
            access_drift_report=data_product_access_drift_report,
            extensions=data_product_extensions,
        )
    )
    blockers.extend(
        _status_blockers(bundle, post_apply_certificate, "post_apply_certificate", "verified", check_bundle=True)
    )
    blockers.extend(_status_blockers(bundle, watch_certificate, "watch_certificate", {"stable", "warning"}))
    blockers.extend(_status_blockers(bundle, remediation_certificate, "remediation_certificate", "certified"))
    blockers.extend(_status_blockers(bundle, backup_certificate, "backup_certificate", "certified"))
    blockers.extend(_status_blockers(bundle, recovery_point, "recovery_point", "usable"))
    blockers.extend(_recovery_chain_blockers(recovery_point, recovery_chain_verification))
    if bundle.get("status") == "blocked":
        blockers.extend(str(item) for item in bundle.get("blockers", []) if str(item))
    return tuple(blockers)


def relationship_warnings(*payloads: Mapping[str, Any] | None) -> tuple[str, ...]:
    warnings: list[str] = []
    for payload in payloads:
        if payload is not None:
            warnings.extend(str(item) for item in payload.get("warnings", []) if str(item))
    return tuple(warnings)


def registry_status(bundle: Mapping[str, Any], gate: Mapping[str, Any] | None, trust: Mapping[str, Any] | None) -> str:
    if gate and gate.get("status") == "warning":
        return "warning"
    if trust and trust.get("status") == "warning":
        return "warning"
    return "warning" if bundle.get("status") == "warning" else "ready"


def artifact_refs(
    *,
    bundle: Mapping[str, Any],
    gate: Mapping[str, Any] | None,
    trust: Mapping[str, Any] | None,
    diff: Mapping[str, Any] | None,
    contract_gate: Mapping[str, Any] | None,
    consumer_gate: Mapping[str, Any] | None,
    consumer_certification: Mapping[str, Any] | None,
    compatibility_view_gate: Mapping[str, Any] | None,
    contract_retirement_gate: Mapping[str, Any] | None,
    data_product_slo_gate: Mapping[str, Any] | None,
    data_product_incident_report: Mapping[str, Any] | None,
    data_product_error_budget_gate: Mapping[str, Any] | None,
    data_product_incident_lifecycle: Mapping[str, Any] | None,
    data_product_release_closeout_gate: Mapping[str, Any] | None,
    data_product_fleet_gate: Mapping[str, Any] | None,
    data_product_fleet_report: Mapping[str, Any] | None,
    data_product_reliability_export: Mapping[str, Any] | None,
    data_product_route_delivery_receipt: Mapping[str, Any] | None,
    data_product_assertion_gate: Mapping[str, Any] | None,
    data_product_assertion_report: Mapping[str, Any] | None,
    data_product_policy_gate: Mapping[str, Any] | None,
    data_product_policy_report: Mapping[str, Any] | None,
    data_product_waiver: Mapping[str, Any] | None,
    data_product_authority_gate: Mapping[str, Any] | None,
    data_product_approval_quorum: Mapping[str, Any] | None,
    data_product_evidence_signature: Mapping[str, Any] | None,
    data_product_governance_export_payload: Mapping[str, Any] | None,
    data_product_governance_publish_receipt: Mapping[str, Any] | None,
    data_product_governance_publish_verification: Mapping[str, Any] | None,
    data_product_compliance_gate: Mapping[str, Any] | None,
    data_product_audit_package: Mapping[str, Any] | None,
    data_product_audit_archive_verification: Mapping[str, Any] | None,
    data_product_audit_retention_plan: Mapping[str, Any] | None,
    data_product_legal_hold: Mapping[str, Any] | None,
    data_product_access_classification: Mapping[str, Any] | None,
    data_product_entitlement_plan: Mapping[str, Any] | None,
    data_product_privacy_impact_assessment: Mapping[str, Any] | None,
    data_product_access_gate: Mapping[str, Any] | None,
    data_product_access_report: Mapping[str, Any] | None,
    data_product_access_enforcement_certificate: Mapping[str, Any] | None,
    data_product_access_drift_report: Mapping[str, Any] | None,
    data_product_extensions: Mapping[str, Mapping[str, Any] | None] | None = None,
    post_apply_certificate: Mapping[str, Any] | None,
    watch_certificate: Mapping[str, Any] | None,
    remediation_certificate: Mapping[str, Any] | None,
    backup_certificate: Mapping[str, Any] | None,
    recovery_point: Mapping[str, Any] | None,
    recovery_chain_verification: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    refs = [dict(item) for item in bundle.get("artifacts", []) if isinstance(item, Mapping)]
    for kind, payload, id_key in (
        ("bundle_gate", gate, "gate_id"),
        ("trust_verification", trust, "trust_verification_id"),
        ("bundle_diff", diff, "diff_id"),
        ("contract_gate", contract_gate, "contract_gate_id"),
        ("consumer_gate", consumer_gate, "consumer_gate_id"),
        ("consumer_certification", consumer_certification, "consumer_certification_id"),
        ("compatibility_view_gate", compatibility_view_gate, "compatibility_view_gate_id"),
        ("contract_retirement_gate", contract_retirement_gate, "retirement_gate_id"),
        ("post_apply_certificate", post_apply_certificate, "certificate_id"),
        ("watch_certificate", watch_certificate, "certificate_id"),
        ("remediation_certificate", remediation_certificate, "certificate_id"),
        ("backup_certificate", backup_certificate, "certificate_id"),
        ("recovery_point", recovery_point, "restore_point_id"),
        ("recovery_chain_verification", recovery_chain_verification, "chain_verification_id"),
    ):
        if payload:
            refs.append(_ref(kind, payload, id_key))
    refs.extend(
        data_product_refs(
            slo_gate=data_product_slo_gate,
            incident_report=data_product_incident_report,
            error_budget_gate=data_product_error_budget_gate,
            incident_lifecycle=data_product_incident_lifecycle,
            release_closeout_gate=data_product_release_closeout_gate,
            fleet_gate=data_product_fleet_gate,
            fleet_report=data_product_fleet_report,
            reliability_export=data_product_reliability_export,
            route_delivery_receipt=data_product_route_delivery_receipt,
            assertion_gate=data_product_assertion_gate,
            assertion_report=data_product_assertion_report,
            policy_gate=data_product_policy_gate,
            policy_report=data_product_policy_report,
            waiver=data_product_waiver,
            authority_gate=data_product_authority_gate,
            approval_quorum=data_product_approval_quorum,
            evidence_signature=data_product_evidence_signature,
            governance_export_payload=data_product_governance_export_payload,
            governance_publish_receipt=data_product_governance_publish_receipt,
            governance_publish_verification=data_product_governance_publish_verification,
            compliance_gate=data_product_compliance_gate,
            audit_package=data_product_audit_package,
            audit_archive_verification=data_product_audit_archive_verification,
            audit_retention_plan=data_product_audit_retention_plan,
            legal_hold=data_product_legal_hold,
            access_classification=data_product_access_classification,
            entitlement_plan=data_product_entitlement_plan,
            privacy_impact_assessment=data_product_privacy_impact_assessment,
            access_gate=data_product_access_gate,
            access_report=data_product_access_report,
            access_enforcement_certificate=data_product_access_enforcement_certificate,
            access_drift_report=data_product_access_drift_report,
            extensions=data_product_extensions,
        )
    )
    return refs


def _pack_bundle_blockers(bundle: Mapping[str, Any], payload: Mapping[str, Any] | None, prefix: str) -> list[str]:
    if payload is None:
        return []
    blockers: list[str] = []
    if payload.get("pack_id") != bundle.get("pack_id"):
        blockers.append(f"evidence_registry.{prefix}_pack_id_mismatch")
    if payload.get("bundle_id") != bundle.get("bundle_id"):
        blockers.append(f"evidence_registry.{prefix}_bundle_id_mismatch")
    blockers.extend(str(item) for item in payload.get("blockers", []) if str(item))
    return blockers


def _diff_blockers(bundle: Mapping[str, Any], diff: Mapping[str, Any] | None) -> list[str]:
    if diff is None:
        return []
    blockers: list[str] = []
    if diff.get("head_pack_id") and diff.get("head_pack_id") != bundle.get("pack_id"):
        blockers.append("evidence_registry.diff_pack_id_mismatch")
    if diff.get("head_bundle_id") and diff.get("head_bundle_id") != bundle.get("bundle_id"):
        blockers.append("evidence_registry.diff_bundle_id_mismatch")
    blockers.extend(str(item) for item in diff.get("blockers", []) if str(item))
    return blockers


def _gate_blockers(bundle: Mapping[str, Any], payload: Mapping[str, Any] | None, kind: str) -> list[str]:
    if payload is None:
        return []
    blockers: list[str] = []
    if payload.get("pack_id") not in {None, bundle.get("pack_id")}:
        blockers.append(f"evidence_registry.{kind}_pack_id_mismatch")
    if payload.get("status") not in {"allowed", "warning"}:
        blockers.append(f"evidence_registry.{kind}_blocked")
    blockers.extend(str(item) for item in payload.get("blockers", []) if str(item))
    return blockers


def _status_blockers(
    bundle: Mapping[str, Any],
    payload: Mapping[str, Any] | None,
    kind: str,
    allowed_status: str | set[str],
    *,
    check_bundle: bool = False,
) -> list[str]:
    if payload is None:
        return []
    blockers: list[str] = []
    if payload.get("pack_id") != bundle.get("pack_id"):
        blockers.append(f"evidence_registry.{kind}_pack_id_mismatch")
    if check_bundle and payload.get("bundle_id") not in {None, bundle.get("bundle_id")}:
        blockers.append(f"evidence_registry.{kind}_bundle_id_mismatch")
    statuses = {allowed_status} if isinstance(allowed_status, str) else allowed_status
    if payload.get("status") not in statuses:
        blockers.append(f"evidence_registry.{kind}_blocked")
    blockers.extend(str(item) for item in payload.get("blockers", []) if str(item))
    return blockers


def _recovery_chain_blockers(
    recovery_point: Mapping[str, Any] | None,
    recovery_chain_verification: Mapping[str, Any] | None,
) -> list[str]:
    if recovery_chain_verification is None:
        return []
    blockers: list[str] = []
    if recovery_chain_verification.get("status") != "verified":
        blockers.append("evidence_registry.recovery_chain_verification_blocked")
    if recovery_point is not None and recovery_chain_verification.get("restore_point_id") != recovery_point.get(
        "restore_point_id"
    ):
        blockers.append("evidence_registry.recovery_point_chain_mismatch")
    blockers.extend(str(item) for item in recovery_chain_verification.get("blockers", []) if str(item))
    return blockers


def _ref(kind: str, payload: Mapping[str, Any], id_key: str) -> dict[str, Any]:
    return MigrationEvidenceArtifactRef(
        kind=kind,
        schema_version=str(payload.get("schema_version")) if payload.get("schema_version") else None,
        evidence_id=str(payload.get(id_key)) if payload.get(id_key) else None,
    ).to_dict()


__all__ = [
    "MigrationEvidenceArtifactRef",
    "artifact_refs",
    "registry_status",
    "relationship_blockers",
    "relationship_warnings",
]
