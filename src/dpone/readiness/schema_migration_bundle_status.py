"""Status matrix checks for schema migration bundle relationships."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from dpone.readiness.schema_migration_bundle_cost import COST_GOVERNANCE_STATUS_CHECKS
from dpone.readiness.schema_migration_bundle_remediation import REMEDIATION_STATUS_CHECKS
from dpone.readiness.schema_migration_bundle_rollout import ROLLOUT_STATUS_CHECKS
from dpone.readiness.schema_migration_bundle_trust_center import TRUST_CENTER_STATUS_CHECKS


class BundleStatusArtifact(Protocol):
    payload: dict[str, Any]


STATUS_CHECKS: tuple[tuple[str, set[str | None], str], ...] = (
    ("certification", {None, "certified"}, "migration_bundle.certification_blocked"),
    ("rehearsal_certificate", {"certified"}, "migration_bundle.rehearsal_certificate_blocked"),
    ("post_apply_certificate", {"verified"}, "migration_bundle.post_apply_certificate_blocked"),
    ("watch_certificate", {"stable", "warning"}, "migration_bundle.watch_certificate_blocked"),
    ("remediation_certificate", {"certified"}, "migration_bundle.remediation_certificate_blocked"),
    ("backup_certificate", {"certified"}, "migration_bundle.backup_certificate_blocked"),
    ("contract_gate", {"allowed", "warning"}, "migration_bundle.contract_gate_blocked"),
    ("consumer_gate", {"allowed", "warning"}, "migration_bundle.consumer_gate_blocked"),
    ("consumer_test_kit", {"ready"}, "migration_bundle.consumer_test_kit_blocked"),
    ("consumer_certification", {"certified"}, "migration_bundle.consumer_certification_blocked"),
    ("compatibility_view_gate", {"allowed", "warning"}, "migration_bundle.compatibility_view_gate_blocked"),
    ("contract_adoption_status", {"migrated", "empty"}, "migration_bundle.contract_adoption_status_blocked"),
    ("contract_retirement_gate", {"allowed", "warning"}, "migration_bundle.contract_retirement_gate_blocked"),
    ("data_product_slo_gate", {"allowed", "warning"}, "migration_bundle.data_product_slo_gate_blocked"),
    (
        "data_product_incident_report",
        {"healthy", "resolved", "warning"},
        "migration_bundle.data_product_incident_report_blocked",
    ),
    (
        "data_product_error_budget_gate",
        {"allowed", "warning"},
        "migration_bundle.data_product_error_budget_gate_blocked",
    ),
    (
        "data_product_incident_lifecycle",
        {"healthy", "acknowledged", "resolved", "warning"},
        "migration_bundle.data_product_incident_lifecycle_blocked",
    ),
    (
        "data_product_release_closeout_gate",
        {"allowed", "warning"},
        "migration_bundle.data_product_release_closeout_gate_blocked",
    ),
    ("data_product_fleet_gate", {"allowed", "warning"}, "migration_bundle.data_product_fleet_gate_blocked"),
    (
        "data_product_fleet_report",
        {"healthy", "degraded", "warning", "frozen"},
        "migration_bundle.data_product_fleet_report_blocked",
    ),
    ("data_product_reliability_export", {"rendered"}, "migration_bundle.data_product_reliability_export_blocked"),
    (
        "data_product_route_delivery_receipt",
        {"dry_run"},
        "migration_bundle.data_product_route_delivery_receipt_blocked",
    ),
    ("data_product_assertion_gate", {"allowed", "warning"}, "migration_bundle.data_product_assertion_gate_blocked"),
    ("data_product_assertion_report", {"passed", "warning"}, "migration_bundle.data_product_assertion_report_blocked"),
    (
        "data_product_policy_gate",
        {"allowed", "warning", "waived"},
        "migration_bundle.data_product_policy_gate_blocked",
    ),
    (
        "data_product_policy_report",
        {"allowed", "warning", "waived"},
        "migration_bundle.data_product_policy_report_blocked",
    ),
    ("data_product_waiver", {"approved"}, "migration_bundle.data_product_waiver_blocked"),
    ("data_product_authority_gate", {"allowed", "warning"}, "migration_bundle.data_product_authority_gate_blocked"),
    ("data_product_approval_quorum", {"allowed", "warning"}, "migration_bundle.data_product_approval_quorum_blocked"),
    (
        "data_product_evidence_signature",
        {"signed", "verified", "warning"},
        "migration_bundle.data_product_evidence_signature_blocked",
    ),
    (
        "data_product_governance_export_payload",
        {"rendered"},
        "migration_bundle.data_product_governance_export_payload_blocked",
    ),
    (
        "data_product_governance_publish_receipt",
        {"dry_run", "published"},
        "migration_bundle.data_product_governance_publish_receipt_blocked",
    ),
    (
        "data_product_governance_publish_verification",
        {"verified", "warning"},
        "migration_bundle.data_product_governance_publish_verification_blocked",
    ),
    (
        "data_product_audit_archive_verification",
        {"verified", "warning"},
        "migration_bundle.data_product_audit_archive_verification_blocked",
    ),
    (
        "data_product_audit_retention_plan",
        {"ready", "warning"},
        "migration_bundle.data_product_audit_retention_plan_blocked",
    ),
    ("data_product_legal_hold", {"held"}, "migration_bundle.data_product_legal_hold_blocked"),
    ("data_product_access_gate", {"allowed", "warning"}, "migration_bundle.data_product_access_gate_blocked"),
    ("data_product_access_report", {"allowed", "warning"}, "migration_bundle.data_product_access_report_blocked"),
    (
        "data_product_privacy_impact_assessment",
        {"assessed", "warning"},
        "migration_bundle.data_product_privacy_impact_assessment_blocked",
    ),
    (
        "data_product_access_enforcement_certificate",
        {"certified", "warning"},
        "migration_bundle.data_product_access_enforcement_certificate_blocked",
    ),
    (
        "data_product_access_drift_report",
        {"clean", "warning"},
        "migration_bundle.data_product_access_drift_report_blocked",
    ),
    *COST_GOVERNANCE_STATUS_CHECKS,
    *ROLLOUT_STATUS_CHECKS,
    *TRUST_CENTER_STATUS_CHECKS,
    *REMEDIATION_STATUS_CHECKS,
    ("recovery_point", {"usable"}, "migration_bundle.recovery_point_blocked"),
    ("recovery_chain_verification", {"verified"}, "migration_bundle.recovery_chain_verification_blocked"),
    ("fixture_build", {"built", "dry_run"}, "migration_bundle.fixture_build_blocked"),
    ("quality_profile", {"profiled"}, "migration_bundle.quality_profile_blocked"),
    ("promotion", {None, "promoted"}, "migration_bundle.promotion_blocked"),
)


def status_blockers(artifacts: Mapping[str, BundleStatusArtifact]) -> list[str]:
    blockers: list[str] = []
    for kind, allowed, code in STATUS_CHECKS:
        artifact = artifacts.get(kind)
        if artifact and artifact.payload.get("status") not in allowed:
            blockers.append(code)
    return blockers


__all__ = ["STATUS_CHECKS", "status_blockers"]
