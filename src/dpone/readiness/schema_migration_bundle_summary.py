"""Summary helpers for schema migration bundle relationships."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol


class BundleSummaryArtifact(Protocol):
    payload: dict[str, Any]


def payload_value(artifact: BundleSummaryArtifact | None, key: str) -> Any:
    return artifact.payload.get(key) if artifact else None


def summary_ids(artifacts: Mapping[str, BundleSummaryArtifact]) -> dict[str, Any]:
    return {
        "rehearsal_certificate_id": payload_value(artifacts.get("rehearsal_certificate"), "certificate_id"),
        "post_apply_certificate_id": payload_value(artifacts.get("post_apply_certificate"), "certificate_id"),
        "watch_certificate_id": payload_value(artifacts.get("watch_certificate"), "certificate_id"),
        "remediation_certificate_id": payload_value(artifacts.get("remediation_certificate"), "certificate_id"),
        "backup_certificate_id": payload_value(artifacts.get("backup_certificate"), "certificate_id"),
        "contract_gate_id": payload_value(artifacts.get("contract_gate"), "contract_gate_id"),
        "contract_version_id": payload_value(artifacts.get("contract_gate"), "contract_version_id"),
        "consumer_gate_id": payload_value(artifacts.get("consumer_gate"), "consumer_gate_id"),
        "consumer_matrix_id": payload_value(artifacts.get("consumer_gate"), "consumer_matrix_id"),
        "consumer_test_kit_id": payload_value(artifacts.get("consumer_test_kit"), "test_kit_id"),
        "consumer_certification_id": payload_value(
            artifacts.get("consumer_certification"), "consumer_certification_id"
        ),
        "compatibility_view_gate_id": payload_value(
            artifacts.get("compatibility_view_gate"), "compatibility_view_gate_id"
        ),
        "compatibility_view_plan_id": payload_value(
            artifacts.get("compatibility_view_gate"), "compatibility_view_plan_id"
        ),
        "contract_adoption_status_id": payload_value(artifacts.get("contract_adoption_status"), "adoption_status_id"),
        "contract_retirement_gate_id": payload_value(artifacts.get("contract_retirement_gate"), "retirement_gate_id"),
        "data_product_slo_gate_id": payload_value(artifacts.get("data_product_slo_gate"), "slo_gate_id"),
        "data_product_incident_report_id": payload_value(
            artifacts.get("data_product_incident_report"), "incident_report_id"
        ),
        "data_product_error_budget_gate_id": payload_value(
            artifacts.get("data_product_error_budget_gate"), "error_budget_gate_id"
        ),
        "data_product_incident_lifecycle_id": payload_value(
            artifacts.get("data_product_incident_lifecycle"), "incident_id"
        ),
        "data_product_release_closeout_gate_id": payload_value(
            artifacts.get("data_product_release_closeout_gate"), "release_closeout_gate_id"
        ),
        "data_product_fleet_gate_id": payload_value(artifacts.get("data_product_fleet_gate"), "fleet_gate_id"),
        "data_product_fleet_report_id": payload_value(artifacts.get("data_product_fleet_report"), "fleet_report_id"),
        "data_product_reliability_export_id": payload_value(
            artifacts.get("data_product_reliability_export"), "export_id"
        ),
        "data_product_route_delivery_receipt_id": payload_value(
            artifacts.get("data_product_route_delivery_receipt"), "route_delivery_receipt_id"
        ),
        "data_product_assertion_gate_id": payload_value(
            artifacts.get("data_product_assertion_gate"), "assertion_gate_id"
        ),
        "data_product_assertion_report_id": payload_value(
            artifacts.get("data_product_assertion_report"), "assertion_report_id"
        ),
        "data_product_policy_gate_id": payload_value(artifacts.get("data_product_policy_gate"), "policy_gate_id"),
        "data_product_policy_report_id": payload_value(artifacts.get("data_product_policy_report"), "policy_report_id"),
        "data_product_waiver_id": payload_value(artifacts.get("data_product_waiver"), "waiver_id"),
        "data_product_authority_gate_id": payload_value(
            artifacts.get("data_product_authority_gate"), "authority_gate_id"
        ),
        "data_product_approval_quorum_id": payload_value(
            artifacts.get("data_product_approval_quorum"), "approval_quorum_id"
        ),
        "data_product_evidence_signature_id": payload_value(
            artifacts.get("data_product_evidence_signature"), "evidence_signature_id"
        ),
        "data_product_governance_export_payload_id": payload_value(
            artifacts.get("data_product_governance_export_payload"), "payload_id"
        ),
        "data_product_governance_publish_receipt_id": payload_value(
            artifacts.get("data_product_governance_publish_receipt"), "publish_receipt_id"
        ),
        "data_product_governance_publish_verification_id": payload_value(
            artifacts.get("data_product_governance_publish_verification"), "publish_verification_id"
        ),
        "data_product_audit_archive_verification_id": payload_value(
            artifacts.get("data_product_audit_archive_verification"), "audit_archive_verification_id"
        ),
        "data_product_audit_retention_plan_id": payload_value(
            artifacts.get("data_product_audit_retention_plan"), "audit_retention_plan_id"
        ),
        "data_product_legal_hold_id": payload_value(artifacts.get("data_product_legal_hold"), "legal_hold_id"),
        "data_product_access_gate_id": payload_value(artifacts.get("data_product_access_gate"), "access_gate_id"),
        "data_product_access_report_id": payload_value(artifacts.get("data_product_access_report"), "access_report_id"),
        "data_product_privacy_impact_id": payload_value(
            artifacts.get("data_product_privacy_impact_assessment"), "privacy_impact_id"
        ),
        "data_product_access_enforcement_certificate_id": payload_value(
            artifacts.get("data_product_access_enforcement_certificate"), "access_enforcement_certificate_id"
        ),
        "data_product_access_drift_report_id": payload_value(
            artifacts.get("data_product_access_drift_report"), "access_drift_report_id"
        ),
        "recovery_point_id": payload_value(artifacts.get("recovery_point"), "restore_point_id"),
        "recovery_chain_verification_id": payload_value(
            artifacts.get("recovery_chain_verification"), "chain_verification_id"
        ),
        "fixture_build_id": payload_value(artifacts.get("fixture_build"), "fixture_build_id"),
        "quality_profile_id": payload_value(artifacts.get("quality_profile"), "profile_id"),
    }


__all__ = ["payload_value", "summary_ids"]
