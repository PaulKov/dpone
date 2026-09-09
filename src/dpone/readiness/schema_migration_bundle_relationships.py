"""Relationship checks and summaries for schema migration bundles."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from dpone.readiness.migration_control import MigrationPack
from dpone.readiness.schema_migration_bundle_cost import COST_GOVERNANCE_PACK_BOUND, cost_governance_summary_ids
from dpone.readiness.schema_migration_bundle_remediation import REMEDIATION_PACK_BOUND, remediation_summary_ids
from dpone.readiness.schema_migration_bundle_rollout import ROLLOUT_PACK_BOUND, rollout_summary_ids
from dpone.readiness.schema_migration_bundle_status import status_blockers
from dpone.readiness.schema_migration_bundle_summary import payload_value, summary_ids
from dpone.readiness.schema_migration_bundle_trust_center import TRUST_CENTER_PACK_BOUND, trust_center_summary_ids


class BundleArtifact(Protocol):
    kind: str
    payload: dict[str, Any]
    pack_id: str | None


def relationship_blockers(
    *,
    pack: MigrationPack,
    artifacts: Mapping[str, BundleArtifact],
) -> tuple[str, ...]:
    blockers: list[str] = []
    for kind, code in _PACK_BOUND_ARTIFACTS.items():
        artifact = artifacts.get(kind)
        if artifact and artifact.pack_id and artifact.pack_id != pack.pack_id:
            blockers.append(code)
    blockers.extend(_artifact_blockers(artifacts))
    return tuple(blockers)


def relationship_warnings(*, artifacts: Mapping[str, BundleArtifact]) -> tuple[str, ...]:
    warnings: list[str] = []
    for kind in _WARNING_ARTIFACTS:
        artifact = artifacts.get(kind)
        if artifact:
            warnings.extend(str(item) for item in artifact.payload.get("warnings", []) if str(item))
    return tuple(warnings)


def required_approvals(impact: BundleArtifact | None) -> list[str]:
    if impact is None:
        return []
    raw = impact.payload.get("required_approvals", impact.payload.get("required_risks", []))
    return sorted({str(item) for item in raw if str(item)}) if isinstance(raw, list) else []


def approval_blockers(required: list[str], approval: BundleArtifact | None) -> tuple[str, ...]:
    if not required:
        return ()
    if approval is None:
        return tuple(f"migration_bundle.approval_required:{risk}" for risk in required)
    approved = approval.payload.get("approved_risks", [])
    approved_set = {str(item) for item in approved} if isinstance(approved, list) else set()
    return tuple(f"migration_bundle.approval_required:{risk}" for risk in required if risk not in approved_set)


def bundle_summary(
    *,
    pack: MigrationPack,
    artifacts: Mapping[str, BundleArtifact],
    required_approvals: list[str],
) -> dict[str, Any]:
    promotion = artifacts.get("promotion")
    approval = artifacts.get("approval")
    return {
        "strategy": pack.strategy,
        "changes_count": len(pack.changes),
        "blockers_count": len(pack.blockers),
        "required_approvals": required_approvals,
        "from_environment": _payload_value(promotion, "from_environment"),
        "to_environment": _payload_value(promotion, "to_environment"),
        "approved_by": _payload_value(approval, "approved_by"),
        **summary_ids(artifacts),
        **cost_governance_summary_ids(artifacts),
        **rollout_summary_ids(artifacts),
        **trust_center_summary_ids(artifacts),
        **remediation_summary_ids(artifacts),
    }


def _artifact_blockers(artifacts: Mapping[str, BundleArtifact]) -> list[str]:
    blockers: list[str] = []
    impact = artifacts.get("impact_plan")
    if impact:
        blockers.extend(str(item) for item in impact.payload.get("blockers", []) if str(item))
    blockers.extend(status_blockers(artifacts))
    blockers.extend(_relationship_id_blockers(artifacts))
    return blockers


def _relationship_id_blockers(artifacts: Mapping[str, BundleArtifact]) -> list[str]:
    blockers: list[str] = []
    if (
        _payload_value(artifacts.get("recovery_chain_verification"), "restore_point_id")
        != _payload_value(artifacts.get("recovery_point"), "restore_point_id")
        and artifacts.get("recovery_point")
        and artifacts.get("recovery_chain_verification")
    ):
        blockers.append("migration_bundle.recovery_point_chain_mismatch")
    if (
        _payload_value(artifacts.get("quality_profile"), "fixture_build_id")
        != _payload_value(artifacts.get("fixture_build"), "fixture_build_id")
        and artifacts.get("fixture_build")
        and artifacts.get("quality_profile")
    ):
        blockers.append("migration_bundle.fixture_profile_id_mismatch")
    if (
        _payload_value(artifacts.get("certification"), "certification_id")
        != _payload_value(artifacts.get("promotion"), "certification_id")
        and artifacts.get("certification")
        and artifacts.get("promotion")
    ):
        blockers.append("migration_bundle.certification_id_mismatch")
    return blockers


def _payload_value(artifact: BundleArtifact | None, key: str) -> Any:
    return payload_value(artifact, key)


_PACK_BOUND_ARTIFACTS = {
    "impact_plan": "migration_bundle.impact_pack_id_mismatch",
    "certification": "migration_bundle.certification_pack_id_mismatch",
    "rehearsal_certificate": "migration_bundle.rehearsal_certificate_pack_id_mismatch",
    "post_apply_certificate": "migration_bundle.post_apply_certificate_pack_id_mismatch",
    "watch_certificate": "migration_bundle.watch_certificate_pack_id_mismatch",
    "remediation_certificate": "migration_bundle.remediation_certificate_pack_id_mismatch",
    "backup_certificate": "migration_bundle.backup_certificate_pack_id_mismatch",
    "contract_gate": "migration_bundle.contract_gate_pack_id_mismatch",
    "consumer_gate": "migration_bundle.consumer_gate_pack_id_mismatch",
    "consumer_test_kit": "migration_bundle.consumer_test_kit_pack_id_mismatch",
    "consumer_certification": "migration_bundle.consumer_certification_pack_id_mismatch",
    "compatibility_view_gate": "migration_bundle.compatibility_view_gate_pack_id_mismatch",
    "contract_adoption_status": "migration_bundle.contract_adoption_status_pack_id_mismatch",
    "contract_retirement_gate": "migration_bundle.contract_retirement_gate_pack_id_mismatch",
    "data_product_slo_gate": "migration_bundle.data_product_slo_gate_pack_id_mismatch",
    "data_product_incident_report": "migration_bundle.data_product_incident_report_pack_id_mismatch",
    "data_product_error_budget_gate": "migration_bundle.data_product_error_budget_gate_pack_id_mismatch",
    "data_product_incident_lifecycle": "migration_bundle.data_product_incident_lifecycle_pack_id_mismatch",
    "data_product_release_closeout_gate": "migration_bundle.data_product_release_closeout_gate_pack_id_mismatch",
    "data_product_fleet_gate": "migration_bundle.data_product_fleet_gate_pack_id_mismatch",
    "data_product_fleet_report": "migration_bundle.data_product_fleet_report_pack_id_mismatch",
    "data_product_reliability_export": "migration_bundle.data_product_reliability_export_pack_id_mismatch",
    "data_product_route_delivery_receipt": "migration_bundle.data_product_route_delivery_receipt_pack_id_mismatch",
    "data_product_assertion_gate": "migration_bundle.data_product_assertion_gate_pack_id_mismatch",
    "data_product_assertion_report": "migration_bundle.data_product_assertion_report_pack_id_mismatch",
    "data_product_policy_gate": "migration_bundle.data_product_policy_gate_pack_id_mismatch",
    "data_product_policy_report": "migration_bundle.data_product_policy_report_pack_id_mismatch",
    "data_product_waiver": "migration_bundle.data_product_waiver_pack_id_mismatch",
    "data_product_authority_gate": "migration_bundle.data_product_authority_gate_pack_id_mismatch",
    "data_product_approval_quorum": "migration_bundle.data_product_approval_quorum_pack_id_mismatch",
    "data_product_evidence_signature": "migration_bundle.data_product_evidence_signature_pack_id_mismatch",
    "data_product_governance_export_payload": "migration_bundle.data_product_governance_export_payload_pack_id_mismatch",
    "data_product_governance_publish_receipt": "migration_bundle.data_product_governance_publish_receipt_pack_id_mismatch",
    "data_product_governance_publish_verification": (
        "migration_bundle.data_product_governance_publish_verification_pack_id_mismatch"
    ),
    "data_product_access_gate": "migration_bundle.data_product_access_gate_pack_id_mismatch",
    "data_product_access_report": "migration_bundle.data_product_access_report_pack_id_mismatch",
    "data_product_privacy_impact_assessment": (
        "migration_bundle.data_product_privacy_impact_assessment_pack_id_mismatch"
    ),
    "data_product_access_enforcement_certificate": (
        "migration_bundle.data_product_access_enforcement_certificate_pack_id_mismatch"
    ),
    "data_product_access_drift_report": "migration_bundle.data_product_access_drift_report_pack_id_mismatch",
    **COST_GOVERNANCE_PACK_BOUND,
    **ROLLOUT_PACK_BOUND,
    **TRUST_CENTER_PACK_BOUND,
    **REMEDIATION_PACK_BOUND,
    "recovery_point": "migration_bundle.recovery_point_pack_id_mismatch",
    "recovery_chain_verification": "migration_bundle.recovery_chain_verification_pack_id_mismatch",
    "fixture_build": "migration_bundle.fixture_build_pack_id_mismatch",
    "quality_profile": "migration_bundle.quality_profile_pack_id_mismatch",
    "promotion": "migration_bundle.promotion_pack_id_mismatch",
    "approval": "migration_bundle.approval_pack_id_mismatch",
}

_WARNING_ARTIFACTS = tuple(kind for kind in _PACK_BOUND_ARTIFACTS if kind != "approval")


__all__ = [
    "approval_blockers",
    "bundle_summary",
    "relationship_blockers",
    "relationship_warnings",
    "required_approvals",
]
