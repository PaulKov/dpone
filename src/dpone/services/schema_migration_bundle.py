"""File-IO facade for schema migration evidence bundles."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpone.readiness.schema_migration_bundle import (
    MigrationBundleBuilder,
    MigrationBundleVerifier,
    MigrationEvidenceArtifact,
    MigrationReviewRenderer,
)


class MigrationBundleFacade:
    """Thin application facade used by CLI and CI jobs."""

    def build(
        self,
        *,
        pack_path: str,
        output_dir: str,
        impact_path: str | None = None,
        environment_contract_path: str | None = None,
        certificate_path: str | None = None,
        rehearsal_certificate_path: str | None = None,
        post_apply_certificate_path: str | None = None,
        watch_certificate_path: str | None = None,
        remediation_certificate_path: str | None = None,
        backup_certificate_path: str | None = None,
        contract_gate_path: str | None = None,
        consumer_gate_path: str | None = None,
        consumer_test_kit_path: str | None = None,
        consumer_certification_path: str | None = None,
        compatibility_view_gate_path: str | None = None,
        contract_adoption_status_path: str | None = None,
        contract_retirement_gate_path: str | None = None,
        data_product_slo_gate_path: str | None = None,
        data_product_incident_report_path: str | None = None,
        data_product_error_budget_gate_path: str | None = None,
        data_product_incident_lifecycle_path: str | None = None,
        data_product_release_closeout_gate_path: str | None = None,
        data_product_fleet_gate_path: str | None = None,
        data_product_fleet_report_path: str | None = None,
        data_product_reliability_export_path: str | None = None,
        data_product_route_delivery_receipt_path: str | None = None,
        data_product_assertion_gate_path: str | None = None,
        data_product_assertion_report_path: str | None = None,
        data_product_policy_gate_path: str | None = None,
        data_product_policy_report_path: str | None = None,
        data_product_waiver_path: str | None = None,
        data_product_authority_gate_path: str | None = None,
        data_product_approval_quorum_path: str | None = None,
        data_product_evidence_signature_path: str | None = None,
        data_product_governance_export_payload_path: str | None = None,
        data_product_governance_publish_receipt_path: str | None = None,
        data_product_governance_publish_verification_path: str | None = None,
        data_product_compliance_gate_path: str | None = None,
        data_product_audit_package_path: str | None = None,
        data_product_audit_archive_verification_path: str | None = None,
        data_product_audit_retention_plan_path: str | None = None,
        data_product_legal_hold_path: str | None = None,
        data_product_access_gate_path: str | None = None,
        data_product_access_report_path: str | None = None,
        data_product_privacy_impact_assessment_path: str | None = None,
        data_product_access_enforcement_certificate_path: str | None = None,
        data_product_access_drift_report_path: str | None = None,
        data_product_cost_gate_path: str | None = None,
        data_product_cost_forecast_path: str | None = None,
        data_product_cost_report_path: str | None = None,
        data_product_ring_gate_path: str | None = None,
        data_product_shadow_validation_path: str | None = None,
        data_product_rollout_promotion_path: str | None = None,
        data_product_rollout_report_path: str | None = None,
        data_product_trust_gate_path: str | None = None,
        data_product_trust_report_path: str | None = None,
        data_product_trust_export_path: str | None = None,
        recovery_point_path: str | None = None,
        recovery_chain_verification_path: str | None = None,
        fixture_build_path: str | None = None,
        quality_profile_path: str | None = None,
        promotion_path: str | None = None,
        approval_path: str | None = None,
        attest: bool = False,
    ) -> dict[str, Any]:
        artifacts = tuple(
            artifact
            for artifact in (
                _artifact("migration_pack", pack_path, required=True),
                _optional_artifact("impact_plan", impact_path),
                _optional_artifact("environment_contract", environment_contract_path),
                _optional_artifact("certification", certificate_path),
                _optional_artifact("rehearsal_certificate", rehearsal_certificate_path),
                _optional_artifact("post_apply_certificate", post_apply_certificate_path),
                _optional_artifact("watch_certificate", watch_certificate_path),
                _optional_artifact("remediation_certificate", remediation_certificate_path),
                _optional_artifact("backup_certificate", backup_certificate_path),
                _optional_artifact("contract_gate", contract_gate_path),
                _optional_artifact("consumer_gate", consumer_gate_path),
                _optional_artifact("consumer_test_kit", consumer_test_kit_path),
                _optional_artifact("consumer_certification", consumer_certification_path),
                _optional_artifact("compatibility_view_gate", compatibility_view_gate_path),
                _optional_artifact("contract_adoption_status", contract_adoption_status_path),
                _optional_artifact("contract_retirement_gate", contract_retirement_gate_path),
                _optional_artifact("data_product_slo_gate", data_product_slo_gate_path),
                _optional_artifact("data_product_incident_report", data_product_incident_report_path),
                _optional_artifact("data_product_error_budget_gate", data_product_error_budget_gate_path),
                _optional_artifact("data_product_incident_lifecycle", data_product_incident_lifecycle_path),
                _optional_artifact("data_product_release_closeout_gate", data_product_release_closeout_gate_path),
                _optional_artifact("data_product_fleet_gate", data_product_fleet_gate_path),
                _optional_artifact("data_product_fleet_report", data_product_fleet_report_path),
                _optional_artifact("data_product_reliability_export", data_product_reliability_export_path),
                _optional_artifact("data_product_route_delivery_receipt", data_product_route_delivery_receipt_path),
                _optional_artifact("data_product_assertion_gate", data_product_assertion_gate_path),
                _optional_artifact("data_product_assertion_report", data_product_assertion_report_path),
                _optional_artifact("data_product_policy_gate", data_product_policy_gate_path),
                _optional_artifact("data_product_policy_report", data_product_policy_report_path),
                _optional_artifact("data_product_waiver", data_product_waiver_path),
                _optional_artifact("data_product_authority_gate", data_product_authority_gate_path),
                _optional_artifact("data_product_approval_quorum", data_product_approval_quorum_path),
                _optional_artifact("data_product_evidence_signature", data_product_evidence_signature_path),
                _optional_artifact(
                    "data_product_governance_export_payload",
                    data_product_governance_export_payload_path,
                ),
                _optional_artifact(
                    "data_product_governance_publish_receipt",
                    data_product_governance_publish_receipt_path,
                ),
                _optional_artifact(
                    "data_product_governance_publish_verification",
                    data_product_governance_publish_verification_path,
                ),
                _optional_artifact("data_product_compliance_gate", data_product_compliance_gate_path),
                _optional_artifact("data_product_audit_package", data_product_audit_package_path),
                _optional_artifact(
                    "data_product_audit_archive_verification",
                    data_product_audit_archive_verification_path,
                ),
                _optional_artifact("data_product_audit_retention_plan", data_product_audit_retention_plan_path),
                _optional_artifact("data_product_legal_hold", data_product_legal_hold_path),
                _optional_artifact("data_product_access_gate", data_product_access_gate_path),
                _optional_artifact("data_product_access_report", data_product_access_report_path),
                _optional_artifact(
                    "data_product_privacy_impact_assessment",
                    data_product_privacy_impact_assessment_path,
                ),
                _optional_artifact(
                    "data_product_access_enforcement_certificate",
                    data_product_access_enforcement_certificate_path,
                ),
                _optional_artifact("data_product_access_drift_report", data_product_access_drift_report_path),
                _optional_artifact("data_product_cost_gate", data_product_cost_gate_path),
                _optional_artifact("data_product_cost_forecast", data_product_cost_forecast_path),
                _optional_artifact("data_product_cost_report", data_product_cost_report_path),
                _optional_artifact("data_product_ring_gate", data_product_ring_gate_path),
                _optional_artifact("data_product_shadow_validation", data_product_shadow_validation_path),
                _optional_artifact("data_product_rollout_promotion", data_product_rollout_promotion_path),
                _optional_artifact("data_product_rollout_report", data_product_rollout_report_path),
                _optional_artifact("data_product_trust_gate", data_product_trust_gate_path),
                _optional_artifact("data_product_trust_report", data_product_trust_report_path),
                _optional_artifact("data_product_trust_export", data_product_trust_export_path),
                _optional_artifact("recovery_point", recovery_point_path),
                _optional_artifact("recovery_chain_verification", recovery_chain_verification_path),
                _optional_artifact("fixture_build", fixture_build_path),
                _optional_artifact("quality_profile", quality_profile_path),
                _optional_artifact("promotion", promotion_path),
                _optional_artifact("approval", approval_path),
            )
            if artifact is not None
        )
        bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=attest)
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        _write_json(out_dir / "bundle.json", bundle)
        return bundle

    def verify(self, *, bundle_path: str, require_attestation: bool = False) -> dict[str, Any]:
        bundle = _read_json(Path(bundle_path))
        artifact_bytes = {
            str(artifact.get("path")): _read_bytes(str(artifact.get("path")), bundle_path=bundle_path)
            for artifact in bundle.get("artifacts", [])
            if isinstance(artifact, dict) and artifact.get("path")
        }
        return MigrationBundleVerifier().verify(
            bundle=bundle,
            artifact_bytes=artifact_bytes,
            require_attestation=require_attestation,
        )

    def render_review(self, *, bundle_path: str) -> dict[str, Any]:
        return MigrationReviewRenderer().render_payload(_read_json(Path(bundle_path)))


def _artifact(kind: str, path: str, *, required: bool) -> MigrationEvidenceArtifact:
    return MigrationEvidenceArtifact.from_bytes(
        kind=kind, path=str(path), content=Path(path).read_bytes(), required=required
    )


def _optional_artifact(kind: str, path: str | None) -> MigrationEvidenceArtifact | None:
    return _artifact(kind, path, required=False) if path else None


def _read_bytes(path: str, *, bundle_path: str) -> bytes:
    raw = Path(path)
    if raw.is_absolute() or raw.exists():
        return raw.read_bytes()
    return (Path(bundle_path).parent / raw).read_bytes()


def _read_json(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return raw


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n", encoding="utf-8")


__all__ = ["MigrationBundleFacade"]
