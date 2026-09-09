"""File-IO facade for schema migration evidence registry commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.schema_migration_bundle import MigrationBundleVerifier
from dpone.readiness.schema_migration_evidence_registry import (
    MigrationEvidenceQuery,
    MigrationEvidenceQueryService,
    MigrationEvidenceRecorder,
)
from dpone.readiness.schema_migration_evidence_registry_reporting import MigrationEvidenceAuditReporter

DEFAULT_REGISTRY = ".dpone/schema-migration/registry/registry.json"
LOCAL_JSON_BACKEND = "local_json"
SQLITE_BACKEND = "sqlite"


class MigrationEvidenceRegistryFacade:
    """Application facade used by CLI and CI jobs."""

    def record(
        self,
        *,
        bundle_path: str,
        environment: str,
        stage: str,
        store_backend: str = LOCAL_JSON_BACKEND,
        store_uri: str | None = None,
        gate_path: str | None = None,
        trust_path: str | None = None,
        diff_path: str | None = None,
        contract_gate_path: str | None = None,
        consumer_gate_path: str | None = None,
        consumer_certification_path: str | None = None,
        compatibility_view_gate_path: str | None = None,
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
        data_product_access_classification_path: str | None = None,
        data_product_entitlement_plan_path: str | None = None,
        data_product_privacy_impact_assessment_path: str | None = None,
        data_product_access_gate_path: str | None = None,
        data_product_access_report_path: str | None = None,
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
        data_product_remediation_gate_path: str | None = None,
        data_product_remediation_runbook_path: str | None = None,
        data_product_remediation_closeout_path: str | None = None,
        data_product_remediation_report_path: str | None = None,
        data_product_remediation_execution_plan_path: str | None = None,
        data_product_remediation_execution_run_path: str | None = None,
        data_product_remediation_execution_certificate_path: str | None = None,
        data_product_remediation_execution_report_path: str | None = None,
        post_apply_certificate_path: str | None = None,
        watch_certificate_path: str | None = None,
        remediation_certificate_path: str | None = None,
        backup_certificate_path: str | None = None,
        recovery_point_path: str | None = None,
        recovery_chain_verification_path: str | None = None,
        actor: str = "ci",
    ) -> dict[str, Any]:
        bundle = _read_mapping(bundle_path)
        artifact_bytes = _artifact_bytes(bundle, bundle_path)
        verification = MigrationBundleVerifier().verify(
            bundle=bundle,
            artifact_bytes=artifact_bytes,
            require_attestation=False,
        )
        record = MigrationEvidenceRecorder().build_record(
            bundle=bundle,
            verification=verification,
            gate=_read_optional(gate_path),
            trust_verification=_read_optional(trust_path),
            diff=_read_optional(diff_path),
            contract_gate=_read_optional(contract_gate_path),
            consumer_gate=_read_optional(consumer_gate_path),
            consumer_certification=_read_optional(consumer_certification_path),
            compatibility_view_gate=_read_optional(compatibility_view_gate_path),
            contract_retirement_gate=_read_optional(contract_retirement_gate_path),
            data_product_slo_gate=_read_optional(data_product_slo_gate_path),
            data_product_incident_report=_read_optional(data_product_incident_report_path),
            data_product_error_budget_gate=_read_optional(data_product_error_budget_gate_path),
            data_product_incident_lifecycle=_read_optional(data_product_incident_lifecycle_path),
            data_product_release_closeout_gate=_read_optional(data_product_release_closeout_gate_path),
            data_product_fleet_gate=_read_optional(data_product_fleet_gate_path),
            data_product_fleet_report=_read_optional(data_product_fleet_report_path),
            data_product_reliability_export=_read_optional(data_product_reliability_export_path),
            data_product_route_delivery_receipt=_read_optional(data_product_route_delivery_receipt_path),
            data_product_assertion_gate=_read_optional(data_product_assertion_gate_path),
            data_product_assertion_report=_read_optional(data_product_assertion_report_path),
            data_product_policy_gate=_read_optional(data_product_policy_gate_path),
            data_product_policy_report=_read_optional(data_product_policy_report_path),
            data_product_waiver=_read_optional(data_product_waiver_path),
            data_product_authority_gate=_read_optional(data_product_authority_gate_path),
            data_product_approval_quorum=_read_optional(data_product_approval_quorum_path),
            data_product_evidence_signature=_read_optional(data_product_evidence_signature_path),
            data_product_governance_export_payload=_read_optional(data_product_governance_export_payload_path),
            data_product_governance_publish_receipt=_read_optional(data_product_governance_publish_receipt_path),
            data_product_governance_publish_verification=_read_optional(
                data_product_governance_publish_verification_path
            ),
            data_product_compliance_gate=_read_optional(data_product_compliance_gate_path),
            data_product_audit_package=_read_optional(data_product_audit_package_path),
            data_product_audit_archive_verification=_read_optional(data_product_audit_archive_verification_path),
            data_product_audit_retention_plan=_read_optional(data_product_audit_retention_plan_path),
            data_product_legal_hold=_read_optional(data_product_legal_hold_path),
            data_product_access_classification=_read_optional(data_product_access_classification_path),
            data_product_entitlement_plan=_read_optional(data_product_entitlement_plan_path),
            data_product_privacy_impact_assessment=_read_optional(data_product_privacy_impact_assessment_path),
            data_product_access_gate=_read_optional(data_product_access_gate_path),
            data_product_access_report=_read_optional(data_product_access_report_path),
            data_product_access_enforcement_certificate=_read_optional(
                data_product_access_enforcement_certificate_path
            ),
            data_product_access_drift_report=_read_optional(data_product_access_drift_report_path),
            data_product_cost_gate=_read_optional(data_product_cost_gate_path),
            data_product_cost_forecast=_read_optional(data_product_cost_forecast_path),
            data_product_cost_report=_read_optional(data_product_cost_report_path),
            data_product_ring_gate=_read_optional(data_product_ring_gate_path),
            data_product_shadow_validation=_read_optional(data_product_shadow_validation_path),
            data_product_rollout_promotion=_read_optional(data_product_rollout_promotion_path),
            data_product_rollout_report=_read_optional(data_product_rollout_report_path),
            data_product_trust_gate=_read_optional(data_product_trust_gate_path),
            data_product_trust_report=_read_optional(data_product_trust_report_path),
            data_product_trust_export=_read_optional(data_product_trust_export_path),
            data_product_remediation_gate=_read_optional(data_product_remediation_gate_path),
            data_product_remediation_runbook=_read_optional(data_product_remediation_runbook_path),
            data_product_remediation_closeout=_read_optional(data_product_remediation_closeout_path),
            data_product_remediation_report=_read_optional(data_product_remediation_report_path),
            data_product_remediation_execution_plan=_read_optional(data_product_remediation_execution_plan_path),
            data_product_remediation_execution_run=_read_optional(data_product_remediation_execution_run_path),
            data_product_remediation_execution_certificate=_read_optional(
                data_product_remediation_execution_certificate_path
            ),
            data_product_remediation_execution_report=_read_optional(data_product_remediation_execution_report_path),
            post_apply_certificate=_read_optional(post_apply_certificate_path),
            watch_certificate=_read_optional(watch_certificate_path),
            remediation_certificate=_read_optional(remediation_certificate_path),
            backup_certificate=_read_optional(backup_certificate_path),
            recovery_point=_read_optional(recovery_point_path),
            recovery_chain_verification=_read_optional(recovery_chain_verification_path),
            environment=environment,
            stage=stage,
            actor=actor,
        )
        append = _store(store_backend, store_uri).append(record)
        if append["status"] == "blocked":
            return {**record, "status": "blocked", "blockers": _merged(record.get("blockers", []), append["blockers"])}
        return {**record, "append_status": append["status"]}

    def history(
        self,
        *,
        target: str | None,
        environment: str | None,
        stage: str | None,
        status: str | None,
        date_from: str | None,
        date_to: str | None,
        store_backend: str = LOCAL_JSON_BACKEND,
        store_uri: str | None = None,
    ) -> dict[str, Any]:
        return MigrationEvidenceQueryService(_store(store_backend, store_uri)).history(
            MigrationEvidenceQuery(
                target=target,
                environment=environment,
                stage=stage,
                status=status,
                date_from=date_from,
                date_to=date_to,
            )
        )

    def latest(
        self,
        *,
        target: str | None,
        environment: str | None,
        stage: str | None,
        status: str | None,
        include_blocked: bool,
        store_backend: str = LOCAL_JSON_BACKEND,
        store_uri: str | None = None,
    ) -> dict[str, Any]:
        return MigrationEvidenceQueryService(_store(store_backend, store_uri)).latest(
            MigrationEvidenceQuery(
                target=target,
                environment=environment,
                stage=stage,
                status=status,
                include_blocked=include_blocked,
            )
        )

    def audit_report(
        self,
        *,
        target: str | None,
        environment: str | None,
        date_from: str | None,
        date_to: str | None,
        store_backend: str = LOCAL_JSON_BACKEND,
        store_uri: str | None = None,
    ) -> dict[str, Any]:
        history = self.history(
            target=target,
            environment=environment,
            stage=None,
            status=None,
            date_from=date_from,
            date_to=date_to,
            store_backend=store_backend,
            store_uri=store_uri,
        )
        return MigrationEvidenceAuditReporter().build(
            records=tuple(history["records"]),
            target=target,
            date_from=date_from,
            date_to=date_to,
        )


def _store(backend: str, store_uri: str | None) -> Any:
    normalized = backend.strip().lower().replace("-", "_")
    if normalized == LOCAL_JSON_BACKEND:
        module = import_module("dpone.readiness.schema_migration_evidence_registry_store")
        return module.LocalJsonEvidenceRegistryStore(store_uri or DEFAULT_REGISTRY)
    if normalized == SQLITE_BACKEND:
        module = import_module("dpone.readiness.schema_migration_evidence_registry_sqlite")
        return module.SqliteEvidenceRegistryStore(store_uri or ".dpone/schema-migration/registry.sqlite3")
    raise ValueError("unsupported evidence registry store backend: " + backend)


def _artifact_bytes(bundle: Mapping[str, Any], bundle_path: str) -> dict[str, bytes]:
    loaded: dict[str, bytes] = {}
    for artifact in bundle.get("artifacts", []):
        if not isinstance(artifact, Mapping) or not artifact.get("path"):
            continue
        try:
            path = str(artifact["path"])
            loaded[path] = _read_bytes(path, bundle_path=bundle_path)
        except FileNotFoundError:
            continue
    return loaded


def _read_optional(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(path) if path else None


def _read_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    text = Path(path).read_text(encoding="utf-8")
    raw = json.loads(text) if path.lower().endswith(".json") else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _read_bytes(path: str, *, bundle_path: str) -> bytes:
    raw = Path(path)
    if raw.is_absolute() or raw.exists():
        return raw.read_bytes()
    return (Path(bundle_path).parent / raw).read_bytes()


def _merged(first: object, second: object) -> list[str]:
    values: list[str] = []
    for raw in (first, second):
        if isinstance(raw, list):
            values.extend(str(item) for item in raw if str(item))
    return list(dict.fromkeys(values))


__all__ = ["MigrationEvidenceRegistryFacade"]
