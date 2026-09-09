"""Provider-neutral schema migration evidence registry contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib import import_module
from typing import Any, Protocol

from dpone.readiness.migration_control import stable_fingerprint
from dpone.readiness.schema_migration_evidence_registry_constants import (
    REGISTRY_AUDIT_SCHEMA,
    REGISTRY_QUERY_SCHEMA,
    REGISTRY_RECORD_SCHEMA,
    REGISTRY_SCHEMA,
)
from dpone.readiness.schema_migration_evidence_registry_data_product import (
    data_product_extension_values,
    known_data_product_extensions,
)
from dpone.readiness.schema_migration_evidence_registry_query import logical_key, record_matches, sort_records
from dpone.readiness.schema_migration_evidence_registry_relationships import (
    MigrationEvidenceArtifactRef,
    artifact_refs,
    registry_status,
    relationship_blockers,
    relationship_warnings,
)


@dataclass(frozen=True, slots=True)
class MigrationEvidenceQuery:
    target: str | None = None
    environment: str | None = None
    stage: str | None = None
    status: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    include_blocked: bool = False


class MigrationEvidenceRegistryStore(Protocol):
    @property
    def backend_name(self) -> str: ...

    def append(self, record: Mapping[str, Any]) -> dict[str, Any]: ...

    def query(self, query: MigrationEvidenceQuery) -> tuple[dict[str, Any], ...]: ...


class MigrationEvidenceRecorder:
    """Builds normalized evidence registry records without filesystem or SCM APIs."""

    def build_record(
        self,
        *,
        bundle: Mapping[str, Any],
        verification: Mapping[str, Any],
        gate: Mapping[str, Any] | None,
        trust_verification: Mapping[str, Any] | None,
        diff: Mapping[str, Any] | None,
        environment: str,
        stage: str,
        contract_gate: Mapping[str, Any] | None = None,
        consumer_gate: Mapping[str, Any] | None = None,
        consumer_certification: Mapping[str, Any] | None = None,
        compatibility_view_gate: Mapping[str, Any] | None = None,
        contract_retirement_gate: Mapping[str, Any] | None = None,
        data_product_slo_gate: Mapping[str, Any] | None = None,
        data_product_incident_report: Mapping[str, Any] | None = None,
        data_product_error_budget_gate: Mapping[str, Any] | None = None,
        data_product_incident_lifecycle: Mapping[str, Any] | None = None,
        data_product_release_closeout_gate: Mapping[str, Any] | None = None,
        data_product_fleet_gate: Mapping[str, Any] | None = None,
        data_product_fleet_report: Mapping[str, Any] | None = None,
        data_product_reliability_export: Mapping[str, Any] | None = None,
        data_product_route_delivery_receipt: Mapping[str, Any] | None = None,
        data_product_assertion_gate: Mapping[str, Any] | None = None,
        data_product_assertion_report: Mapping[str, Any] | None = None,
        data_product_policy_gate: Mapping[str, Any] | None = None,
        data_product_policy_report: Mapping[str, Any] | None = None,
        data_product_waiver: Mapping[str, Any] | None = None,
        data_product_authority_gate: Mapping[str, Any] | None = None,
        data_product_approval_quorum: Mapping[str, Any] | None = None,
        data_product_evidence_signature: Mapping[str, Any] | None = None,
        data_product_governance_export_payload: Mapping[str, Any] | None = None,
        data_product_governance_publish_receipt: Mapping[str, Any] | None = None,
        data_product_governance_publish_verification: Mapping[str, Any] | None = None,
        data_product_compliance_gate: Mapping[str, Any] | None = None,
        data_product_audit_package: Mapping[str, Any] | None = None,
        data_product_audit_archive_verification: Mapping[str, Any] | None = None,
        data_product_audit_retention_plan: Mapping[str, Any] | None = None,
        data_product_legal_hold: Mapping[str, Any] | None = None,
        data_product_access_classification: Mapping[str, Any] | None = None,
        data_product_entitlement_plan: Mapping[str, Any] | None = None,
        data_product_privacy_impact_assessment: Mapping[str, Any] | None = None,
        data_product_access_gate: Mapping[str, Any] | None = None,
        data_product_access_report: Mapping[str, Any] | None = None,
        data_product_access_enforcement_certificate: Mapping[str, Any] | None = None,
        data_product_access_drift_report: Mapping[str, Any] | None = None,
        post_apply_certificate: Mapping[str, Any] | None = None,
        watch_certificate: Mapping[str, Any] | None = None,
        remediation_certificate: Mapping[str, Any] | None = None,
        backup_certificate: Mapping[str, Any] | None = None,
        recovery_point: Mapping[str, Any] | None = None,
        recovery_chain_verification: Mapping[str, Any] | None = None,
        actor: str = "ci",
        recorded_at: str | None = None,
        **data_product_extensions: Any,
    ) -> dict[str, Any]:
        recorded = recorded_at or _utc_now()
        data_product_extensions = known_data_product_extensions(data_product_extensions)
        blockers = list(
            relationship_blockers(
                bundle=bundle,
                verification=verification,
                gate=gate,
                trust=trust_verification,
                diff=diff,
                contract_gate=contract_gate,
                consumer_gate=consumer_gate,
                consumer_certification=consumer_certification,
                compatibility_view_gate=compatibility_view_gate,
                contract_retirement_gate=contract_retirement_gate,
                data_product_slo_gate=data_product_slo_gate,
                data_product_incident_report=data_product_incident_report,
                data_product_error_budget_gate=data_product_error_budget_gate,
                data_product_incident_lifecycle=data_product_incident_lifecycle,
                data_product_release_closeout_gate=data_product_release_closeout_gate,
                data_product_fleet_gate=data_product_fleet_gate,
                data_product_fleet_report=data_product_fleet_report,
                data_product_reliability_export=data_product_reliability_export,
                data_product_route_delivery_receipt=data_product_route_delivery_receipt,
                data_product_assertion_gate=data_product_assertion_gate,
                data_product_assertion_report=data_product_assertion_report,
                data_product_policy_gate=data_product_policy_gate,
                data_product_policy_report=data_product_policy_report,
                data_product_waiver=data_product_waiver,
                data_product_authority_gate=data_product_authority_gate,
                data_product_approval_quorum=data_product_approval_quorum,
                data_product_evidence_signature=data_product_evidence_signature,
                data_product_governance_export_payload=data_product_governance_export_payload,
                data_product_governance_publish_receipt=data_product_governance_publish_receipt,
                data_product_governance_publish_verification=data_product_governance_publish_verification,
                data_product_compliance_gate=data_product_compliance_gate,
                data_product_audit_package=data_product_audit_package,
                data_product_audit_archive_verification=data_product_audit_archive_verification,
                data_product_audit_retention_plan=data_product_audit_retention_plan,
                data_product_legal_hold=data_product_legal_hold,
                data_product_access_classification=data_product_access_classification,
                data_product_entitlement_plan=data_product_entitlement_plan,
                data_product_privacy_impact_assessment=data_product_privacy_impact_assessment,
                data_product_access_gate=data_product_access_gate,
                data_product_access_report=data_product_access_report,
                data_product_access_enforcement_certificate=data_product_access_enforcement_certificate,
                data_product_access_drift_report=data_product_access_drift_report,
                data_product_extensions=data_product_extensions,
                post_apply_certificate=post_apply_certificate,
                watch_certificate=watch_certificate,
                remediation_certificate=remediation_certificate,
                backup_certificate=backup_certificate,
                recovery_point=recovery_point,
                recovery_chain_verification=recovery_chain_verification,
                stage=stage,
            )
        )
        warnings = list(
            relationship_warnings(
                bundle,
                verification,
                gate,
                trust_verification,
                diff,
                contract_gate,
                consumer_gate,
                consumer_certification,
                compatibility_view_gate,
                contract_retirement_gate,
                data_product_slo_gate,
                data_product_incident_report,
                data_product_error_budget_gate,
                data_product_incident_lifecycle,
                data_product_release_closeout_gate,
                data_product_fleet_gate,
                data_product_fleet_report,
                data_product_reliability_export,
                data_product_route_delivery_receipt,
                data_product_assertion_gate,
                data_product_assertion_report,
                data_product_policy_gate,
                data_product_policy_report,
                data_product_waiver,
                data_product_authority_gate,
                data_product_approval_quorum,
                data_product_evidence_signature,
                data_product_governance_export_payload,
                data_product_governance_publish_receipt,
                data_product_governance_publish_verification,
                data_product_compliance_gate,
                data_product_audit_package,
                data_product_audit_archive_verification,
                data_product_audit_retention_plan,
                data_product_legal_hold,
                data_product_access_classification,
                data_product_entitlement_plan,
                data_product_privacy_impact_assessment,
                data_product_access_gate,
                data_product_access_report,
                data_product_access_enforcement_certificate,
                data_product_access_drift_report,
                *data_product_extension_values(data_product_extensions),
                post_apply_certificate,
                watch_certificate,
                remediation_certificate,
                backup_certificate,
                recovery_point,
                recovery_chain_verification,
            )
        )
        status = "blocked" if blockers else "warning" if warnings else registry_status(bundle, gate, trust_verification)
        target = _target(bundle)
        record_base: dict[str, Any] = {
            "schema_version": REGISTRY_RECORD_SCHEMA,
            "recorded_at": recorded,
            "target": target,
            "environment": environment,
            "stage": stage,
            "pack_id": bundle.get("pack_id"),
            "bundle_id": bundle.get("bundle_id"),
            "gate_id": _optional(gate, "gate_id"),
            "trust_verification_id": _optional(trust_verification, "trust_verification_id"),
            "diff_id": _optional(diff, "diff_id"),
            "status": status,
            "artifact_refs": artifact_refs(
                bundle=bundle,
                gate=gate,
                trust=trust_verification,
                diff=diff,
                contract_gate=contract_gate,
                consumer_gate=consumer_gate,
                consumer_certification=consumer_certification,
                compatibility_view_gate=compatibility_view_gate,
                contract_retirement_gate=contract_retirement_gate,
                data_product_slo_gate=data_product_slo_gate,
                data_product_incident_report=data_product_incident_report,
                data_product_error_budget_gate=data_product_error_budget_gate,
                data_product_incident_lifecycle=data_product_incident_lifecycle,
                data_product_release_closeout_gate=data_product_release_closeout_gate,
                data_product_fleet_gate=data_product_fleet_gate,
                data_product_fleet_report=data_product_fleet_report,
                data_product_reliability_export=data_product_reliability_export,
                data_product_route_delivery_receipt=data_product_route_delivery_receipt,
                data_product_assertion_gate=data_product_assertion_gate,
                data_product_assertion_report=data_product_assertion_report,
                data_product_policy_gate=data_product_policy_gate,
                data_product_policy_report=data_product_policy_report,
                data_product_waiver=data_product_waiver,
                data_product_authority_gate=data_product_authority_gate,
                data_product_approval_quorum=data_product_approval_quorum,
                data_product_evidence_signature=data_product_evidence_signature,
                data_product_governance_export_payload=data_product_governance_export_payload,
                data_product_governance_publish_receipt=data_product_governance_publish_receipt,
                data_product_governance_publish_verification=data_product_governance_publish_verification,
                data_product_compliance_gate=data_product_compliance_gate,
                data_product_audit_package=data_product_audit_package,
                data_product_audit_archive_verification=data_product_audit_archive_verification,
                data_product_audit_retention_plan=data_product_audit_retention_plan,
                data_product_legal_hold=data_product_legal_hold,
                data_product_access_classification=data_product_access_classification,
                data_product_entitlement_plan=data_product_entitlement_plan,
                data_product_privacy_impact_assessment=data_product_privacy_impact_assessment,
                data_product_access_gate=data_product_access_gate,
                data_product_access_report=data_product_access_report,
                data_product_access_enforcement_certificate=data_product_access_enforcement_certificate,
                data_product_access_drift_report=data_product_access_drift_report,
                data_product_extensions=data_product_extensions,
                post_apply_certificate=post_apply_certificate,
                watch_certificate=watch_certificate,
                remediation_certificate=remediation_certificate,
                backup_certificate=backup_certificate,
                recovery_point=recovery_point,
                recovery_chain_verification=recovery_chain_verification,
            ),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "actor": actor,
            "scm": _scm(trust_verification),
        }
        record_base["record_id"] = stable_fingerprint(
            {key: value for key, value in record_base.items() if key not in {"record_id", "artifact_refs"}}
            | {"artifact_refs": _stable_artifact_refs(record_base["artifact_refs"])}
        )
        return record_base


class MigrationEvidenceQueryService:
    """Read facade over registry stores."""

    def __init__(self, store: MigrationEvidenceRegistryStore) -> None:
        self._store = store

    def history(self, query: MigrationEvidenceQuery) -> dict[str, Any]:
        records = self._store.query(query)
        return {
            "schema_version": REGISTRY_QUERY_SCHEMA,
            "store_backend": self._store.backend_name,
            "query": _query_dict(query),
            "record_count": len(records),
            "records": list(records),
        }

    def latest(self, query: MigrationEvidenceQuery) -> dict[str, Any]:
        records = self._store.query(query)
        candidates = (
            records if query.include_blocked else tuple(item for item in records if item.get("status") != "blocked")
        )
        record = candidates[-1] if candidates else None
        return {
            "schema_version": REGISTRY_QUERY_SCHEMA,
            "store_backend": self._store.backend_name,
            "query": _query_dict(query),
            "record": record,
            "recommendations": _latest_recommendations(record),
        }


def _stable_artifact_refs(refs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted((dict(item) for item in refs), key=lambda item: (str(item.get("kind")), str(item.get("path"))))


def _target(bundle: Mapping[str, Any]) -> dict[str, Any]:
    return dict(bundle.get("target", {})) if isinstance(bundle.get("target"), Mapping) else {}


def _scm(trust: Mapping[str, Any] | None) -> dict[str, Any]:
    if not trust:
        return {}
    raw = trust.get("scm")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _optional(payload: Mapping[str, Any] | None, key: str) -> Any:
    return payload.get(key) if payload else None


def _query_dict(query: MigrationEvidenceQuery) -> dict[str, Any]:
    return {key: value for key, value in asdict(query).items() if value not in {None, False}}


def _latest_recommendations(record: Mapping[str, Any] | None) -> list[str]:
    if record is None:
        return ["Record a schema migration evidence bundle before querying latest approved evidence."]
    return ["Use this record as immutable review evidence for the matching migration pack."]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()  # noqa: UP017


_REPORTING_EXPORTS = ("MigrationEvidenceAuditReporter",)


def __getattr__(name: str) -> Any:
    if name in _REPORTING_EXPORTS:
        module = import_module("dpone.readiness.schema_migration_evidence_registry_reporting")
        return getattr(module, name)
    raise AttributeError(name)


__all__ = [
    *_REPORTING_EXPORTS,
    "MigrationEvidenceArtifactRef",
    "MigrationEvidenceQuery",
    "MigrationEvidenceQueryService",
    "MigrationEvidenceRecorder",
    "MigrationEvidenceRegistryStore",
    "REGISTRY_AUDIT_SCHEMA",
    "REGISTRY_QUERY_SCHEMA",
    "REGISTRY_RECORD_SCHEMA",
    "REGISTRY_SCHEMA",
    "logical_key",
    "record_matches",
    "sort_records",
]
