"""Schema migration bundle gate check taxonomy."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

APPROVAL_IMPACT_REQUIRED = "impact_required"

KNOWN_BUNDLE_ARTIFACTS = frozenset(
    {
        "migration_pack",
        "impact_plan",
        "approval",
        "environment_contract",
        "certification",
        "rehearsal_certificate",
        "post_apply_certificate",
        "watch_certificate",
        "remediation_certificate",
        "backup_certificate",
        "contract_gate",
        "consumer_gate",
        "consumer_test_kit",
        "consumer_certification",
        "compatibility_view_gate",
        "contract_adoption_status",
        "contract_retirement_gate",
        "data_product_slo_gate",
        "data_product_incident_report",
        "data_product_error_budget_gate",
        "data_product_incident_lifecycle",
        "data_product_release_closeout_gate",
        "data_product_fleet_gate",
        "data_product_fleet_report",
        "data_product_reliability_export",
        "data_product_route_delivery_receipt",
        "data_product_assertion_gate",
        "data_product_assertion_report",
        "data_product_policy_gate",
        "data_product_policy_report",
        "data_product_waiver",
        "data_product_authority_gate",
        "data_product_approval_quorum",
        "data_product_evidence_signature",
        "data_product_governance_export_payload",
        "data_product_governance_publish_receipt",
        "data_product_governance_publish_verification",
        "data_product_compliance_gate",
        "data_product_audit_package",
        "data_product_audit_archive_verification",
        "data_product_audit_retention_plan",
        "data_product_legal_hold",
        "data_product_access_gate",
        "data_product_access_report",
        "data_product_privacy_impact_assessment",
        "data_product_access_enforcement_certificate",
        "data_product_access_drift_report",
        "data_product_cost_gate",
        "data_product_cost_forecast",
        "data_product_cost_report",
        "data_product_ring_gate",
        "data_product_shadow_validation",
        "data_product_rollout_promotion",
        "data_product_rollout_report",
        "data_product_trust_gate",
        "data_product_trust_report",
        "data_product_trust_export",
        "data_product_remediation_gate",
        "data_product_remediation_runbook",
        "data_product_remediation_closeout",
        "data_product_remediation_report",
        "data_product_remediation_execution_plan",
        "data_product_remediation_execution_run",
        "data_product_remediation_execution_certificate",
        "data_product_remediation_execution_report",
        "recovery_point",
        "recovery_chain_verification",
        "fixture_build",
        "quality_profile",
        "promotion",
    }
)


def verify_gate(
    bundle: Mapping[str, Any], verification: Mapping[str, Any], policy: Any, checks: list[dict[str, Any]]
) -> tuple[str, ...]:
    blockers: list[str] = []
    if policy.require_attestation and not isinstance(bundle.get("attestation"), Mapping):
        blockers.append("migration_bundle_gate.attestation_required")
    if verification.get("status") == "blocked":
        blockers.append("migration_bundle_gate.verification_blocked")
        blockers.extend(str(item) for item in verification.get("blockers", []) if str(item))
    if bundle.get("status") == "blocked":
        blockers.append("migration_bundle_gate.bundle_blocked")
        blockers.extend(str(item) for item in bundle.get("blockers", []) if str(item))
    checks.append(check("bundle_integrity", "blocked" if blockers else "passed", blockers))
    return tuple(blockers)


def required_artifact_blockers(bundle: Mapping[str, Any], policy: Any, checks: list[dict[str, Any]]) -> tuple[str, ...]:
    present = artifact_kinds(bundle)
    blockers = tuple(
        f"migration_bundle_gate.required_artifact_missing:{kind}"
        for kind in policy.required_artifacts
        if kind not in present
    )
    checks.append(check("required_artifacts", "blocked" if blockers else "passed", blockers))
    return blockers


def approval_blockers(
    bundle: Mapping[str, Any],
    payloads: Mapping[str, Mapping[str, Any]],
    policy: Any,
    checks: list[dict[str, Any]],
) -> tuple[str, ...]:
    blockers: list[str] = []
    approval = payloads.get("approval", {})
    if policy.require_approved_risks == APPROVAL_IMPACT_REQUIRED:
        approved = _string_set(approval.get("approved_risks", []))
        for risk in _required_approvals(bundle, payloads):
            if risk not in approved:
                blockers.append(f"migration_bundle_gate.approval_required:{risk}")
    if policy.approval.require_approved_by and not str(approval.get("approved_by", "")).strip():
        blockers.append("migration_bundle_gate.approved_by_required")
    if policy.approval.require_not_expired:
        blockers.extend(_approval_expiry_blockers(approval))
    checks.append(check("approval", "blocked" if blockers else "passed", blockers))
    return tuple(blockers)


def promotion_blockers(
    payloads: Mapping[str, Mapping[str, Any]],
    policy: Any,
    checks: list[dict[str, Any]],
) -> tuple[str, ...]:
    if not policy.target_environment:
        checks.append(check("target_environment", "passed", ()))
        return ()
    promotion = payloads.get("promotion", {})
    actual = str(promotion.get("to_environment", ""))
    blockers = (
        (f"migration_bundle_gate.target_environment_mismatch:{actual}:{policy.target_environment}",)
        if actual != policy.target_environment
        else ()
    )
    checks.append(check("target_environment", "blocked" if blockers else "passed", blockers))
    return blockers


def trust_blockers(
    bundle: Mapping[str, Any],
    policy: Any,
    trust_verification: Mapping[str, Any] | None,
    checks: list[dict[str, Any]],
) -> tuple[str, ...]:
    if not policy.require_trusted_provenance and trust_verification is None:
        checks.append(check("trusted_provenance", "passed", ()))
        return ()
    if trust_verification is None:
        blockers = ("migration_bundle_gate.trust_verification_required",)
        checks.append(check("trusted_provenance", "blocked", blockers))
        return blockers
    blockers = _trust_status_blockers(bundle, policy, trust_verification)
    checks.append(check("trusted_provenance", "blocked" if blockers else "passed", blockers))
    return tuple(blockers)


def warnings(bundle: Mapping[str, Any], verification: Mapping[str, Any]) -> list[str]:
    return list(
        dict.fromkeys(
            [
                *(str(item) for item in bundle.get("warnings", []) if str(item)),
                *(str(item) for item in verification.get("warnings", []) if str(item)),
            ]
        )
    )


def unknown_artifact_warnings(bundle: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(
        f"migration_bundle_gate.unknown_artifact_kind:{kind}"
        for kind in sorted(artifact_kinds(bundle) - KNOWN_BUNDLE_ARTIFACTS)
    )


def artifact_kinds(bundle: Mapping[str, Any]) -> set[str]:
    raw = bundle.get("artifacts", [])
    if not isinstance(raw, list):
        return set()
    return {str(item.get("kind")) for item in raw if isinstance(item, Mapping) and item.get("kind")}


def check(name: str, status: str, details: tuple[str, ...] | list[str]) -> dict[str, Any]:
    return {"name": name, "status": status, "details": list(details)}


def recommendations(status: str, blockers: list[str], warnings_: list[str]) -> list[str]:
    if status == "blocked":
        return ["Resolve bundle gate blockers before running migration apply in protected environments."]
    if warnings_:
        return ["Review bundle gate warnings before promoting this migration."]
    return ["Use this gate receipt as a required CI check before migration apply."]


def _trust_status_blockers(bundle: Mapping[str, Any], policy: Any, trust_verification: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    if trust_verification.get("bundle_id") != bundle.get("bundle_id"):
        blockers.append("migration_bundle_gate.trust_bundle_id_mismatch")
    if trust_verification.get("pack_id") != bundle.get("pack_id"):
        blockers.append("migration_bundle_gate.trust_pack_id_mismatch")
    status = str(trust_verification.get("status", ""))
    if status == "blocked":
        blockers.append("migration_bundle_gate.trust_verification_blocked")
        blockers.extend(str(item) for item in trust_verification.get("blockers", []) if str(item))
    if policy.fail_on_warnings and status == "warning":
        blockers.append("migration_bundle_gate.trust_verification_warning")
    if policy.require_trusted_provenance and status != "trusted":
        blockers.append("migration_bundle_gate.trust_verification_not_trusted")
    return blockers


def _approval_expiry_blockers(approval: Mapping[str, Any]) -> tuple[str, ...]:
    raw = approval.get("expires_at")
    if not raw:
        return ("migration_bundle_gate.approval_expiry_required",)
    try:
        expires_at = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return ("migration_bundle_gate.approval_expiry_invalid",)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)  # noqa: UP017
    return ("migration_bundle_gate.approval_expired",) if expires_at <= datetime.now(timezone.utc) else ()  # noqa: UP017


def _required_approvals(bundle: Mapping[str, Any], payloads: Mapping[str, Mapping[str, Any]]) -> tuple[str, ...]:
    summary = bundle.get("summary", {}) if isinstance(bundle.get("summary"), Mapping) else {}
    raw = summary.get("required_approvals") or payloads.get("impact_plan", {}).get("required_approvals", [])
    return tuple(sorted(_string_set(raw)))


def _string_set(raw: object) -> set[str]:
    return {str(item) for item in raw if str(item)} if isinstance(raw, list) else set()


__all__ = [
    "approval_blockers",
    "check",
    "promotion_blockers",
    "recommendations",
    "required_artifact_blockers",
    "trust_blockers",
    "unknown_artifact_warnings",
    "verify_gate",
    "warnings",
]
