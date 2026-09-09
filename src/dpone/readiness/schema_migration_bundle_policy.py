"""Provider-neutral release policy gate for schema migration bundles."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint
from dpone.readiness.schema_migration_bundle_gate_checks import (
    approval_blockers,
    check,
    promotion_blockers,
    recommendations,
    required_artifact_blockers,
    trust_blockers,
    unknown_artifact_warnings,
    verify_gate,
    warnings,
)

BUNDLE_POLICY_SCHEMA = "dpone.schema_migration_bundle_policy.v1"
BUNDLE_GATE_SCHEMA = "dpone.schema_migration_bundle_gate.v1"
APPROVAL_IMPACT_REQUIRED = "impact_required"
APPROVAL_NONE = "none"


@dataclass(frozen=True, slots=True)
class MigrationBundleApprovalPolicy:
    require_approved_by: bool = False
    require_not_expired: bool = False

    @classmethod
    def from_mapping(
        cls, raw: Mapping[str, Any] | None, base: MigrationBundleApprovalPolicy
    ) -> MigrationBundleApprovalPolicy:
        if raw is None:
            return base
        return replace(
            base,
            require_approved_by=_bool(raw.get("require_approved_by"), base.require_approved_by),
            require_not_expired=_bool(raw.get("require_not_expired"), base.require_not_expired),
        )

    def to_dict(self) -> dict[str, bool]:
        return {
            "require_approved_by": self.require_approved_by,
            "require_not_expired": self.require_not_expired,
        }


@dataclass(frozen=True, slots=True)
class MigrationBundlePolicyOptions:
    profile: str
    require_attestation: bool
    required_artifacts: tuple[str, ...]
    require_approved_risks: str = APPROVAL_IMPACT_REQUIRED
    fail_on_warnings: bool = False
    require_trusted_provenance: bool = False
    target_environment: str | None = None
    approval: MigrationBundleApprovalPolicy = MigrationBundleApprovalPolicy()

    @classmethod
    def resolve(
        cls,
        *,
        profile: str | None = None,
        policy_payload: Mapping[str, Any] | None = None,
        target_environment: str | None = None,
    ) -> MigrationBundlePolicyOptions:
        from dpone.readiness.schema_migration_bundle_policy_profiles import MigrationBundlePolicyProfileRegistry

        raw_profile = str(profile or (policy_payload or {}).get("profile") or "pr_review")
        base = MigrationBundlePolicyProfileRegistry().resolve(raw_profile)
        if policy_payload is None:
            return replace(base, target_environment=target_environment or base.target_environment)
        if policy_payload.get("schema_version") not in {None, BUNDLE_POLICY_SCHEMA}:
            raise ValueError(f"unsupported policy schema: {policy_payload.get('schema_version')}")
        raw_required = policy_payload.get("required_artifacts")
        required = (
            base.required_artifacts if raw_required is None else _string_tuple(raw_required, "required_artifacts")
        )
        raw_approval = policy_payload.get("approval")
        if raw_approval is not None and not isinstance(raw_approval, Mapping):
            raise ValueError("approval policy must be an object")
        return replace(
            base,
            require_attestation=_bool(policy_payload.get("require_attestation"), base.require_attestation),
            required_artifacts=required,
            require_approved_risks=str(policy_payload.get("require_approved_risks", base.require_approved_risks)),
            fail_on_warnings=_bool(policy_payload.get("fail_on_warnings"), base.fail_on_warnings),
            require_trusted_provenance=_bool(
                policy_payload.get("require_trusted_provenance"), base.require_trusted_provenance
            ),
            target_environment=target_environment
            or _optional_string(policy_payload.get("target_environment"))
            or base.target_environment,
            approval=MigrationBundleApprovalPolicy.from_mapping(raw_approval, base.approval),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "require_attestation": self.require_attestation,
            "required_artifacts": list(self.required_artifacts),
            "require_approved_risks": self.require_approved_risks,
            "fail_on_warnings": self.fail_on_warnings,
            "require_trusted_provenance": self.require_trusted_provenance,
            "target_environment": self.target_environment,
            "approval": self.approval.to_dict(),
        }


class MigrationBundlePolicyProfileRegistry:
    """Compatibility facade over the built-in profile catalog."""

    def resolve(self, profile: str | None) -> MigrationBundlePolicyOptions:
        from dpone.readiness.schema_migration_bundle_policy_profiles import (
            MigrationBundlePolicyProfileRegistry as _Registry,
        )

        return _Registry().resolve(profile)

    def names(self) -> tuple[str, ...]:
        from dpone.readiness.schema_migration_bundle_policy_profiles import (
            MigrationBundlePolicyProfileRegistry as _Registry,
        )

        return _Registry().names()


class MigrationBundlePolicyEvaluator:
    """Evaluates bundle integrity evidence against one release policy."""

    def evaluate(
        self,
        *,
        bundle: Mapping[str, Any],
        verification: Mapping[str, Any],
        policy: MigrationBundlePolicyOptions,
        artifact_payloads: Mapping[str, Mapping[str, Any]],
        trust_verification: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        blockers: list[str] = []
        gate_warnings = warnings(bundle, verification)
        checks: list[dict[str, Any]] = []
        blockers.extend(verify_gate(bundle, verification, policy, checks))
        blockers.extend(required_artifact_blockers(bundle, policy, checks))
        blockers.extend(approval_blockers(bundle, artifact_payloads, policy, checks))
        blockers.extend(promotion_blockers(artifact_payloads, policy, checks))
        blockers.extend(trust_blockers(bundle, policy, trust_verification, checks))
        if unknown := unknown_artifact_warnings(bundle):
            gate_warnings.extend(unknown)
            checks.append(check("unknown_artifact_kinds", "warning", unknown))
        if policy.fail_on_warnings and gate_warnings:
            blockers.append("migration_bundle_gate.warnings_present")
            checks.append(check("warnings", "blocked", gate_warnings))
        status = "blocked" if blockers else "warning" if gate_warnings else "allowed"
        blockers = list(dict.fromkeys(blockers))
        gate_warnings = list(dict.fromkeys(gate_warnings))
        decision: dict[str, Any] = {
            "schema_version": BUNDLE_GATE_SCHEMA,
            "status": status,
            "profile": policy.profile,
            "bundle_id": bundle.get("bundle_id"),
            "pack_id": bundle.get("pack_id"),
            "target": dict(bundle.get("target", {})) if isinstance(bundle.get("target"), Mapping) else {},
            "checks": checks,
            "blockers": blockers,
            "warnings": gate_warnings,
            "recommendations": recommendations(status, blockers, gate_warnings),
        }
        decision["gate_id"] = stable_fingerprint(
            {
                "bundle_id": decision["bundle_id"],
                "profile": policy.profile,
                "policy": policy.to_dict(),
                "checks": checks,
                "blockers": blockers,
                "warnings": gate_warnings,
            }
        )
        return decision


def policy_error_decision(*, profile: str | None, message: str) -> dict[str, Any]:
    blocker = f"migration_bundle_gate.policy_invalid:{message}"
    decision: dict[str, Any] = {
        "schema_version": BUNDLE_GATE_SCHEMA,
        "status": "blocked",
        "profile": profile or "pr_review",
        "bundle_id": None,
        "pack_id": None,
        "target": {},
        "checks": [check("policy", "blocked", (blocker,))],
        "blockers": [blocker],
        "warnings": [],
        "recommendations": ["Fix the bundle policy file before using it as a CI required check."],
    }
    decision["gate_id"] = stable_fingerprint({"profile": decision["profile"], "blockers": decision["blockers"]})
    return decision


def _string_tuple(raw: object, field: str) -> tuple[str, ...]:
    if not isinstance(raw, list):
        raise ValueError(f"{field} must be a list")
    values = tuple(str(item) for item in raw if str(item))
    if not values:
        raise ValueError(f"{field} must not be empty")
    return values


def _bool(raw: object, default: bool) -> bool:
    return default if raw is None else bool(raw)


def _optional_string(raw: object) -> str | None:
    return str(raw) if raw not in {None, ""} else None


__all__ = [
    "BUNDLE_GATE_SCHEMA",
    "BUNDLE_POLICY_SCHEMA",
    "MigrationBundleApprovalPolicy",
    "MigrationBundlePolicyEvaluator",
    "MigrationBundlePolicyOptions",
    "MigrationBundlePolicyProfileRegistry",
    "policy_error_decision",
]
