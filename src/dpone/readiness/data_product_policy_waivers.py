"""Waiver lifecycle helpers for data product policy gates."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

WAIVER_REQUEST_SCHEMA = "dpone.data_product_waiver_request.v1"
WAIVER_SCHEMA = "dpone.data_product_waiver.v1"


class WaiverRequestBuilder:
    """Builds deterministic waiver requests for failed or warning policy rules."""

    def request(
        self,
        *,
        evaluation: Mapping[str, Any],
        rule_id: str,
        reason: str,
        expires_at: str,
        requested_by: str | None = None,
    ) -> dict[str, Any]:
        blockers: list[str] = []
        rule = _rule(evaluation, rule_id)
        if not rule:
            blockers.append(f"data_product_policy.rule_not_found:{rule_id}")
        elif rule.get("status") not in {"failed", "warning"}:
            blockers.append(f"data_product_policy.rule_not_waivable:{rule_id}")
        if not reason.strip():
            blockers.append("data_product_policy.waiver_reason_required")
        if not _parse_time(expires_at):
            blockers.append("data_product_policy.waiver_expires_at_invalid")
        payload: dict[str, Any] = {
            "schema_version": WAIVER_REQUEST_SCHEMA,
            "status": "blocked" if blockers else "requested",
            "policy_evaluation_id": evaluation.get("policy_evaluation_id"),
            "policy_pack_id": evaluation.get("policy_pack_id"),
            "product_id": _product_id(evaluation),
            "pack_id": evaluation.get("pack_id"),
            "bundle_id": evaluation.get("bundle_id"),
            "rule_id": rule_id,
            "severity": rule.get("severity") if rule else None,
            "reason": reason,
            "expires_at": expires_at,
            "requested_by": requested_by,
            "waiver_policy": dict(evaluation.get("waiver_policy", {}))
            if isinstance(evaluation.get("waiver_policy"), Mapping)
            else {},
            "blockers": blockers,
            "warnings": [],
        }
        payload["waiver_request_id"] = stable_fingerprint(payload)
        return payload


class WaiverApprover:
    """Approves waiver requests using local approval evidence only."""

    def approve(
        self,
        *,
        request: Mapping[str, Any],
        actor: str,
        approval: Mapping[str, Any],
        authority_check: Mapping[str, Any] | None = None,
        approval_quorum: Mapping[str, Any] | None = None,
        approved_at: str | None = None,
    ) -> dict[str, Any]:
        approved = approved_at or _utc_now()
        blockers = _approval_blockers(request=request, actor=actor, approval=approval, approved_at=approved)
        blockers.extend(_authority_blockers(authority_check=authority_check, approval_quorum=approval_quorum))
        payload: dict[str, Any] = {
            "schema_version": WAIVER_SCHEMA,
            "status": "blocked" if blockers else "approved",
            "waiver_request_id": request.get("waiver_request_id"),
            "policy_evaluation_id": request.get("policy_evaluation_id"),
            "policy_pack_id": request.get("policy_pack_id"),
            "product_id": request.get("product_id"),
            "pack_id": request.get("pack_id"),
            "bundle_id": request.get("bundle_id"),
            "rule_id": request.get("rule_id"),
            "severity": request.get("severity"),
            "reason": request.get("reason"),
            "expires_at": request.get("expires_at"),
            "approved_at": approved,
            "actor": actor,
            "approval_evidence_id": stable_fingerprint(dict(approval)) if approval else None,
            "authority_evidence": _authority_evidence(authority_check=authority_check, approval_quorum=approval_quorum),
            "blockers": blockers,
            "warnings": [],
        }
        payload["waiver_id"] = stable_fingerprint(payload)
        return payload


def waiver_covers(
    *,
    waiver: Mapping[str, Any],
    evaluation: Mapping[str, Any],
    rule_id: str,
    observed_at: str | None,
) -> tuple[bool, tuple[str, ...]]:
    """Return whether a waiver covers a rule plus any validation blockers."""

    blockers: list[str] = []
    if waiver.get("status") != "approved":
        blockers.append(f"data_product_policy.waiver_not_approved:{rule_id}")
    if waiver.get("rule_id") != rule_id:
        return False, ()
    if waiver.get("policy_evaluation_id") != evaluation.get("policy_evaluation_id"):
        blockers.append(f"data_product_policy.waiver_evaluation_mismatch:{rule_id}")
    expires = _parse_time(str(waiver.get("expires_at") or ""))
    observed = _parse_time(observed_at or _utc_now())
    if expires is None:
        blockers.append(f"data_product_policy.waiver_expiry_invalid:{rule_id}")
    elif observed is not None and expires < observed:
        blockers.append(f"data_product_policy.waiver_expired:{rule_id}")
    return not blockers, tuple(blockers)


def _approval_blockers(
    *,
    request: Mapping[str, Any],
    actor: str,
    approval: Mapping[str, Any],
    approved_at: str,
) -> list[str]:
    blockers: list[str] = []
    if request.get("status") != "requested":
        blockers.append("data_product_policy.waiver_request_not_requested")
    if not actor.strip():
        blockers.append("data_product_policy.waiver_actor_required")
    if not approval:
        blockers.append("data_product_policy.waiver_approval_evidence_required")
    approved = _parse_time(approved_at)
    expires = _parse_time(str(request.get("expires_at") or ""))
    if not approved:
        blockers.append("data_product_policy.waiver_approved_at_invalid")
    if expires is None:
        blockers.append("data_product_policy.waiver_expires_at_invalid")
    elif approved is not None and _exceeds_max_duration(request, approved=approved, expires=expires):
        blockers.append("data_product_policy.waiver_duration_exceeds_policy")
    return blockers


def _authority_blockers(
    *,
    authority_check: Mapping[str, Any] | None,
    approval_quorum: Mapping[str, Any] | None,
) -> list[str]:
    blockers: list[str] = []
    if authority_check and authority_check.get("status") == "blocked":
        blockers.append("data_product_policy.authority_check_blocked")
        blockers.extend(str(item) for item in authority_check.get("blockers", []) if str(item))
    if approval_quorum and approval_quorum.get("status") == "blocked":
        blockers.append("data_product_policy.approval_quorum_blocked")
        blockers.extend(str(item) for item in approval_quorum.get("blockers", []) if str(item))
    return blockers


def _authority_evidence(
    *,
    authority_check: Mapping[str, Any] | None,
    approval_quorum: Mapping[str, Any] | None,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    if authority_check:
        evidence["authority_check_id"] = authority_check.get("authority_check_id")
        evidence["authority_check_status"] = authority_check.get("status")
    if approval_quorum:
        evidence["approval_quorum_id"] = approval_quorum.get("approval_quorum_id")
        evidence["approval_quorum_status"] = approval_quorum.get("status")
    return evidence


def _exceeds_max_duration(request: Mapping[str, Any], *, approved: datetime, expires: datetime) -> bool:
    policy = request.get("waiver_policy")
    if not isinstance(policy, Mapping):
        return False
    try:
        max_days = float(policy.get("max_duration_days", 0))
    except (TypeError, ValueError):
        return False
    return max_days > 0 and (expires - approved).total_seconds() > max_days * 24 * 60 * 60


def _rule(evaluation: Mapping[str, Any], rule_id: str) -> Mapping[str, Any] | None:
    rules = evaluation.get("rules")
    if not isinstance(rules, list):
        return None
    return next((item for item in rules if isinstance(item, Mapping) and item.get("id") == rule_id), None)


def _product_id(evaluation: Mapping[str, Any]) -> str | None:
    product = evaluation.get("product")
    return str(product.get("id")) if isinstance(product, Mapping) and product.get("id") else None


def _parse_time(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


__all__ = [
    "WAIVER_REQUEST_SCHEMA",
    "WAIVER_SCHEMA",
    "WaiverApprover",
    "WaiverRequestBuilder",
    "waiver_covers",
]
