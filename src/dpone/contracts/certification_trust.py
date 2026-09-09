"""Pure trust rules for behavioral and certification evidence."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

EVIDENCE_STATUSES = frozenset({"PASS", "FAIL", "SKIP", "UNVERIFIED"})
_LOCAL_ONLY_SCHEMAS = frozenset(
    {
        "dpone.dbt_sqlserver.wide_materialization.v1",
        "dpone.local_route_certification_receipt.v1",
        "dpone.mssql-clickhouse-wide-release-authority.v1",
        "dpone.mssql-clickhouse-wide-release-campaign.v1",
    }
)
_SCHEMA_BOUND_DIGEST_FIELDS = frozenset(
    {
        "authority_sha256",
        "campaign_sha256",
        "evidence_sha256",
        "receipt_sha256",
        "source_snapshot_sha256",
    }
)


@dataclass(frozen=True, slots=True)
class CertificationTrustDecision:
    """Separate behavioral success from trusted certification evidence."""

    passed: bool
    behavior_passed: bool
    evidence_status: str
    reason_code: str


def certification_trust(payload: Mapping[str, Any]) -> CertificationTrustDecision:
    raw_status = payload.get("evidence_status")
    evidence_status = raw_status if isinstance(raw_status, str) and raw_status in EVIDENCE_STATUSES else "UNVERIFIED"
    behavior_passed = payload.get("passed") is True and not _has_certification_contradiction(payload)
    if not behavior_passed:
        reason_code = "certification_behavior_failed"
    elif evidence_status != "PASS":
        reason_code = "certification_evidence_not_verified"
    else:
        reason_code = "certification_evidence_passed"
    return CertificationTrustDecision(
        passed=behavior_passed and evidence_status == "PASS",
        behavior_passed=behavior_passed,
        evidence_status=evidence_status,
        reason_code=reason_code,
    )


def is_local_only_evidence(payload: Mapping[str, Any]) -> bool:
    """Return whether a payload retains identity from a local-only contract."""

    schema = payload.get("schema_version", payload.get("schema"))
    return schema in _LOCAL_ONLY_SCHEMAS or (
        schema is None and any(field in payload for field in _SCHEMA_BOUND_DIGEST_FIELDS)
    )


def _has_certification_contradiction(payload: Mapping[str, Any]) -> bool:
    production_certification = payload.get("production_certification")
    if "production_certification" in payload and (
        not isinstance(production_certification, str) or production_certification.strip().upper() != "VERIFIED"
    ):
        return True
    environment_class = payload.get("environment_class")
    if isinstance(environment_class, str) and environment_class.strip().upper() == "LOCAL_DOCKER":
        return True
    if is_local_only_evidence(payload):
        # Legacy unversioned evidence remains compatible, but a durable hashed
        # envelope cannot shed its schema and be reinterpreted as legacy PASS.
        return True
    if _has_values(payload, "blockers", "violations", "findings", "errors", "failed_stages"):
        return True
    raw_status = payload.get("status")
    if isinstance(raw_status, str) and raw_status.strip().lower() in {
        "blocked",
        "failed",
        "regression",
        "rollback_required",
    }:
        return True
    stages = payload.get("stages")
    if not isinstance(stages, list | tuple):
        return False
    return any(_stage_failed(stage) for stage in stages)


def _has_values(payload: Mapping[str, Any], *keys: str) -> bool:
    return any(bool(payload.get(key)) for key in keys)


def _stage_failed(stage: object) -> bool:
    if not isinstance(stage, Mapping):
        return True
    required = stage.get("required") is not False
    if not required:
        return False
    return stage.get("passed") is not True or _has_values(stage, "blockers", "violations", "findings", "errors")


__all__ = [
    "EVIDENCE_STATUSES",
    "CertificationTrustDecision",
    "certification_trust",
    "is_local_only_evidence",
]
