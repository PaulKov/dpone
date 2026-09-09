"""Closed domain contracts for trusted PR Gate shadow audit receipts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Final, Literal

from dpone.contracts.strict_json import canonical_json_bytes

AuditDecision = Literal["PASS", "FAIL", "UNVERIFIED"]
AuditStage = Literal["EVENT_RECEIVED", "RUN_AUTHENTICATED", "PLAN_RECOMPUTED", "JOBS_BOUND", "CLAIMS_BOUND"]

AUDIT_STAGES: Final[tuple[AuditStage, ...]] = (
    "EVENT_RECEIVED",
    "RUN_AUTHENTICATED",
    "PLAN_RECOMPUTED",
    "JOBS_BOUND",
    "CLAIMS_BOUND",
)
AUDITOR_MERGE_REF_UNVERIFIED: Final = "AUDITOR_MERGE_REF_UNVERIFIED"
AUDITOR_RUN_UNVERIFIED: Final = "AUDITOR_RUN_UNVERIFIED"
AUDITOR_CLAIMS_UNVERIFIED: Final = "AUDITOR_CLAIMS_UNVERIFIED"
AUDITOR_JOBS_UNVERIFIED: Final = "AUDITOR_JOBS_UNVERIFIED"
AUDITOR_BUNDLE_UNAPPROVED: Final = "AUDITOR_BUNDLE_UNAPPROVED"
PROVENANCE_APPROVAL_PENDING: Final = "PROVENANCE_APPROVAL_PENDING"


def eligible_set_digest(records: object) -> str:
    """Return the v1-compatible digest for an exact eligible-PR record set."""

    return "sha256:" + hashlib.sha256(canonical_json_bytes(records)).hexdigest()


@dataclass(frozen=True)
class MergeTuple:
    """One fully authenticated current pull-request merge observation."""

    repository_id: int
    pr_number: int
    base_sha: str
    head_sha: str
    merge_sha: str
    eligible_set_digest: str


@dataclass(frozen=True)
class AuditResult:
    """The decision and durable receipt payload produced by an audit attempt."""

    decision: AuditDecision
    receipt: dict[str, object]
