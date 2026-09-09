"""Immutable lifecycle evaluation receipt for semantic refresh V2."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.dbt_contract_validation import canonical_fingerprint, require_digest
from dpone.contracts.dbt_semantic_refresh_common import (
    ProofStatus,
    SemanticRefreshProofIssue,
    proof_status,
)

SQLSERVER_LIFECYCLE_REPORT_SCHEMA = "dpone.dbt-semantic-refresh-lifecycle-report.v1"


@dataclass(frozen=True, slots=True)
class SemanticRefreshLifecycleReport:
    """Fail-closed lifecycle outcome bound to policy and observed authority."""

    status: ProofStatus
    lifecycle_policy_sha256: str
    lifecycle_authority_sha256: str
    lifecycle_evaluation_sha256: str
    certification_coordinate_sha256: str
    issues: tuple[SemanticRefreshProofIssue, ...]
    schema: str = SQLSERVER_LIFECYCLE_REPORT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != SQLSERVER_LIFECYCLE_REPORT_SCHEMA:
            raise ValueError("semantic-refresh lifecycle report schema is unsupported")
        if proof_status(self.issues) != self.status:
            raise ValueError("semantic-refresh lifecycle status differs from its issue closure")
        for field in (
            "lifecycle_policy_sha256",
            "lifecycle_authority_sha256",
            "lifecycle_evaluation_sha256",
            "certification_coordinate_sha256",
        ):
            require_digest(getattr(self, field), field, "DPONE_DBT_V2_LIFECYCLE_REPORT_INVALID")
        if self.lifecycle_evaluation_sha256 != canonical_fingerprint(self._unsigned()):
            raise ValueError("semantic-refresh lifecycle report digest differs from its content")

    @classmethod
    def build(
        cls,
        *,
        status: ProofStatus,
        lifecycle_policy_sha256: str,
        lifecycle_authority_sha256: str,
        certification_coordinate_sha256: str,
        issues: tuple[SemanticRefreshProofIssue, ...],
    ) -> SemanticRefreshLifecycleReport:
        """Build one content-addressed lifecycle evaluation receipt."""

        unsigned = lifecycle_report_payload(
            status,
            lifecycle_policy_sha256,
            lifecycle_authority_sha256,
            certification_coordinate_sha256,
            issues,
        )
        return cls(
            status,
            lifecycle_policy_sha256,
            lifecycle_authority_sha256,
            canonical_fingerprint(unsigned),
            certification_coordinate_sha256,
            issues,
        )

    def _unsigned(self) -> dict[str, object]:
        return lifecycle_report_payload(
            self.status,
            self.lifecycle_policy_sha256,
            self.lifecycle_authority_sha256,
            self.certification_coordinate_sha256,
            self.issues,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the closed immutable lifecycle evidence projection."""

        return {**self._unsigned(), "lifecycle_evaluation_sha256": self.lifecycle_evaluation_sha256}


def lifecycle_report_payload(
    status: ProofStatus,
    lifecycle_policy_sha256: str,
    lifecycle_authority_sha256: str,
    certification_coordinate_sha256: str,
    issues: tuple[SemanticRefreshProofIssue, ...],
) -> dict[str, object]:
    """Return the canonical unsigned lifecycle receipt payload."""

    return {
        "certification_coordinate_sha256": certification_coordinate_sha256,
        "issues": [item.to_jsonable() for item in issues],
        "lifecycle_authority_sha256": lifecycle_authority_sha256,
        "lifecycle_policy_sha256": lifecycle_policy_sha256,
        "schema": SQLSERVER_LIFECYCLE_REPORT_SCHEMA,
        "status": status,
    }
