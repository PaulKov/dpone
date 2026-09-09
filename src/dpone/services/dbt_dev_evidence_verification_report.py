"""Immutable report contract for dbt dev-evidence verification."""

from __future__ import annotations

from dataclasses import dataclass

DBT_DEV_EVIDENCE_VERIFIED = "DPONE_DBT_DEV_EVIDENCE_VERIFIED"
DBT_DEV_EVIDENCE_UNVERIFIED = "DPONE_DBT_DEV_EVIDENCE_UNVERIFIED"
_REPORT_SCHEMA = "dpone.dbt-dev-evidence-verification.v1"


@dataclass(frozen=True, slots=True)
class DbtDevEvidenceVerificationReport:
    """Bounded proof that every workload in a release passed in dev."""

    code: str
    release_id: str
    deployment_id: str
    evidence_set_id: str | None = None
    required_workloads: tuple[str, ...] = ()
    verified_workloads: tuple[str, ...] = ()
    verified_dbt_workflows: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    exact_activation: bool = False

    @property
    def passed(self) -> bool:
        return self.code == DBT_DEV_EVIDENCE_VERIFIED

    def to_dict(self) -> dict[str, object]:
        exact_schema = self.exact_activation and self.evidence_set_id is not None
        payload: dict[str, object] = {
            "schema": ("dpone.dbt-dev-evidence-verification.v2" if exact_schema else _REPORT_SCHEMA),
            "status": "passed" if self.passed else "unverified",
            "passed": self.passed,
            "code": self.code,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "required_workloads": list(self.required_workloads),
            "verified_workloads": list(self.verified_workloads),
            "verified_dbt_workflows": list(self.verified_dbt_workflows),
            "reason_codes": list(self.reason_codes),
        }
        if exact_schema:
            payload["evidence_set_id"] = self.evidence_set_id
        return payload


def unverified_report(
    release_id: str,
    deployment_id: str,
    reason: str,
    *,
    evidence_set_id: str | None = None,
    required: tuple[str, ...] = (),
    verified: tuple[str, ...] = (),
) -> DbtDevEvidenceVerificationReport:
    """Build one fail-closed report without duplicating identity fields."""

    return DbtDevEvidenceVerificationReport(
        code=DBT_DEV_EVIDENCE_UNVERIFIED,
        release_id=release_id,
        deployment_id=deployment_id,
        evidence_set_id=evidence_set_id,
        required_workloads=required,
        verified_workloads=verified,
        reason_codes=(reason,),
    )


__all__ = [
    "DBT_DEV_EVIDENCE_UNVERIFIED",
    "DBT_DEV_EVIDENCE_VERIFIED",
    "DbtDevEvidenceVerificationReport",
    "unverified_report",
]
