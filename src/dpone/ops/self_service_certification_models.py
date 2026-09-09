"""Public value objects for Airflow self-service certification evidence."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

SELF_SERVICE_CERTIFICATION_SCHEMA = "dpone.self-service-certification.v1"
EVIDENCE_STATUSES = ("PASS", "FAIL", "UNVERIFIED")


class SelfServiceCertificationError(ValueError):
    """Safe public failure raised for invalid invocation or publication paths."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class UsabilityThresholds:
    minimum_participants: int = 5
    minimum_success_rate: float = 0.8
    maximum_first_dag_seconds: int = 600
    maximum_safe_sample_seconds: int = 900
    maximum_commands: int = 5

    def to_dict(self) -> dict[str, int | float]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class UsabilityEvidence:
    status: str
    supplied: bool
    source_sha256: str | None
    valid_sessions: int
    successful_sessions: int
    complete_success_rate: float | None
    first_dag_target_rate: float | None
    safe_sample_target_rate: float | None
    no_airflow_python_rate: float | None
    p50_first_dag_seconds: float | None
    p50_safe_sample_seconds: float | None
    thresholds: UsabilityThresholds
    blockers: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "supplied": self.supplied,
            "source_sha256": self.source_sha256,
            "valid_sessions": self.valid_sessions,
            "successful_sessions": self.successful_sessions,
            "complete_success_rate": self.complete_success_rate,
            "first_dag_target_rate": self.first_dag_target_rate,
            "safe_sample_target_rate": self.safe_sample_target_rate,
            "no_airflow_python_rate": self.no_airflow_python_rate,
            "p50_first_dag_seconds": self.p50_first_dag_seconds,
            "p50_safe_sample_seconds": self.p50_safe_sample_seconds,
            "thresholds": self.thresholds.to_dict(),
            "blockers": list(self.blockers),
        }


@dataclass(frozen=True, slots=True)
class ReferenceDeploymentProof:
    evidence_set: str
    status: str
    release_id: str | None
    deployment_id: str | None
    signer_identity: str | None
    correlation_id: str | None
    release_set_sha256: str | None
    deployment_set_sha256: str | None
    airflow_evidence_sha256: str | None
    blockers: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["blockers"] = list(self.blockers)
        return payload


@dataclass(frozen=True, slots=True)
class ReferenceDeploymentEvidence:
    status: str
    supplied_count: int
    valid_count: int
    distinct_deployment_count: int
    distinct_signer_count: int
    proofs: tuple[ReferenceDeploymentProof, ...]
    blockers: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "supplied_count": self.supplied_count,
            "valid_count": self.valid_count,
            "distinct_deployment_count": self.distinct_deployment_count,
            "distinct_signer_count": self.distinct_signer_count,
            "proofs": [proof.to_dict() for proof in self.proofs],
            "blockers": list(self.blockers),
        }


@dataclass(frozen=True, slots=True)
class SelfServiceCertificationReport:
    expected_commit: str
    evaluated_at: str
    overall_status: str
    has_input_failures: bool
    usability: UsabilityEvidence
    reference_deployments: ReferenceDeploymentEvidence
    output_dir: str

    @property
    def json_path(self) -> Path:
        return Path(self.output_dir) / "self-service-certification.json"

    @property
    def markdown_path(self) -> Path:
        return Path(self.output_dir) / "self-service-certification.md"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": SELF_SERVICE_CERTIFICATION_SCHEMA,
            "expected_commit": self.expected_commit,
            "evaluated_at": self.evaluated_at,
            "overall_status": self.overall_status,
            "has_input_failures": self.has_input_failures,
            "usability": self.usability.to_dict(),
            "reference_deployments": self.reference_deployments.to_dict(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        usability = self.usability
        references = self.reference_deployments
        blockers = tuple(dict.fromkeys((*usability.blockers, *references.blockers)))
        lines = [
            "# dpone Airflow self-service certification",
            "",
            f"- Expected commit: `{self.expected_commit}`",
            f"- Evaluated at: `{self.evaluated_at}`",
            f"- Overall status: **{self.overall_status}**",
            "",
            "| Gate | Status | Evidence |",
            "|---|---|---|",
            (
                f"| Five-user usability | **{usability.status}** | "
                f"{usability.successful_sessions}/{usability.valid_sessions} complete sessions |"
            ),
            (
                f"| Production references | **{references.status}** | "
                f"{references.valid_count}/{references.supplied_count} valid; "
                f"{references.distinct_deployment_count} deployments; "
                f"{references.distinct_signer_count} signers |"
            ),
            "",
            "## Blockers",
            "",
        ]
        lines.extend(f"- `{blocker}`" for blocker in blockers)
        if not blockers:
            lines.append("- none")
        lines.extend(
            [
                "",
                "## Interpretation",
                "",
                "- Missing human or production evidence is `UNVERIFIED`, never `PASS`.",
                "- Repair authoritative inputs and republish; do not edit this generated report.",
                "",
            ]
        )
        return "\n".join(lines)


__all__ = [
    "EVIDENCE_STATUSES",
    "SELF_SERVICE_CERTIFICATION_SCHEMA",
    "ReferenceDeploymentEvidence",
    "ReferenceDeploymentProof",
    "SelfServiceCertificationError",
    "SelfServiceCertificationReport",
    "UsabilityEvidence",
    "UsabilityThresholds",
]
