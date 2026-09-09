"""Pure acceptance policy for Airflow self-service evidence."""

from __future__ import annotations

from dpone.ops.self_service_certification_models import (
    ReferenceDeploymentEvidence,
    UsabilityEvidence,
)


def overall_status(usability: UsabilityEvidence, references: ReferenceDeploymentEvidence) -> str:
    """Combine independent gates without allowing missing evidence to pass."""

    statuses = {usability.status, references.status}
    if "FAIL" in statuses:
        return "FAIL"
    if statuses == {"PASS"}:
        return "PASS"
    return "UNVERIFIED"


__all__ = ["overall_status"]
