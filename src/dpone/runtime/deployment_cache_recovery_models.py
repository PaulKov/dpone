"""Public reports and issues for local deployment cache recovery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


class DeploymentCacheRecoveryApplyError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        path: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.path = path
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class DeploymentCacheRecoveryIssue:
    code: str
    severity: str
    message: str
    path: str

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "path": self.path,
        }


@dataclass(frozen=True, slots=True)
class DeploymentCacheRecoveryCandidate:
    deployment_id: str
    path: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "deployment_id": self.deployment_id,
            "path": self.path,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class DeploymentCacheRecoveryPlan:
    environment: str
    status: str
    current_deployment_id: str | None
    current_path_deployment_id: str | None
    preferred_repair_deployment_id: str | None
    issues: tuple[DeploymentCacheRecoveryIssue, ...]
    repair_candidates: tuple[DeploymentCacheRecoveryCandidate, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.deployment-cache-recovery-plan.v1",
            "environment": self.environment,
            "status": self.status,
            "current_deployment_id": self.current_deployment_id,
            "current_path_deployment_id": self.current_path_deployment_id,
            "preferred_repair_deployment_id": self.preferred_repair_deployment_id,
            "issues": [issue.to_dict() for issue in self.issues],
            "repair_candidates": [candidate.to_dict() for candidate in self.repair_candidates],
        }


@dataclass(frozen=True, slots=True)
class DeploymentCacheRecoveryApplyReport:
    environment: str
    recovered_deployment_id: str
    release_id: str
    current_path: str
    pointer_path: str

    def to_dict(self) -> dict[str, str]:
        return {
            "schema": "dpone.deployment-cache-recovery-apply.v1",
            "environment": self.environment,
            "recovered_deployment_id": self.recovered_deployment_id,
            "release_id": self.release_id,
            "current_path": self.current_path,
            "pointer_path": self.pointer_path,
        }


__all__ = [
    "DeploymentCacheRecoveryApplyError",
    "DeploymentCacheRecoveryApplyReport",
    "DeploymentCacheRecoveryCandidate",
    "DeploymentCacheRecoveryIssue",
    "DeploymentCacheRecoveryPlan",
]
