"""Stable value objects returned by deployment cache operations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class CurrentDeployment:
    """One activated deployment and its durable promotion provenance."""

    activation_id: str | None
    deployment_id: str
    release_id: str
    environment: str
    current_path: Path
    pointer_path: Path
    promoted_by: str
    promoted_at: str
    previous_deployment_id: str | None = None
    source_commit: str | None = None
    attestation_ref: str | None = None
    workspace_authority_connection_ref: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema": "dpone.current-pointer.v1",
            "activation_id": self.activation_id,
            "environment": self.environment,
            "deployment_id": self.deployment_id,
            "release_id": self.release_id,
            "current_path": self.current_path.as_posix(),
            "pointer_path": self.pointer_path.as_posix(),
            "promoted_by": self.promoted_by,
            "promoted_at": self.promoted_at,
            "previous_deployment_id": self.previous_deployment_id,
            "source_commit": self.source_commit,
            "attestation_ref": self.attestation_ref,
            "workspace_authority_connection_ref": self.workspace_authority_connection_ref,
        }
        return {key: value for key, value in payload.items() if value is not None}


@dataclass(frozen=True, slots=True)
class ValidatedDeploymentProjection:
    """Deployment and index payloads accepted by the integrity validator."""

    deployment: dict[str, Any]
    airflow_index: dict[str, Any]
    # Observed from the integrity-checked release, never an index/caller hint.
    dbt_runtime_wire_contract: str | None = None

    @property
    def deployment_id(self) -> str:
        return str(self.deployment["deployment_id"])

    @property
    def release_id(self) -> str:
        return str(self.deployment["release_ref"])

    def identity(self) -> dict[str, str]:
        return {"deployment_id": self.deployment_id, "release_id": self.release_id}


__all__ = ["CurrentDeployment", "ValidatedDeploymentProjection"]
