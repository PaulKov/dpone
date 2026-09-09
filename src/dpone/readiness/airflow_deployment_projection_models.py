"""Public result and identity helpers for Airflow deployment projections."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment import (
    canonical_fingerprint,
    deployment_id,
    release_id,
)


@dataclass(frozen=True)
class AirflowDeploymentProjection:
    """One immutable deployment projection and its authenticated fingerprints."""

    deployment_dir: Path
    deployment: dict[str, Any]
    airflow_index: dict[str, Any]
    binding_set_fingerprint: str
    connection_registry_fingerprint: str
    credential_runtime_fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment_dir": self.deployment_dir.as_posix(),
            "deployment": self.deployment,
            "airflow_index": self.airflow_index,
            "binding_set_fingerprint": self.binding_set_fingerprint,
            "connection_registry_fingerprint": self.connection_registry_fingerprint,
            "credential_runtime_fingerprint": self.credential_runtime_fingerprint,
        }


def compute_release_id(payload: dict[str, Any]) -> str:
    return release_id(payload)


def compute_deployment_id(payload: dict[str, Any]) -> str:
    return deployment_id(payload)


def deployment_payload_fingerprint(payload: dict[str, Any]) -> str:
    """Fingerprint one environment projection with the canonical algorithm."""

    return canonical_fingerprint(payload)


__all__ = [
    "AirflowDeploymentProjection",
    "compute_deployment_id",
    "compute_release_id",
    "deployment_payload_fingerprint",
]
