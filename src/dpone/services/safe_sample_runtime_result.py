"""Serializable result contract for one safe-sample runtime execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class SafeSampleRuntimeExecutionResult:
    """Secret-free execution result persisted as runtime evidence."""

    execution_status: str
    data_outcome: str
    release_id: str | None
    deployment_id: str | None
    init_fetch: dict[str, Any] | None
    temporary_target_prepare: dict[str, Any] | None
    data_copy: dict[str, Any] | None
    temporary_target_cleanup: dict[str, Any] | None
    errors: tuple[dict[str, Any], ...]
    execution_mode: str = "local_handoff"
    deployment_identity: dict[str, Any] = field(default_factory=dict)
    route_attestation_verification: dict[str, Any] | None = None
    source_snapshot: dict[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        deployment_identity = self.deployment_identity or {
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "workload_packs": [],
            "runtime_artifact_delivery": {},
        }
        return {
            "schema": "dpone.safe-sample-runtime-execution.v1",
            "execution_mode": self.execution_mode,
            "execution_status": self.execution_status,
            "data_outcome": self.data_outcome,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "deployment_identity": dict(deployment_identity),
            "source_snapshot": dict(self.source_snapshot) if self.source_snapshot is not None else None,
            "route_attestation_verification": self.route_attestation_verification,
            "init_fetch": self.init_fetch,
            "temporary_target_prepare": self.temporary_target_prepare,
            "data_copy": self.data_copy,
            "temporary_target_cleanup": self.temporary_target_cleanup,
            "errors": [dict(error) for error in self.errors],
        }


__all__ = ["SafeSampleRuntimeExecutionResult"]
