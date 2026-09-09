"""Canonical trusted-attempt termination capability boundary.

Compatibility names re-export the sole public V2 contract; this module does
not issue a second schema or digest identity.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Protocol

from dpone.contracts.semantic_refresh_termination_receipt import (
    SemanticRefreshContainerTermination as ContainerTermination,
)
from dpone.contracts.semantic_refresh_termination_receipt import (
    SemanticRefreshTrustedAttemptTerminationReceipt as AttemptTerminationReceipt,
)


class SemanticRefreshTerminationReceiptPort(Protocol):
    """Create-only protected store for canonical trusted termination receipts."""

    def store(self, receipt: AttemptTerminationReceipt) -> None:
        """Persist exact receipt or reconcile a byte-identical existing record."""


@dataclass(frozen=True, slots=True)
class MssqlTerminationObservationAuthority:
    """Protected Kubernetes lookup coordinates for one canonical attempt."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    operation_ids: tuple[str, ...]
    operation_set_sha256: str
    attempt_binding_sha256: str
    dag_id: str
    run_id: str
    task_id: str
    map_index: int
    try_number: int
    cluster_id: str
    namespace: str
    pod_name: str
    pod_uid: str
    observer_authority: str
    observer_policy_sha256: str
    observer_attestation_sha256: str
    observation_authority_sha256: str
    status: str


class SemanticRefreshTerminationObservationAuthorityPort(Protocol):
    """Load protected scheduler/cluster observation coordinates."""

    def load_observation_authority(
        self,
        *,
        workflow_execution_binding_sha256: str,
        attempt_binding_sha256: str,
    ) -> MssqlTerminationObservationAuthority:
        """Return one ACTIVE authority or fail closed."""


def mssql_termination_observation_authority_sha256(
    authority: MssqlTerminationObservationAuthority,
) -> str:
    """Digest the closed protected scheduler and observer coordinates."""

    payload = {
        "attempt_binding_sha256": authority.attempt_binding_sha256,
        "cluster_id": authority.cluster_id,
        "dag_id": authority.dag_id,
        "map_index": authority.map_index,
        "namespace": authority.namespace,
        "observer_attestation_sha256": authority.observer_attestation_sha256,
        "observer_authority": authority.observer_authority,
        "observer_policy_sha256": authority.observer_policy_sha256,
        "operation_ids": list(authority.operation_ids),
        "operation_set_sha256": authority.operation_set_sha256,
        "pod_name": authority.pod_name,
        "pod_uid": authority.pod_uid,
        "run_id": authority.run_id,
        "schema": "dpone.semantic-refresh-mssql-termination-observation-authority.v1",
        "status": authority.status,
        "task_id": authority.task_id,
        "try_number": authority.try_number,
        "workflow_execution_binding_sha256": authority.workflow_execution_binding_sha256,
        "workflow_execution_id": authority.workflow_execution_id,
    }
    raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


__all__ = [
    "AttemptTerminationReceipt",
    "ContainerTermination",
    "MssqlTerminationObservationAuthority",
    "SemanticRefreshTerminationObservationAuthorityPort",
    "SemanticRefreshTerminationReceiptPort",
    "mssql_termination_observation_authority_sha256",
]
