"""Canonical signature subject for trusted attempt termination observations."""

from __future__ import annotations

from dpone.contracts.semantic_refresh_container_termination import SemanticRefreshContainerTermination
from dpone.contracts.semantic_refresh_document import semantic_refresh_sha256


def semantic_refresh_termination_observation_signature_subject(
    *,
    attempt_binding_sha256: str,
    cluster_id: str,
    container_terminations: tuple[SemanticRefreshContainerTermination, ...],
    observed_at: str,
    observer_attestation_sha256: str,
    observer_policy_sha256: str,
    namespace: str,
    operation_ids: tuple[str, ...],
    operation_set_sha256: str,
    pod_name: str,
    pod_resource_version: str,
    pod_uid: str,
    terminal_phase: str,
    workflow_execution_binding_sha256: str,
) -> str:
    """Return the canonical protected-observer signature subject."""

    return semantic_refresh_sha256(
        {
            "attempt_binding_sha256": attempt_binding_sha256,
            "cluster_id": cluster_id,
            "container_terminations": [item.to_dict() for item in container_terminations],
            "observed_at": observed_at,
            "observer_attestation_sha256": observer_attestation_sha256,
            "observer_policy_sha256": observer_policy_sha256,
            "namespace": namespace,
            "operation_ids": list(operation_ids),
            "operation_set_sha256": operation_set_sha256,
            "pod_name": pod_name,
            "pod_resource_version": pod_resource_version,
            "pod_uid": pod_uid,
            "schema": "dpone.semantic-refresh-termination-observation-signature-subject.v1",
            "terminal_phase": terminal_phase,
            "workflow_execution_binding_sha256": workflow_execution_binding_sha256,
        }
    )
