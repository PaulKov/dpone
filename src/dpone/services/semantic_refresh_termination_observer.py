"""Trusted Kubernetes termination observer for semantic-refresh takeover."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from dpone.contracts import semantic_refresh_termination_receipt as termination_contract

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_kubernetes import (
        ObservedTerminalPod,
        SemanticRefreshKubernetesObservationPort,
        SemanticRefreshObserverSignaturePort,
    )
    from dpone.ports.semantic_refresh_termination import (
        MssqlTerminationObservationAuthority,
        SemanticRefreshTerminationReceiptPort,
    )

_UTC = timezone.utc  # noqa: UP017 - package supports Python 3.10


@dataclass(frozen=True, slots=True)
class SemanticRefreshTerminationObserverService:
    """Observe, sign, and durably publish one exact terminal attempt."""

    observation: SemanticRefreshKubernetesObservationPort
    signer: SemanticRefreshObserverSignaturePort
    receipt_store: SemanticRefreshTerminationReceiptPort
    clock: Callable[[], datetime]

    def observe_and_store(
        self,
        authority: MssqlTerminationObservationAuthority,
    ) -> termination_contract.SemanticRefreshTrustedAttemptTerminationReceipt:
        """Create a receipt only from an ACTIVE protected observation authority."""

        if authority.status != "ACTIVE":
            raise ValueError("termination observation authority is not ACTIVE")
        pod = self.observation.observe_terminal_pod(authority)
        _assert_pod_authority(pod, authority)
        observed_at = _canonical_utc(self.clock(), "observer clock")
        containers = tuple(
            termination_contract.SemanticRefreshContainerTermination(
                name=item.name,
                container_id=item.container_id,
                reason=item.reason,
                finished_at=item.finished_at,
                exit_code=item.exit_code,
            )
            for item in pod.container_terminations
        )
        signature_subject = termination_contract.semantic_refresh_termination_observation_signature_subject(
            attempt_binding_sha256=authority.attempt_binding_sha256,
            cluster_id=pod.cluster_id,
            container_terminations=containers,
            observed_at=observed_at,
            observer_attestation_sha256=authority.observer_attestation_sha256,
            observer_policy_sha256=authority.observer_policy_sha256,
            namespace=pod.namespace,
            operation_ids=authority.operation_ids,
            operation_set_sha256=authority.operation_set_sha256,
            pod_name=pod.pod_name,
            pod_resource_version=pod.pod_resource_version,
            pod_uid=pod.pod_uid,
            terminal_phase=pod.terminal_phase,
            workflow_execution_binding_sha256=authority.workflow_execution_binding_sha256,
        )
        signature = self.signer.sign(
            subject_sha256=signature_subject,
            authority_id=authority.observer_authority,
        )
        receipt = termination_contract.SemanticRefreshTrustedAttemptTerminationReceipt.build(
            workflow_execution_id=authority.workflow_execution_id,
            workflow_execution_binding_sha256=authority.workflow_execution_binding_sha256,
            operation_ids=authority.operation_ids,
            attempt_binding_sha256=authority.attempt_binding_sha256,
            dag_id=authority.dag_id,
            run_id=authority.run_id,
            task_id=authority.task_id,
            map_index=authority.map_index,
            try_number=authority.try_number,
            cluster_id=pod.cluster_id,
            namespace=pod.namespace,
            pod_name=pod.pod_name,
            pod_uid=pod.pod_uid,
            pod_resource_version=pod.pod_resource_version,
            terminal_phase=pod.terminal_phase,
            container_terminations=containers,
            observed_at=observed_at,
            observer_authority=authority.observer_authority,
            observer_policy_sha256=authority.observer_policy_sha256,
            observer_attestation_sha256=authority.observer_attestation_sha256,
            observer_signature_sha256=signature,
        )
        self.receipt_store.store(receipt)
        return receipt

    def verify(
        self,
        *,
        authority: MssqlTerminationObservationAuthority,
        receipt: termination_contract.SemanticRefreshTrustedAttemptTerminationReceipt,
    ) -> termination_contract.SemanticRefreshTrustedAttemptTerminationReceipt:
        """Authenticate a stored receipt before it can authorize takeover."""

        _assert_receipt_authority(receipt, authority)
        subject = termination_contract.semantic_refresh_termination_observation_signature_subject(
            attempt_binding_sha256=receipt.attempt_binding_sha256,
            cluster_id=receipt.cluster_id,
            container_terminations=receipt.container_terminations,
            observed_at=receipt.observed_at,
            observer_attestation_sha256=receipt.observer_attestation_sha256,
            observer_policy_sha256=receipt.observer_policy_sha256,
            namespace=receipt.namespace,
            operation_ids=receipt.operation_ids,
            operation_set_sha256=receipt.operation_set_sha256,
            pod_name=receipt.pod_name,
            pod_resource_version=receipt.pod_resource_version,
            pod_uid=receipt.pod_uid,
            terminal_phase=receipt.terminal_phase,
            workflow_execution_binding_sha256=receipt.workflow_execution_binding_sha256,
        )
        if not self.signer.verify(
            subject_sha256=subject,
            authority_id=authority.observer_authority,
            signature_sha256=receipt.observer_signature_sha256,
        ):
            raise ValueError("termination observer signature is not trusted")
        return receipt


def _assert_pod_authority(
    pod: ObservedTerminalPod,
    authority: MssqlTerminationObservationAuthority,
) -> None:
    if (
        pod.cluster_id != authority.cluster_id
        or pod.namespace != authority.namespace
        or pod.pod_name != authority.pod_name
        or pod.pod_uid != authority.pod_uid
    ):
        raise ValueError("observed pod identity differs from protected authority")
    if pod.terminal_phase not in {"Succeeded", "Failed"}:
        raise ValueError("observed pod is not terminal")
    if not pod.container_terminations:
        raise ValueError("terminal pod has no complete container termination closure")
    names = tuple(item.name for item in pod.container_terminations)
    if names != tuple(sorted(names)) or len(names) != len(set(names)):
        raise ValueError("container termination closure must be sorted and unique")


def _assert_receipt_authority(
    receipt: termination_contract.SemanticRefreshTrustedAttemptTerminationReceipt,
    authority: MssqlTerminationObservationAuthority,
) -> None:
    if (
        receipt.workflow_execution_id != authority.workflow_execution_id
        or receipt.workflow_execution_binding_sha256 != authority.workflow_execution_binding_sha256
        or receipt.operation_ids != authority.operation_ids
        or receipt.operation_set_sha256 != authority.operation_set_sha256
        or receipt.attempt_binding_sha256 != authority.attempt_binding_sha256
        or receipt.dag_id != authority.dag_id
        or receipt.run_id != authority.run_id
        or receipt.task_id != authority.task_id
        or receipt.map_index != authority.map_index
        or receipt.try_number != authority.try_number
        or receipt.cluster_id != authority.cluster_id
        or receipt.namespace != authority.namespace
        or receipt.pod_name != authority.pod_name
        or receipt.pod_uid != authority.pod_uid
        or receipt.observer_authority != authority.observer_authority
        or receipt.observer_policy_sha256 != authority.observer_policy_sha256
        or receipt.observer_attestation_sha256 != authority.observer_attestation_sha256
    ):
        raise ValueError("termination receipt differs from protected observation authority")


def _canonical_utc(value: datetime, label: str) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(_UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


__all__ = [
    "SemanticRefreshTerminationObserverService",
]
