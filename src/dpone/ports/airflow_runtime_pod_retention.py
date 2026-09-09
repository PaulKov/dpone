"""Ports for metadata-only Airflow runtime Pod retention."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal, Protocol

from dpone.contracts.airflow_runtime_pod_metadata import TerminalPodMetadataSnapshot

if TYPE_CHECKING:
    from dpone.contracts.airflow_runtime_pod_retention_ack import (
        AirflowRuntimePodRetentionEvidenceAcknowledgement,
    )


class AirflowRuntimePodRetentionApiError(RuntimeError):
    def __init__(self, *, operation: str, status: int | None, reason: str) -> None:
        self.operation = operation
        self.status = status
        self.reason = reason
        suffix = f" (status {status})" if status is not None else ""
        super().__init__(f"dpone runtime Pod retention {operation} failed: {reason}{suffix}")


class AirflowRuntimePodRetentionEvidenceError(RuntimeError):
    """Safe failure raised when mutation evidence cannot be published."""


class AirflowRuntimePodRetentionInventory(Protocol):
    def list_terminal_pod_metadata(
        self,
        *,
        namespace: str,
        page_size: int,
    ) -> tuple[TerminalPodMetadataSnapshot, ...]: ...


class AirflowRuntimePodRetentionDeletion(Protocol):
    def delete_pod(
        self,
        *,
        namespace: str,
        name: str,
        uid: str,
        resource_version: str,
    ) -> None: ...


class AirflowRuntimePodRetentionCredentialSource(Protocol):
    def resolved_credential_mode(self) -> str: ...

    def resolved_credential_context(self) -> str | None: ...


class AirflowRuntimePodRetentionEvidencePublisher(Protocol):
    @property
    def durability(self) -> Literal["process_ordered", "durable_acknowledged"]: ...

    def publish(self, event: dict[str, object]) -> None: ...


class AirflowRuntimePodRetentionDurableSink(Protocol):
    def append_and_ack(
        self,
        event: Mapping[str, object],
    ) -> AirflowRuntimePodRetentionEvidenceAcknowledgement: ...


__all__ = [
    "AirflowRuntimePodRetentionApiError",
    "AirflowRuntimePodRetentionCredentialSource",
    "AirflowRuntimePodRetentionDeletion",
    "AirflowRuntimePodRetentionDurableSink",
    "AirflowRuntimePodRetentionEvidenceError",
    "AirflowRuntimePodRetentionEvidencePublisher",
    "AirflowRuntimePodRetentionInventory",
    "TerminalPodMetadataSnapshot",
]
