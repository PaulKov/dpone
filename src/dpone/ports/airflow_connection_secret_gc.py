"""Ports for metadata-only Airflow Connection Secret garbage collection."""

from __future__ import annotations

from typing import Protocol

from dpone.ports.kubernetes_metadata import KubernetesMetadataSnapshot


class AirflowConnectionSecretGcApiError(RuntimeError):
    """Redacted infrastructure failure exposed by the Kubernetes adapter."""

    def __init__(self, *, operation: str, status: int | None, reason: str) -> None:
        self.operation = operation
        self.status = status
        self.reason = reason
        suffix = f" (status {status})" if status is not None else ""
        super().__init__(f"dpone credential GC {operation} failed: {reason}{suffix}")


class AirflowConnectionSecretGcInventory(Protocol):
    """Inventory only managed Secret/Pod metadata without object payloads."""

    def list_secret_metadata(
        self,
        *,
        namespace: str,
        page_size: int,
    ) -> tuple[KubernetesMetadataSnapshot, ...]: ...

    def list_pod_metadata(
        self,
        *,
        namespace: str,
        page_size: int,
    ) -> tuple[KubernetesMetadataSnapshot, ...]: ...


class AirflowConnectionSecretGcDeletion(Protocol):
    """Delete one exact Secret generation with optimistic preconditions."""

    def delete_secret(
        self,
        *,
        namespace: str,
        name: str,
        uid: str,
        resource_version: str,
    ) -> None: ...


__all__ = [
    "AirflowConnectionSecretGcApiError",
    "AirflowConnectionSecretGcDeletion",
    "AirflowConnectionSecretGcInventory",
    "KubernetesMetadataSnapshot",
]
