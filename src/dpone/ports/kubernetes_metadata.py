"""Connector-neutral metadata-only Kubernetes inventory contracts."""

from __future__ import annotations

from typing import Protocol

from dpone.contracts.kubernetes_metadata import KubernetesMetadataSnapshot


class KubernetesMetadataApiError(RuntimeError):
    """Redacted metadata API failure independent of one cleanup policy."""

    def __init__(self, *, operation: str, status: int | None, reason: str) -> None:
        self.operation = operation
        self.status = status
        self.reason = reason
        suffix = f" (status {status})" if status is not None else ""
        super().__init__(f"Kubernetes metadata {operation} failed: {reason}{suffix}")


class KubernetesMetadataTransport(Protocol):
    def list_metadata_page(
        self,
        *,
        resource: str,
        namespace: str,
        label_selector: str,
        limit: int,
        continue_token: str | None,
        accept: str,
        field_selector: str | None = None,
    ) -> dict[str, object]: ...


class KubernetesMetadataDeletionTransport(KubernetesMetadataTransport, Protocol):
    """Composite capability required by metadata-bound conditional deletion."""

    def delete_pod(self, *, namespace: str, name: str, uid: str, resource_version: str) -> None: ...


class KubernetesSecretMetadataDeletionTransport(KubernetesMetadataTransport, Protocol):
    """Composite capability required by metadata-bound Secret deletion."""

    def delete_secret(self, *, namespace: str, name: str, uid: str, resource_version: str) -> None: ...


__all__ = [
    "KubernetesMetadataApiError",
    "KubernetesMetadataDeletionTransport",
    "KubernetesSecretMetadataDeletionTransport",
    "KubernetesMetadataSnapshot",
    "KubernetesMetadataTransport",
]
