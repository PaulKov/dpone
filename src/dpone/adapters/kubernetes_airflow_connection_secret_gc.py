"""Kubernetes adapter for Airflow Connection Secret GC."""

from __future__ import annotations

from dpone_airflow_pack.connection_secret_lifecycle import POD_LABEL_SELECTOR, SECRET_LABEL_SELECTOR

from dpone.adapters.kubernetes_metadata import (
    PARTIAL_METADATA_ACCEPT,
    KubernetesApiMetadataTransport,
    KubernetesPartialMetadataClient,
    build_kubernetes_metadata_transport,
)
from dpone.ports.airflow_connection_secret_gc import (
    AirflowConnectionSecretGcApiError,
    KubernetesMetadataSnapshot,
)
from dpone.ports.kubernetes_metadata import (
    KubernetesMetadataApiError,
    KubernetesSecretMetadataDeletionTransport,
)


class KubernetesAirflowConnectionSecretGcAdapter:
    """Require metadata-only inventory and conditional Secret deletes."""

    def __init__(self, *, transport: KubernetesSecretMetadataDeletionTransport) -> None:
        self._transport = transport
        self._metadata = KubernetesPartialMetadataClient(transport=transport)

    def list_secret_metadata(self, *, namespace: str, page_size: int) -> tuple[KubernetesMetadataSnapshot, ...]:
        return self._list(
            resource="secrets",
            namespace=namespace,
            page_size=page_size,
            label_selector=SECRET_LABEL_SELECTOR,
        )

    def list_pod_metadata(self, *, namespace: str, page_size: int) -> tuple[KubernetesMetadataSnapshot, ...]:
        return self._list(
            resource="pods",
            namespace=namespace,
            page_size=page_size,
            label_selector=POD_LABEL_SELECTOR,
        )

    def delete_secret(self, *, namespace: str, name: str, uid: str, resource_version: str) -> None:
        try:
            self._transport.delete_secret(
                namespace=namespace,
                name=name,
                uid=uid,
                resource_version=resource_version,
            )
        except AirflowConnectionSecretGcApiError:
            raise
        except KubernetesMetadataApiError as exc:
            raise _translate(exc) from None
        except Exception as exc:  # noqa: BLE001 - vendor details stay behind the boundary.
            raise _translate_exception("delete", exc) from None

    def _list(
        self,
        *,
        resource: str,
        namespace: str,
        page_size: int,
        label_selector: str,
    ) -> tuple[KubernetesMetadataSnapshot, ...]:
        try:
            return self._metadata.list_metadata(
                resource=resource,
                namespace=namespace,
                label_selector=label_selector,
                field_selector=None,
                page_size=page_size,
            )
        except KubernetesMetadataApiError as exc:
            raise _translate(exc) from None


def build_kubernetes_airflow_connection_secret_gc_adapter(
    *,
    auth_mode: str = "auto",
    kube_context: str | None = None,
) -> KubernetesAirflowConnectionSecretGcAdapter:
    try:
        transport = build_kubernetes_metadata_transport(auth_mode=auth_mode, kube_context=kube_context)
        return KubernetesAirflowConnectionSecretGcAdapter(transport=transport)
    except KubernetesMetadataApiError as exc:
        raise _translate(exc) from None


def _translate(exc: KubernetesMetadataApiError) -> AirflowConnectionSecretGcApiError:
    return AirflowConnectionSecretGcApiError(operation=exc.operation, status=exc.status, reason=exc.reason)


def _translate_exception(operation: str, exc: BaseException) -> AirflowConnectionSecretGcApiError:
    status = getattr(exc, "status", None)
    safe_status = status if isinstance(status, int) and not isinstance(status, bool) and 100 <= status <= 599 else None
    if safe_status in {401, 403}:
        reason = "access_denied"
    elif safe_status == 406:
        reason = "metadata_only_unsupported"
    elif safe_status == 410:
        reason = "inventory_expired"
    else:
        reason = "api_failure"
    return AirflowConnectionSecretGcApiError(operation=operation, status=safe_status, reason=reason)


# Preserve the tested internal construction seam until a documented deprecation.
_KubernetesApiTransport = KubernetesApiMetadataTransport

__all__ = [
    "PARTIAL_METADATA_ACCEPT",
    "KubernetesAirflowConnectionSecretGcAdapter",
    "build_kubernetes_airflow_connection_secret_gc_adapter",
]
