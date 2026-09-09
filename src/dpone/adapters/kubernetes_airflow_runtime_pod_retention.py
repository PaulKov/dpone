"""Kubernetes metadata adapter for Airflow runtime Pod retention."""

from __future__ import annotations

from dpone.adapters.kubernetes_metadata import (
    KubernetesMetadataReadBudget,
    KubernetesPartialMetadataClient,
    build_kubernetes_metadata_transport,
)
from dpone.ports.airflow_runtime_pod_retention import (
    AirflowRuntimePodRetentionApiError,
    TerminalPodMetadataSnapshot,
)
from dpone.ports.kubernetes_metadata import KubernetesMetadataApiError, KubernetesMetadataDeletionTransport

_TERMINAL_PHASES = ("Failed", "Succeeded")


class KubernetesAirflowRuntimePodRetentionAdapter:
    def __init__(
        self,
        *,
        transport: KubernetesMetadataDeletionTransport,
        credential_mode: str,
        credential_context: str | None,
        label_selector: str,
    ) -> None:
        self._transport = transport
        self._metadata = KubernetesPartialMetadataClient(transport=transport)
        if credential_mode not in {"in-cluster", "kubeconfig"}:
            raise ValueError("resolved Kubernetes credential mode is invalid")
        self._credential_mode = credential_mode
        self._credential_context = credential_context
        if not label_selector.strip() or label_selector != label_selector.strip():
            raise ValueError("runtime Pod label selector is invalid")
        self._label_selector = label_selector

    def resolved_credential_mode(self) -> str:
        return self._credential_mode

    def resolved_credential_context(self) -> str | None:
        return self._credential_context

    def list_terminal_pod_metadata(
        self,
        *,
        namespace: str,
        page_size: int,
    ) -> tuple[TerminalPodMetadataSnapshot, ...]:
        snapshots: list[TerminalPodMetadataSnapshot] = []
        budget = KubernetesMetadataReadBudget()
        try:
            for phase in _TERMINAL_PHASES:
                listed = self._metadata.list_metadata(
                    resource="pods",
                    namespace=namespace,
                    label_selector=self._label_selector,
                    field_selector=f"status.phase={phase}",
                    page_size=page_size,
                    restart_on_expired=True,
                    budget=budget,
                )
                snapshots.extend(TerminalPodMetadataSnapshot(metadata=item, phase=phase) for item in listed)
        except KubernetesMetadataApiError as exc:
            raise _translate(exc) from None
        return tuple(snapshots)

    def delete_pod(self, *, namespace: str, name: str, uid: str, resource_version: str) -> None:
        try:
            self._transport.delete_pod(
                namespace=namespace,
                name=name,
                uid=uid,
                resource_version=resource_version,
            )
        except AirflowRuntimePodRetentionApiError:
            raise
        except KubernetesMetadataApiError as exc:
            raise _translate(exc) from None
        except Exception as exc:  # noqa: BLE001 - vendor errors are redacted below.
            status = getattr(exc, "status", None)
            safe_status = (
                status if isinstance(status, int) and not isinstance(status, bool) and 100 <= status <= 599 else None
            )
            reason = "access_denied" if safe_status in {401, 403} else "api_failure"
            raise AirflowRuntimePodRetentionApiError(operation="delete", status=safe_status, reason=reason) from None


def build_kubernetes_airflow_runtime_pod_retention_adapter(
    *,
    auth_mode: str = "auto",
    kube_context: str | None = None,
    label_selector: str,
) -> KubernetesAirflowRuntimePodRetentionAdapter:
    try:
        transport = build_kubernetes_metadata_transport(auth_mode=auth_mode, kube_context=kube_context)
        return KubernetesAirflowRuntimePodRetentionAdapter(
            transport=transport,
            credential_mode=transport.credential_mode,
            credential_context=kube_context,
            label_selector=label_selector,
        )
    except KubernetesMetadataApiError as exc:
        raise _translate(exc) from None


def _translate(exc: KubernetesMetadataApiError) -> AirflowRuntimePodRetentionApiError:
    return AirflowRuntimePodRetentionApiError(operation=exc.operation, status=exc.status, reason=exc.reason)


__all__ = [
    "KubernetesAirflowRuntimePodRetentionAdapter",
    "build_kubernetes_airflow_runtime_pod_retention_adapter",
]
