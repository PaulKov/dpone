"""Lazy Kubernetes client construction for Airflow task execution.

Connection-secret projection and launch-pin Authority C run inside
K8sExecutor / worker pods. Creating ``CoreV1Api()`` without loading
in-cluster (or kubeconfig / Airflow Connection) credentials leaves the
client pointed at ``localhost:80`` and fails closed as ``status=unavailable``.
"""

from __future__ import annotations

from typing import Any, Protocol


class KubernetesCoreV1Api(Protocol):
    def create_namespaced_secret(self, *, namespace: str, body: object) -> object: ...

    def delete_namespaced_secret(self, *, namespace: str, name: str) -> object: ...

    def read_namespaced_pod(self, *, name: str, namespace: str) -> object: ...

    def delete_namespaced_pod(self, *, name: str, namespace: str, body: object = None, **kwargs: Any) -> object: ...

    def create_namespaced_config_map(self, *, namespace: str, body: object) -> object: ...

    def read_namespaced_config_map(self, *, name: str, namespace: str) -> object: ...

    def replace_namespaced_config_map(self, *, name: str, namespace: str, body: object) -> object: ...

    def delete_namespaced_config_map(
        self, *, name: str, namespace: str, body: object = None, **kwargs: Any
    ) -> object: ...


def build_core_v1_api(*, kubernetes_conn_id: str | None = None) -> KubernetesCoreV1Api:
    """Return a CoreV1 API client after configuring cluster auth.

    When ``kubernetes_conn_id`` is set, the Airflow KubernetesHook is mandatory
    (fail-closed). Ambient in-cluster / kubeconfig fallback is refused so gate
    and cleanup cannot silently target a different cluster authority than KPO.
    """

    if kubernetes_conn_id:
        hook_client = _core_v1_from_airflow_hook(kubernetes_conn_id)
        if hook_client is not None:
            return hook_client
        raise RuntimeError(
            "DPONE_AIRFLOW_DEPLOYMENT_IDENTITY_PIN_UNAVAILABLE: "
            f"kubernetes_conn_id={kubernetes_conn_id!r} is unavailable; "
            "refusing ambient in-cluster/kubeconfig fallback"
        )

    try:
        from kubernetes.client import CoreV1Api
        from kubernetes.config import load_incluster_config, load_kube_config
        from kubernetes.config.config_exception import ConfigException
    except Exception as exc:  # pragma: no cover - exercised only with provider extras installed.
        raise RuntimeError(
            "Kubernetes client construction requires the kubernetes Python client at task execution time."
        ) from exc

    try:
        load_incluster_config()
    except ConfigException:
        load_kube_config()
    return CoreV1Api()


def _core_v1_from_airflow_hook(kubernetes_conn_id: str) -> KubernetesCoreV1Api | None:
    try:
        from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
    except Exception:
        return None
    try:
        hook = KubernetesHook(conn_id=kubernetes_conn_id)
        client = getattr(hook, "core_v1_client", None)
        if client is not None:
            return client
        get_conn = getattr(hook, "get_conn", None)
        if callable(get_conn):
            api_client = get_conn()
            from kubernetes.client import CoreV1Api

            return CoreV1Api(api_client)
    except Exception:
        return None
    return None


__all__ = ["KubernetesCoreV1Api", "build_core_v1_api"]
