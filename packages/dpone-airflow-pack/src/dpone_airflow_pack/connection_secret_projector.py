"""Kubernetes Secret projection for Airflow Connection URIs at task execution."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from dpone_airflow_pack.connection_secret_identity import secret_name_reference
from dpone_airflow_pack.operator_runtime import _secret_manifest_name


class AirflowConnectionSecretProjector(Protocol):
    """Execution-time port for create-only temporary Secret publication.

    ``upsert`` is retained as a compatibility method name. Implementations must
    fail on an existing object and must never replace another task attempt's
    Secret.
    """

    def upsert(self, *, namespace: str, secret: Mapping[str, Any]) -> None: ...

    def delete(self, *, namespace: str, name: str) -> None: ...


class KubernetesCoreV1Api(Protocol):
    def create_namespaced_secret(self, *, namespace: str, body: Mapping[str, object]) -> object: ...

    def delete_namespaced_secret(self, *, namespace: str, name: str) -> object: ...


def _kubernetes_status(exc: BaseException) -> int | None:
    status = getattr(exc, "status", None)
    return int(status) if isinstance(status, int) else None


class AirflowConnectionSecretProjectionError(RuntimeError):
    """Redacted execution-time Kubernetes Secret projection failure."""

    def __init__(
        self,
        *,
        code: str,
        operation: str,
        status: int | None,
        secret_ref: str,
    ) -> None:
        self.code = code
        self.operation = operation
        self.status = status
        self.secret_ref = secret_ref
        status_text = str(status) if status is not None else "unavailable"
        super().__init__(
            f"{code}: Airflow Connection Secret {operation} failed (status={status_text}, ref={secret_ref})"
        )


class AirflowConnectionSecretConflictError(AirflowConnectionSecretProjectionError):
    """Fail closed when an attempt-scoped Secret name already exists."""

    def __init__(self, *, secret_ref: str) -> None:
        super().__init__(
            code="DPONE_AIRFLOW_CONNECTION_SECRET_CONFLICT",
            operation="create",
            status=409,
            secret_ref=secret_ref,
        )


class AirflowConnectionSecretDependencyError(AirflowConnectionSecretProjectionError):
    """Report an unavailable Kubernetes execution dependency without its details."""

    def __init__(self, *, secret_ref: str) -> None:
        super().__init__(
            code="DPONE_AIRFLOW_CONNECTION_SECRET_DEPENDENCY_UNAVAILABLE",
            operation="client_init",
            status=None,
            secret_ref=secret_ref,
        )


class KubernetesApiAirflowConnectionSecretProjector:
    """Publish projected Airflow Connection URIs through the Kubernetes API.

    Imports stay inside execution methods so importing DAG files remains free of
    Kubernetes, Airflow metadata DB, Variables, Connections, Vault, and network
    side effects.
    """

    def upsert(self, *, namespace: str, secret: Mapping[str, Any]) -> None:
        name = _secret_manifest_name(secret)
        secret_ref = secret_name_reference(name)
        api = self._api(secret_ref=secret_ref)
        try:
            api.create_namespaced_secret(namespace=namespace, body=secret)
        except Exception as exc:
            status = _kubernetes_status(exc)
            if status == 409:
                raise AirflowConnectionSecretConflictError(secret_ref=secret_ref) from None
            raise AirflowConnectionSecretProjectionError(
                code="DPONE_AIRFLOW_CONNECTION_SECRET_PUBLISH_FAILED",
                operation="create",
                status=status,
                secret_ref=secret_ref,
            ) from None

    def delete(self, *, namespace: str, name: str) -> None:
        secret_ref = secret_name_reference(name)
        api = self._api(secret_ref=secret_ref)
        try:
            api.delete_namespaced_secret(namespace=namespace, name=name)
        except Exception as exc:
            status = _kubernetes_status(exc)
            if status == 404:
                return
            raise AirflowConnectionSecretProjectionError(
                code="DPONE_AIRFLOW_CONNECTION_SECRET_CLEANUP_FAILED",
                operation="delete",
                status=status,
                secret_ref=secret_ref,
            ) from None

    def _api(self, *, secret_ref: str) -> KubernetesCoreV1Api:
        try:
            return self._core_v1_api()
        except Exception:
            raise AirflowConnectionSecretDependencyError(secret_ref=secret_ref) from None

    def _core_v1_api(self) -> KubernetesCoreV1Api:
        from dpone_airflow_pack.kubernetes_runtime_client import build_core_v1_api

        return build_core_v1_api()


__all__ = [
    "AirflowConnectionSecretConflictError",
    "AirflowConnectionSecretDependencyError",
    "AirflowConnectionSecretProjectionError",
    "AirflowConnectionSecretProjector",
    "KubernetesApiAirflowConnectionSecretProjector",
    "KubernetesCoreV1Api",
]
