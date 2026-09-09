"""Validated public requests for Airflow runtime Pod retention."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.kubernetes_names import require_kubernetes_dns_label

MINIMUM_AGE_MIN = 300
MINIMUM_AGE_MAX = 2_592_000
PAGE_SIZE_MAX = 1_000
DELETE_COUNT_MAX = 1_000
KUBE_AUTH_MODES = frozenset({"auto", "in-cluster", "kubeconfig"})


class AirflowRuntimePodRetentionError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class AirflowRuntimePodRetentionPlanRequest:
    namespace: str
    minimum_age_seconds: int = 86_400
    page_size: int = 500

    def __post_init__(self) -> None:
        _validate_common(self.namespace, self.minimum_age_seconds, self.page_size)


@dataclass(frozen=True, slots=True)
class AirflowRuntimePodRetentionApplyRequest:
    namespace: str
    minimum_age_seconds: int = 86_400
    page_size: int = 500
    max_delete_count: int = 100
    actor: str = ""
    allowed_actors: tuple[str, ...] = ()
    confirm_delete: bool = False
    kube_auth_mode: str = "auto"

    def __post_init__(self) -> None:
        _validate_common(self.namespace, self.minimum_age_seconds, self.page_size)
        validate_max_delete_count(self.max_delete_count)
        if self.kube_auth_mode not in KUBE_AUTH_MODES:
            raise invalid_input("kube_auth_mode must be auto, in-cluster, or kubeconfig")
        if self.kube_auth_mode == "auto":
            raise invalid_input("runtime Pod retention apply requires explicit in-cluster or kubeconfig auth")

    def require_authorized(self) -> None:
        if not self.confirm_delete:
            raise AirflowRuntimePodRetentionError(
                "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_CONFIRMATION_REQUIRED",
                "Airflow runtime Pod retention apply requires --confirm-delete.",
            )
        actor = self.actor.strip()
        if not actor:
            raise AirflowRuntimePodRetentionError(
                "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_ACTOR_MISSING",
                "Airflow runtime Pod retention apply requires a non-empty actor.",
            )
        allowed = tuple(item.strip() for item in self.allowed_actors if item.strip())
        if not allowed or actor not in allowed:
            raise AirflowRuntimePodRetentionError(
                "DPONE_AIRFLOW_RUNTIME_POD_RETENTION_ACTOR_UNAUTHORIZED",
                "Airflow runtime Pod retention actor is not in the exact allowlist.",
            )


def invalid_input(message: str) -> AirflowRuntimePodRetentionError:
    return AirflowRuntimePodRetentionError("DPONE_AIRFLOW_RUNTIME_POD_RETENTION_INPUT_INVALID", message)


def validate_max_delete_count(value: int) -> None:
    if isinstance(value, bool) or not 1 <= value <= DELETE_COUNT_MAX:
        raise invalid_input("max_delete_count must be between 1 and 1000")


def validate_apply_kubernetes_authority(auth_mode: str, kube_context: str | None) -> None:
    if auth_mode == "auto":
        raise invalid_input("runtime Pod retention apply requires explicit in-cluster or kubeconfig auth")
    if auth_mode == "in-cluster":
        if kube_context is not None:
            raise invalid_input("kube_context cannot be used with in-cluster authentication")
        return
    if auth_mode != "kubeconfig":
        raise invalid_input("kube_auth_mode must be auto, in-cluster, or kubeconfig")
    if (
        kube_context is None
        or not kube_context
        or kube_context != kube_context.strip()
        or len(kube_context) > 253
        or any(char in kube_context for char in "\r\n\0")
    ):
        raise invalid_input("kubeconfig apply requires one explicit bounded kube_context")


def _validate_common(namespace: str, minimum_age_seconds: int, page_size: int) -> None:
    try:
        require_kubernetes_dns_label(namespace, context="namespace")
    except ValueError:
        raise invalid_input("namespace must be a Kubernetes DNS label") from None
    if isinstance(minimum_age_seconds, bool) or not MINIMUM_AGE_MIN <= minimum_age_seconds <= MINIMUM_AGE_MAX:
        raise invalid_input("minimum_age_seconds must be between 300 and 2592000")
    if isinstance(page_size, bool) or not 1 <= page_size <= PAGE_SIZE_MAX:
        raise invalid_input("page_size must be between 1 and 1000")


__all__ = [
    "AirflowRuntimePodRetentionApplyRequest",
    "AirflowRuntimePodRetentionError",
    "AirflowRuntimePodRetentionPlanRequest",
    "validate_apply_kubernetes_authority",
    "validate_max_delete_count",
]
