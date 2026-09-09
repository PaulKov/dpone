"""Validated requests and stable errors for Airflow Connection Secret GC."""

from __future__ import annotations

from dataclasses import dataclass

from dpone_airflow_pack.connection_names import require_kubernetes_dns_label

MINIMUM_AGE_MIN = 300
MINIMUM_AGE_MAX = 2_592_000
PAGE_SIZE_MAX = 1_000
DELETE_COUNT_MAX = 1_000


class AirflowConnectionSecretGcError(RuntimeError):
    """Stable, topology-free application error."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class AirflowConnectionSecretGcPlanRequest:
    namespace: str
    minimum_age_seconds: int = 86_400
    page_size: int = 500

    def __post_init__(self) -> None:
        _validate_common_request(self.namespace, self.minimum_age_seconds, self.page_size)


@dataclass(frozen=True, slots=True)
class AirflowConnectionSecretGcApplyRequest:
    namespace: str
    minimum_age_seconds: int = 86_400
    page_size: int = 500
    max_delete_count: int = 100
    actor: str = ""
    allowed_actors: tuple[str, ...] = ()
    confirm_delete: bool = False

    def __post_init__(self) -> None:
        _validate_common_request(self.namespace, self.minimum_age_seconds, self.page_size)
        if isinstance(self.max_delete_count, bool) or not 1 <= self.max_delete_count <= DELETE_COUNT_MAX:
            raise invalid_input("max_delete_count must be between 1 and 1000")

    def require_authorized(self) -> None:
        """Fail before infrastructure construction or inventory I/O."""

        if not self.confirm_delete:
            raise AirflowConnectionSecretGcError(
                "DPONE_AIRFLOW_SECRET_GC_CONFIRMATION_REQUIRED",
                "Airflow Connection Secret GC apply requires --confirm-delete.",
            )
        if not self.actor.strip():
            raise AirflowConnectionSecretGcError(
                "DPONE_AIRFLOW_SECRET_GC_ACTOR_MISSING",
                "Airflow Connection Secret GC apply requires a non-empty actor.",
            )
        allowed = tuple(item.strip() for item in self.allowed_actors if item.strip())
        if not allowed or self.actor.strip() not in allowed:
            raise AirflowConnectionSecretGcError(
                "DPONE_AIRFLOW_SECRET_GC_ACTOR_UNAUTHORIZED",
                "Airflow Connection Secret GC actor is not in the exact allowlist.",
            )


def invalid_input(message: str) -> AirflowConnectionSecretGcError:
    return AirflowConnectionSecretGcError("DPONE_AIRFLOW_SECRET_GC_INPUT_INVALID", message)


def _validate_common_request(namespace: str, minimum_age_seconds: int, page_size: int) -> None:
    try:
        require_kubernetes_dns_label(namespace, context="namespace")
    except ValueError:
        raise invalid_input("namespace must be a Kubernetes DNS label")
    if isinstance(minimum_age_seconds, bool) or not MINIMUM_AGE_MIN <= minimum_age_seconds <= MINIMUM_AGE_MAX:
        raise invalid_input("minimum_age_seconds must be between 300 and 2592000")
    if isinstance(page_size, bool) or not 1 <= page_size <= PAGE_SIZE_MAX:
        raise invalid_input("page_size must be between 1 and 1000")


__all__ = [
    "AirflowConnectionSecretGcApplyRequest",
    "AirflowConnectionSecretGcError",
    "AirflowConnectionSecretGcPlanRequest",
]
