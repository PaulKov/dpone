from __future__ import annotations

import re

AIRFLOW_CONNECTION_ID_PATTERN = r"^[A-Za-z0-9_.-]+$"
AIRFLOW_CONN_ENV_NAME_PATTERN = r"^AIRFLOW_CONN_[A-Z0-9_]+$"
KUBERNETES_DNS_LABEL_MAX_LENGTH = 63
KUBERNETES_DNS_LABEL_PATTERN = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"

_CONNECTION_ID_RE = re.compile(AIRFLOW_CONNECTION_ID_PATTERN)
_ENV_NAME_RE = re.compile(AIRFLOW_CONN_ENV_NAME_PATTERN)
_KUBERNETES_DNS_LABEL_RE = re.compile(KUBERNETES_DNS_LABEL_PATTERN)


def is_valid_airflow_connection_id(value: object) -> bool:
    """Return True when value is a safe logical Airflow Connection id."""

    return isinstance(value, str) and bool(_CONNECTION_ID_RE.fullmatch(value.strip()))


def require_airflow_connection_id(value: object, *, context: str) -> str:
    """Return a logical Airflow Connection id or raise a redacted error."""

    if not isinstance(value, str):
        raise ValueError(f"{context} must be a logical Airflow Connection id")
    normalized = value.strip()
    if not _CONNECTION_ID_RE.fullmatch(normalized):
        raise ValueError(f"{context} must be a logical Airflow Connection id, not a URI or credentials")
    return normalized


def require_airflow_conn_env_name(value: object, *, context: str) -> str:
    """Return a safe ``AIRFLOW_CONN_*`` env/Secret key or raise a redacted error."""

    if not isinstance(value, str):
        raise ValueError(f"{context} must be a safe AIRFLOW_CONN_* key")
    normalized = value.strip()
    if not _ENV_NAME_RE.fullmatch(normalized):
        raise ValueError(f"{context} must be a safe AIRFLOW_CONN_* key")
    return normalized


def require_kubernetes_dns_label(value: object, *, context: str) -> str:
    """Return a safe Kubernetes DNS label or raise a redacted error."""

    if not isinstance(value, str):
        raise ValueError(f"{context} must be a safe Kubernetes Secret name")
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > KUBERNETES_DNS_LABEL_MAX_LENGTH
        or not _KUBERNETES_DNS_LABEL_RE.fullmatch(normalized)
    ):
        raise ValueError(f"{context} must be a safe Kubernetes Secret name")
    return normalized


__all__ = [
    "AIRFLOW_CONNECTION_ID_PATTERN",
    "AIRFLOW_CONN_ENV_NAME_PATTERN",
    "KUBERNETES_DNS_LABEL_MAX_LENGTH",
    "KUBERNETES_DNS_LABEL_PATTERN",
    "is_valid_airflow_connection_id",
    "require_kubernetes_dns_label",
    "require_airflow_conn_env_name",
    "require_airflow_connection_id",
]
