"""Dependency-free Airflow connection naming helpers."""

from __future__ import annotations

import re

AIRFLOW_CONNECTION_ID_PATTERN = r"^[A-Za-z0-9_.-]+$"
AIRFLOW_CONN_ENV_NAME_PATTERN = r"^AIRFLOW_CONN_[A-Z0-9_]+$"

_ENV_NAME_RE = re.compile(r"[^A-Za-z0-9_]")
_CONNECTION_ID_RE = re.compile(AIRFLOW_CONNECTION_ID_PATTERN)
_ENV_NAME_PATTERN_RE = re.compile(AIRFLOW_CONN_ENV_NAME_PATTERN)


def airflow_conn_env_name(connection_id: str) -> str:
    """Return the standard ``AIRFLOW_CONN_*`` variable name for a connection id."""

    suffix = _ENV_NAME_RE.sub("_", str(connection_id or "").strip()).upper()
    return f"AIRFLOW_CONN_{suffix}"


def is_valid_airflow_connection_id(value: object) -> bool:
    """Return True when value is a safe logical Airflow Connection id."""

    return isinstance(value, str) and bool(_CONNECTION_ID_RE.fullmatch(value.strip()))


def is_valid_airflow_conn_env_name(value: object) -> bool:
    """Return True when value is a safe ``AIRFLOW_CONN_*`` env/Secret key."""

    return isinstance(value, str) and bool(_ENV_NAME_PATTERN_RE.fullmatch(value.strip()))


__all__ = [
    "AIRFLOW_CONNECTION_ID_PATTERN",
    "AIRFLOW_CONN_ENV_NAME_PATTERN",
    "airflow_conn_env_name",
    "is_valid_airflow_conn_env_name",
    "is_valid_airflow_connection_id",
]
