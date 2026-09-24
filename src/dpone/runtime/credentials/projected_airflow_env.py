"""Resolve an explicitly projected Airflow URI without importing Airflow.

Only the canonical environment variable derived from the declared connection
ID is read. Registry coordinates retain authority over the URI's defaults.
Errors never include input values or parser exception details.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from dpone.airflow_connection_names import airflow_conn_env_name, is_valid_airflow_connection_id
from dpone.runtime.credentials.airflow_uri_registry import credentials_from_airflow_connection_uri
from dpone.runtime.credentials.config import CredentialsConfig


def resolve_projected_airflow_env(
    *, connection: Mapping[str, Any], credentials: Mapping[str, Any]
) -> CredentialsConfig:
    """Read the declared URI or fail closed with a value-free diagnostic."""
    connection_id = credentials.get("connection_id")
    if not is_valid_airflow_connection_id(connection_id):
        raise ValueError("airflow_env resolver requires a valid connection_id")
    uri = os.getenv(airflow_conn_env_name(str(connection_id)))
    if not uri:
        raise ValueError("airflow_env projected connection is missing")
    try:
        return credentials_from_airflow_connection_uri(uri, connection=connection)
    except (ValueError, TypeError, RuntimeError, NotImplementedError):
        raise ValueError("airflow_env projected connection URI is invalid") from None
