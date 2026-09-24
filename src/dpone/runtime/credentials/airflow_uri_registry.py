"""Overlay registry-owned MSSQL catalog fields onto a parsed Airflow URI."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from dpone.airflow_connection_names import airflow_conn_env_name, is_valid_airflow_connection_id
from dpone.runtime.credentials.airflow_env import parse_airflow_connection_uri
from dpone.runtime.credentials.config import CredentialsConfig


def credentials_from_airflow_connection_uri(
    uri: str,
    *,
    connection: Mapping[str, Any] | None = None,
) -> CredentialsConfig:
    """Parse an Airflow URI, then let registry-owned catalog/schema win.

    Shared Airflow Connection URIs carry leftover catalog identity
    (``analytics_staging`` / ``etl_state``). Environment registry
    ``connection.database`` / ``connection.schema`` are the authority.
    Host, port, username and password stay URI-owned so a shared listener
    can serve multiple logical aliases.
    """

    parsed = parse_airflow_connection_uri(uri)
    if not isinstance(connection, Mapping):
        return parsed
    database = _optional_text(connection.get("database")) or parsed.database
    schema = _optional_text(connection.get("schema")) or parsed.schema
    if database == parsed.database and schema == parsed.schema:
        return parsed
    return replace(parsed, database=database, schema=schema)


def resolve_projected_airflow_env(
    *, connection: Mapping[str, Any], credentials: Mapping[str, Any]
) -> CredentialsConfig:
    """Read only the declared URI; preserve registry coordinates and redact errors."""
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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


__all__ = ["credentials_from_airflow_connection_uri", "resolve_projected_airflow_env"]
