"""Overlay registry-owned MSSQL catalog fields onto a parsed Airflow URI."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any

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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


__all__ = ["credentials_from_airflow_connection_uri"]
