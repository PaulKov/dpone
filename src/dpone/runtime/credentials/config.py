"""Конфигурации для подключения к источникам/приёмникам."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dpone._compat import StrEnum


class ConnectionType(StrEnum):
    POSTGRES = "postgres"
    CLICKHOUSE = "clickhouse"
    BIGQUERY = "bigquery"
    MSSQL = "mssql"
    MYSQL = "mysql"
    KAFKA = "kafka"


class CredentialsSource(StrEnum):
    ENVIRONMENT = "env"
    VAULT = "vault"
    AIRFLOW = "airflow"
    PARAMS = "params"


def require_connection_id(connection_id: str | None, *, backend: str) -> str:
    """Normalize the logical id required before any connector is constructed."""

    normalized = str(connection_id or "").strip()
    if not normalized:
        raise ValueError(f"{backend} state connector requires a non-empty connection_id")
    return normalized


@dataclass
class CredentialsConfig:
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    schema: str | None = None
    additional_params: dict[str, Any] | None = field(default=None, repr=False)
    project_id: str | None = None
    service_account_key_file: str | None = field(default=None, repr=False)
    service_account_info: dict[str, Any] | None = field(default=None, repr=False)
    endpoint: str | None = None
    token: str | None = field(default=None, repr=False)
    api_key: str | None = field(default=None, repr=False)
    secure: bool = False
    compression: bool = True
    connect_timeout: int = 10
    send_receive_timeout: int = 300
    settings: dict[str, Any] | None = None
    driver: str | None = None
    encrypt: str | bool | None = None
    trust_server_certificate: str | bool | None = None
    query_timeout: int | None = None
    bcp_path: str | None = None
    bootstrap_servers: str | None = None
    security_protocol: str | None = None
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = field(default=None, repr=False)
    ssl_ca_location: str | None = None
    client_id: str | None = None
    schema_registry_url: str | None = None
    schema_registry_username: str | None = None
    schema_registry_password: str | None = field(default=None, repr=False)
