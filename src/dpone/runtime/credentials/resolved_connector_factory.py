"""Construct runtime connectors from one already-resolved connection snapshot."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.runtime.credentials.config import CredentialsConfig


from collections.abc import Mapping
from typing import Any


class ResolvedConnectorFactory:
    """Dependency-injected connector factory with no credential discovery."""

    @classmethod
    def create(
        cls,
        connection: ResolvedBindingConnection,
        *,
        autocommit: bool = True,
        proxy_connection: ResolvedBindingConnection | None = None,
    ) -> Any:
        descriptor = connection.descriptor
        if descriptor is None:
            raise ValueError("Resolved connection descriptor is required")
        connection_type = descriptor.connection_type.strip().lower().replace("-", "_")
        credentials = connection.credentials
        if connection_type == "postgres":
            return cls.postgres(credentials, autocommit=autocommit)
        if connection_type == "clickhouse":
            return cls.clickhouse(credentials)
        if connection_type == "mssql":
            return cls.mssql(credentials, autocommit=autocommit)
        if connection_type == "kafka":
            return cls.kafka(credentials)
        if connection_type == "bigquery":
            return cls.bigquery(credentials, proxy_connection=proxy_connection)
        raise NotImplementedError(f"Unsupported resolved connection type: {connection_type}")

    @staticmethod
    def postgres(
        credentials: CredentialsConfig,
        *,
        autocommit: bool = True,
    ) -> Any:
        if not credentials.host or not credentials.database or not credentials.username or not credentials.password:
            raise ValueError("PostgreSQL credentials require host, database, username, and password")
        from dpone.runtime.connectors.postgres import PostgresConnector

        return PostgresConnector(
            host=credentials.host,
            port=credentials.port or 5432,
            database=credentials.database,
            user=credentials.username,
            password=credentials.password,
            application_name="dpone",
            autocommit=autocommit,
        )

    @staticmethod
    def clickhouse(credentials: CredentialsConfig) -> Any:
        if not credentials.host or not credentials.username:
            raise ValueError("ClickHouse credentials require host and username")
        from dpone.runtime.connectors.clickhouse import ClickHouseConnector

        params = credentials.additional_params or {}
        driver = _clickhouse_driver(params, explicit=credentials.driver)
        secure = bool(credentials.secure)
        return ClickHouseConnector(
            host=credentials.host,
            port=credentials.port or _clickhouse_default_port(driver=driver, secure=secure),
            database=credentials.database or "default",
            user=credentials.username,
            password=credentials.password or "",
            application_name="dpone-clickhouse",
            secure=secure,
            compression=credentials.compression,
            connect_timeout=credentials.connect_timeout,
            send_receive_timeout=credentials.send_receive_timeout,
            settings=credentials.settings,
            driver=driver,
            ca_cert=_param_value(params, "ca_cert", "tls_cert_file", "tls_ca_cert"),
        )

    @staticmethod
    def mssql(
        credentials: CredentialsConfig,
        *,
        autocommit: bool = True,
    ) -> Any:
        if not credentials.host or not credentials.database:
            raise ValueError("MSSQL credentials require host and database")
        from dpone.runtime.connectors.mssql import MSSQLConnector

        params = credentials.additional_params or {}
        return MSSQLConnector(
            host=credentials.host,
            port=credentials.port or 1433,
            database=credentials.database,
            user=credentials.username,
            password=credentials.password,
            driver=credentials.driver or _param_value(params, "driver") or "ODBC Driver 18 for SQL Server",
            encrypt=(
                credentials.encrypt if credentials.encrypt is not None else _param_value(params, "encrypt") or "yes"
            ),
            trust_server_certificate=(
                credentials.trust_server_certificate
                if credentials.trust_server_certificate is not None
                else _param_value(
                    params,
                    "trust_server_certificate",
                    "TrustServerCertificate",
                )
                or "no"
            ),
            connect_timeout=int(
                _param_value(params, "connect_timeout", "login_timeout", "LoginTimeout")
                or credentials.connect_timeout
                or 0
            ),
            query_timeout=credentials.query_timeout or int(_param_value(params, "query_timeout", "QueryTimeout") or 0),
            autocommit=autocommit,
            bcp_path=credentials.bcp_path or _param_value(params, "bcp_path") or "bcp",
            odbc_options=params,
        )

    @staticmethod
    def kafka(credentials: CredentialsConfig) -> Any:
        from dpone.runtime.connectors.kafka import KafkaConnector

        params = credentials.additional_params or {}
        bootstrap_servers = (
            credentials.bootstrap_servers or params.get("bootstrap_servers") or params.get("bootstrap.servers")
        )
        if not bootstrap_servers:
            raise ValueError("Kafka credentials require bootstrap_servers")
        return KafkaConnector(
            bootstrap_servers=bootstrap_servers,
            security_protocol=credentials.security_protocol
            or params.get("security_protocol")
            or params.get("security.protocol"),
            sasl_mechanism=credentials.sasl_mechanism or params.get("sasl_mechanism") or params.get("sasl.mechanism"),
            sasl_username=credentials.sasl_username
            or params.get("sasl_username")
            or params.get("sasl.username")
            or credentials.username,
            sasl_password=credentials.sasl_password
            or params.get("sasl_password")
            or params.get("sasl.password")
            or credentials.password,
            ssl_ca_location=credentials.ssl_ca_location
            or params.get("ssl_ca_location")
            or params.get("ssl.ca.location"),
            client_id=credentials.client_id or params.get("client_id", "dpone"),
            schema_registry_url=credentials.schema_registry_url or params.get("schema_registry_url"),
            schema_registry_username=credentials.schema_registry_username or params.get("schema_registry_username"),
            schema_registry_password=credentials.schema_registry_password or params.get("schema_registry_password"),
            additional_config={key: value for key, value in params.items() if "." in str(key)},
        )

    @staticmethod
    def bigquery(
        credentials: CredentialsConfig,
        *,
        proxy_connection: ResolvedBindingConnection | None = None,
    ) -> Any:
        from google.oauth2 import service_account

        from dpone.runtime.connectors.bigquery import BigQueryConnector

        key_info = credentials.service_account_info or credentials.additional_params or {}
        if not key_info and not credentials.service_account_key_file:
            raise ValueError("BigQuery credentials require service account data")
        google_credentials = (
            service_account.Credentials.from_service_account_file(credentials.service_account_key_file)
            if credentials.service_account_key_file
            else service_account.Credentials.from_service_account_info(key_info)
        )
        project_id = (
            credentials.project_id or key_info.get("project_id") or getattr(google_credentials, "project_id", None)
        )
        if not project_id:
            raise ValueError("BigQuery credentials require project_id")
        proxy_manager = None
        if proxy_connection is not None:
            from dpone.runtime.connectors.proxy import GCPProxyManager

            proxy_manager = GCPProxyManager.from_resolved_connection(
                credentials=google_credentials,
                connection=proxy_connection,
            )
        return BigQueryConnector(
            project_id=project_id,
            credentials=google_credentials,
            proxy_manager=proxy_manager,
        )


def _clickhouse_driver(
    params: Mapping[str, Any],
    *,
    explicit: str | None = None,
) -> str:
    raw = explicit or _param_value(
        params,
        "driver",
        "interface",
        "protocol",
        "transport",
        "client",
    )
    value = str(raw or "native").strip().lower().replace("-", "_")
    return "http" if value in {"https", "http_tls"} else value or "native"


def _clickhouse_default_port(*, driver: str, secure: bool) -> int:
    if driver in {"http", "clickhouse_connect", "connect"}:
        return 8443 if secure else 8123
    return 9440 if secure else 9000


def _param_value(params: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in params:
            return params[name]
    wanted = {_token(name) for name in names}
    for key, value in params.items():
        if _token(key) in wanted:
            return value
    return None


def _token(value: object) -> str:
    return "".join(character for character in str(value or "").lower() if character.isalnum())


__all__ = ["ResolvedConnectorFactory"]
