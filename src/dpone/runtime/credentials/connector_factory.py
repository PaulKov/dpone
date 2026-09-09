"""Connector construction helpers backed by configured credentials."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dpone.runtime.credentials.manager import CredentialsManager

if TYPE_CHECKING:
    from dpone.runtime.connectors.bigquery import BigQueryConnector
    from dpone.runtime.connectors.clickhouse import ClickHouseConnector
    from dpone.runtime.connectors.kafka import KafkaConnector
    from dpone.runtime.connectors.mssql import MSSQLConnector
    from dpone.runtime.connectors.mysql import MySQLConnector
    from dpone.runtime.connectors.postgres import PostgresConnector
    from dpone.runtime.credentials.config import CredentialsSource

logger = logging.getLogger(__name__)


class BaseFactory:
    """Shared connector construction helpers."""

    manager: CredentialsManager | None = None

    @classmethod
    def _manager(cls) -> CredentialsManager:
        """Return the legacy credential manager, creating it on explicit use."""

        manager = cls.manager
        if manager is None:
            manager = CredentialsManager()
            BaseFactory.manager = manager
        return manager

    @classmethod
    def _create_postgres_connector(
        cls,
        connection_id: str,
        credentials_source: CredentialsSource,
        autocommit: bool = True,
        mount_point: str | None = None,
        path: str | None = None,
    ) -> PostgresConnector:
        creds = cls._manager().get_credentials(connection_id, credentials_source, mount_point, path)
        if not creds.host or not creds.database or not creds.username or not creds.password:
            raise ValueError("Неполные креденшиалы для PostgreSQL")
        from dpone.runtime.connectors.postgres import PostgresConnector

        return PostgresConnector(
            host=creds.host,
            port=creds.port or 5432,
            database=creds.database,
            user=creds.username,
            password=creds.password,
            application_name="dpone",
            autocommit=autocommit,
        )

    @classmethod
    def _create_bigquery_connector(
        cls,
        connection_id: str,
        credentials_source: CredentialsSource,
        mount_point: str | None = None,
        path: str | None = None,
        proxy_enable: bool = False,
        proxy_mount_point: str | None = None,
        proxy_path: str = "network/proxy/gcp/current",
    ) -> BigQueryConnector:
        from google.oauth2 import service_account

        from dpone.runtime.connectors.bigquery import BigQueryConnector

        creds = cls._manager().get_credentials(connection_id, credentials_source, mount_point, path)
        key_info = creds.service_account_info or creds.additional_params or {}
        if not key_info and not creds.service_account_key_file:
            raise ValueError("Отсутствуют service account данные для BigQuery")
        credentials = (
            service_account.Credentials.from_service_account_file(creds.service_account_key_file)
            if creds.service_account_key_file
            else service_account.Credentials.from_service_account_info(key_info)
        )
        project_id = creds.project_id or key_info.get("project_id") or getattr(credentials, "project_id", None)
        if not project_id:
            raise ValueError("Не указан project_id для BigQuery")
        proxy_manager = None
        if proxy_enable:
            from vault_kv_client import get_default_manager

            from dpone.runtime.connectors.proxy import GCPProxyManager

            resolved_proxy_mount_point = proxy_mount_point or mount_point
            if not resolved_proxy_mount_point:
                raise ValueError(
                    "Proxy requested but proxy_mount_point не указан и не может быть выведен из mount_point"
                )
            proxy_manager = GCPProxyManager(
                credentials=credentials,
                proxy_mount_point=resolved_proxy_mount_point,
                proxy_path=proxy_path,
                vault_manager_loader=get_default_manager,
            )
            logger.info(
                "BigQuery connector will use proxy: mount_point=%s, path=%s", resolved_proxy_mount_point, proxy_path
            )
        return BigQueryConnector(project_id=project_id, credentials=credentials, proxy_manager=proxy_manager)

    @classmethod
    def _create_clickhouse_connector(
        cls,
        connection_id: str,
        credentials_source: CredentialsSource,
        mount_point: str | None = None,
        path: str | None = None,
        secure: bool = False,
        compression: bool = True,
        connect_timeout: int = 10,
        send_receive_timeout: int = 300,
        settings: dict | None = None,
    ) -> ClickHouseConnector:
        from dpone.runtime.connectors.clickhouse import ClickHouseConnector

        creds = cls._manager().get_credentials(connection_id, credentials_source, mount_point, path)
        if not creds.host or not creds.username:
            raise ValueError("Неполные креденшиалы для ClickHouse (требуется host, username)")
        params = creds.additional_params or {}
        driver = _clickhouse_driver(params, explicit=creds.driver)
        secure_value = bool(creds.secure if hasattr(creds, "secure") else secure)
        database = creds.database or "default"
        return ClickHouseConnector(
            host=creds.host,
            port=creds.port or _clickhouse_default_port(driver=driver, secure=secure_value),
            database=database,
            user=creds.username,
            password=creds.password or "",
            application_name="dpone-clickhouse",
            secure=secure_value,
            compression=creds.compression if hasattr(creds, "compression") else compression,
            connect_timeout=creds.connect_timeout if hasattr(creds, "connect_timeout") else connect_timeout,
            send_receive_timeout=creds.send_receive_timeout
            if hasattr(creds, "send_receive_timeout")
            else send_receive_timeout,
            settings=creds.settings if hasattr(creds, "settings") else settings,
            driver=driver,
            ca_cert=_param_value(params, "ca_cert", "tls_cert_file", "tls_ca_cert"),
        )

    @classmethod
    def _create_kafka_connector(
        cls,
        connection_id: str,
        credentials_source: CredentialsSource,
        mount_point: str | None = None,
        path: str | None = None,
    ) -> KafkaConnector:
        from dpone.runtime.connectors.kafka import KafkaConnector

        creds = cls._manager().get_credentials(connection_id, credentials_source, mount_point, path)
        params = creds.additional_params or {}
        bootstrap_servers = (
            creds.bootstrap_servers or params.get("bootstrap_servers") or params.get("bootstrap.servers")
        )
        if not bootstrap_servers:
            raise ValueError("Неполные креденшиалы для Kafka (требуется bootstrap_servers)")
        return KafkaConnector(
            bootstrap_servers=bootstrap_servers,
            security_protocol=creds.security_protocol
            or params.get("security_protocol")
            or params.get("security.protocol"),
            sasl_mechanism=creds.sasl_mechanism or params.get("sasl_mechanism") or params.get("sasl.mechanism"),
            sasl_username=creds.sasl_username
            or params.get("sasl_username")
            or params.get("sasl.username")
            or creds.username,
            sasl_password=creds.sasl_password
            or params.get("sasl_password")
            or params.get("sasl.password")
            or creds.password,
            ssl_ca_location=creds.ssl_ca_location or params.get("ssl_ca_location") or params.get("ssl.ca.location"),
            client_id=creds.client_id or params.get("client_id", "dpone"),
            schema_registry_url=creds.schema_registry_url or params.get("schema_registry_url"),
            schema_registry_username=creds.schema_registry_username or params.get("schema_registry_username"),
            schema_registry_password=creds.schema_registry_password or params.get("schema_registry_password"),
            additional_config={k: v for k, v in params.items() if "." in str(k)},
        )

    @classmethod
    def _create_mysql_connector(
        cls,
        connection_id: str,
        credentials_source: CredentialsSource,
        autocommit: bool = True,
        mount_point: str | None = None,
        path: str | None = None,
    ) -> MySQLConnector:
        creds = cls._manager().get_credentials(connection_id, credentials_source, mount_point, path)
        if not creds.host or not creds.database or not creds.username or not creds.password:
            raise ValueError("Неполные креденшиалы для MySQL (требуются host, database, username, password)")
        from dpone.runtime.connectors.mysql import MySQLConnector

        params = creds.additional_params or {}
        return MySQLConnector(
            host=creds.host,
            port=creds.port or 3306,
            database=creds.database,
            user=creds.username,
            password=creds.password,
            connect_timeout=int(creds.connect_timeout or 10),
            autocommit=autocommit,
            ssl=_mysql_ssl_settings(creds, params),
        )

    @classmethod
    def _create_mssql_connector(
        cls,
        connection_id: str,
        credentials_source: CredentialsSource,
        autocommit: bool = True,
        mount_point: str | None = None,
        path: str | None = None,
    ) -> MSSQLConnector:
        from dpone.runtime.connectors.mssql import MSSQLConnector

        creds = cls._manager().get_credentials(connection_id, credentials_source, mount_point, path)
        params = creds.additional_params or {}
        if not creds.host or not creds.database:
            raise ValueError("Неполные креденшиалы для MSSQL (требуются host и database)")
        return MSSQLConnector(
            host=creds.host,
            port=creds.port or 1433,
            database=creds.database,
            user=creds.username,
            password=creds.password,
            driver=creds.driver or _param_value(params, "driver") or "ODBC Driver 18 for SQL Server",
            encrypt=creds.encrypt if creds.encrypt is not None else _param_value(params, "encrypt") or "yes",
            trust_server_certificate=creds.trust_server_certificate
            if creds.trust_server_certificate is not None
            else _param_value(params, "trust_server_certificate", "TrustServerCertificate") or "no",
            connect_timeout=_mssql_connect_timeout(creds, params),
            query_timeout=creds.query_timeout or int(_param_value(params, "query_timeout", "QueryTimeout") or 0),
            autocommit=autocommit,
            bcp_path=creds.bcp_path or _param_value(params, "bcp_path") or "bcp",
            odbc_options=params,
        )


def _mssql_connect_timeout(creds, params: dict) -> int:
    raw = _param_value(params, "connect_timeout", "login_timeout", "LoginTimeout") or creds.connect_timeout
    return int(raw or 0)


def _mysql_ssl_settings(creds, params: dict) -> dict[str, object] | bool | None:
    """Map Airflow/Vault extras and env SSL fields into PyMySQL ``ssl=``."""

    if isinstance(params.get("ssl"), dict):
        return dict(params["ssl"])
    if isinstance(params.get("ssl"), bool):
        return params["ssl"]

    ca = (
        _param_value(params, "ssl_ca", "ca", "ssl.ca")
        or getattr(creds, "ssl_ca_location", None)
        or _param_value(params, "ssl_ca_location")
    )
    cert = _param_value(params, "ssl_cert", "cert", "ssl.cert")
    key = _param_value(params, "ssl_key", "key", "ssl.key")
    mode = str(_param_value(params, "ssl_mode", "ssl.mode") or "").strip().upper()
    ssl_cfg: dict[str, object] = {}
    if ca:
        ssl_cfg["ca"] = str(ca)
    if cert:
        ssl_cfg["cert"] = str(cert)
    if key:
        ssl_cfg["key"] = str(key)
    verify_modes = {"VERIFY_CA", "VERIFY_IDENTITY", "REQUIRED_IDENTITY"}
    if mode in verify_modes and "ca" not in ssl_cfg:
        raise ValueError(
            f"MySQL ssl_mode={mode} requires ssl_ca (or ssl.ca / ssl_ca_location); "
            "refusing encryption-only downgrade without server CA verification."
        )
    if mode == "VERIFY_IDENTITY" and "ca" in ssl_cfg:
        ssl_cfg.setdefault("check_hostname", True)
    if mode in {"REQUIRED", *verify_modes}:
        # PyMySQL accepts an empty/partial ssl dict as "use TLS" for REQUIRED.
        return ssl_cfg or True
    if ssl_cfg:
        return ssl_cfg
    if mode in {"DISABLED", "DISABLED_TLS", "FALSE", "0"}:
        return None
    return None


def _clickhouse_driver(params: dict, *, explicit: str | None = None) -> str:
    raw = explicit or _param_value(params, "driver", "interface", "protocol", "transport", "client")
    value = str(raw or "native").strip().lower().replace("-", "_")
    if value in {"https", "http_tls"}:
        return "http"
    return value or "native"


def _clickhouse_default_port(*, driver: str, secure: bool) -> int:
    if driver in {"http", "clickhouse_connect", "connect"}:
        return 8443 if secure else 8123
    return 9440 if secure else 9000


def _param_value(params: dict, *names: str):
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


__all__ = ["BaseFactory"]
