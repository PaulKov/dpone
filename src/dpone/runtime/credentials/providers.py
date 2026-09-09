"""Провайдеры получения креденшиалов."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from dpone.contracts.credential_env import CANONICAL_CONNECTION_ENV_PREFIX, connection_env_suffix
from dpone.runtime.credentials.airflow_base_hook import load_airflow_base_hook
from dpone.runtime.credentials.airflow_env import (
    AirflowConnectionEnvError,
    airflow_conn_env_name,
    credentials_from_airflow_env,
)
from dpone.runtime.credentials.config import CredentialsConfig

logger = logging.getLogger(__name__)

_VAULT_EXPORTS = {"VaultCredentialsProvider", "get_default_manager"}


def __getattr__(name: str) -> Any:
    if name in _VAULT_EXPORTS:
        module = import_module("dpone.runtime.credentials.vault_provider")
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class CredentialsProvider:
    """Базовый класс провайдеров."""

    def get_credentials(self, connection_name: str) -> CredentialsConfig:
        raise NotImplementedError


@dataclass
class EnvironmentCredentialsProvider(CredentialsProvider):
    prefix: str = CANONICAL_CONNECTION_ENV_PREFIX
    legacy_prefixes: tuple[str, ...] = ("", "DPONE_")

    def get_credentials(self, connection_name: str) -> CredentialsConfig:
        env_keys = self._env_keys(connection_name)
        return CredentialsConfig(
            host=self._getenv(env_keys, "HOST"),
            port=int(self._getenv(env_keys, "PORT") or "0") or None,
            database=self._getenv(env_keys, "DATABASE"),
            username=self._getenv(env_keys, "USERNAME") or self._getenv(env_keys, "USER"),
            password=self._getenv(env_keys, "PASSWORD"),
            schema=self._getenv(env_keys, "SCHEMA"),
            project_id=self._getenv(env_keys, "PROJECT_ID"),
            service_account_key_file=self._getenv(env_keys, "SERVICE_ACCOUNT_KEY_FILE"),
            service_account_info=self._parse_service_account_info(env_keys),
            endpoint=self._getenv(env_keys, "ENDPOINT") or self._getenv(env_keys, "BASE_URL"),
            token=self._getenv(env_keys, "TOKEN") or self._getenv(env_keys, "BEARER_TOKEN"),
            api_key=self._getenv(env_keys, "API_KEY"),
            additional_params=self._parse_additional_params(env_keys),
            secure=self._parse_bool(self._getenv(env_keys, "SECURE"), default=False),
            compression=self._parse_bool(self._getenv(env_keys, "COMPRESSION"), default=True),
            connect_timeout=int(self._getenv(env_keys, "CONNECT_TIMEOUT") or "10"),
            send_receive_timeout=int(self._getenv(env_keys, "SEND_RECEIVE_TIMEOUT") or "300"),
            settings=self._parse_json_env(self._getenv(env_keys, "SETTINGS")),
            driver=self._getenv(env_keys, "DRIVER"),
            encrypt=self._getenv(env_keys, "ENCRYPT"),
            trust_server_certificate=self._getenv(env_keys, "TRUST_SERVER_CERTIFICATE"),
            query_timeout=int(self._getenv(env_keys, "QUERY_TIMEOUT") or "0") or None,
            bcp_path=self._getenv(env_keys, "BCP_PATH"),
            bootstrap_servers=self._getenv(env_keys, "BOOTSTRAP_SERVERS"),
            security_protocol=self._getenv(env_keys, "SECURITY_PROTOCOL"),
            sasl_mechanism=self._getenv(env_keys, "SASL_MECHANISM"),
            sasl_username=self._getenv(env_keys, "SASL_USERNAME"),
            sasl_password=self._getenv(env_keys, "SASL_PASSWORD"),
            ssl_ca_location=self._getenv(env_keys, "SSL_CA_LOCATION"),
            client_id=self._getenv(env_keys, "CLIENT_ID"),
            schema_registry_url=self._getenv(env_keys, "SCHEMA_REGISTRY_URL"),
            schema_registry_username=self._getenv(env_keys, "SCHEMA_REGISTRY_USERNAME"),
            schema_registry_password=self._getenv(env_keys, "SCHEMA_REGISTRY_PASSWORD"),
        )

    def _env_keys(self, connection_name: str) -> tuple[str, ...]:
        suffix = connection_env_suffix(connection_name)
        primary = f"{self.prefix}{suffix}".upper()
        keys = [primary]
        if self.prefix == CANONICAL_CONNECTION_ENV_PREFIX:
            keys.extend(f"{legacy_prefix}{suffix}".upper() for legacy_prefix in self.legacy_prefixes)
        return tuple(dict.fromkeys(keys))

    @staticmethod
    def _getenv(env_keys: tuple[str, ...], field: str) -> str | None:
        for env_key in env_keys:
            value = os.getenv(f"{env_key}_{field}")
            if value is not None:
                return value
        return None

    def _parse_additional_params(self, env_keys: tuple[str, ...]) -> dict[str, Any] | None:
        params: dict[str, Any] = {}
        for env_key in env_keys:
            for key, value in os.environ.items():
                if key.startswith(f"{env_key}_ADDITIONAL_"):
                    param_name = key.replace(f"{env_key}_ADDITIONAL_", "").lower()
                    params.setdefault(param_name, value)
        return params or None

    def _parse_service_account_info(self, env_keys: tuple[str, ...]) -> dict[str, Any] | None:
        raw = self._getenv(env_keys, "SERVICE_ACCOUNT_INFO") or self._getenv(env_keys, "CREDENTIALS_JSON")
        return self._parse_json_env(raw)

    @staticmethod
    def _parse_json_env(raw: str | None) -> dict[str, Any] | None:
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Не удалось распарсить JSON env значение")
            return None
        return parsed if isinstance(parsed, dict) else None

    @staticmethod
    def _parse_bool(value: str | None, *, default: bool) -> bool:
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}


class AirflowCredentialsProvider(CredentialsProvider):
    def __init__(self):
        self.BaseHook = load_airflow_base_hook()

    def get_credentials(self, connection_name: str) -> CredentialsConfig:
        if not self.BaseHook:
            try:
                return credentials_from_airflow_env(connection_name)
            except AirflowConnectionEnvError as exc:
                env_name = airflow_conn_env_name(connection_name)
                raise RuntimeError(
                    "Airflow is not installed and the runtime-only AIRFLOW_CONN env bridge is missing. "
                    f"Set {env_name} through Kubernetes Secret/env or use an Airflow-in-runtime image."
                ) from exc

        conn = self.BaseHook.get_connection(connection_name)
        conn_type = conn.conn_type.lower()

        if conn_type == "postgres":
            return self._from_postgres(conn)
        if conn_type in {"mysql", "mariadb"}:
            return self._from_mysql(conn)
        if conn_type in {"mssql", "microsoft mssql", "sqlserver", "odbc"}:
            return self._from_mssql(conn)
        if conn_type in {"clickhouse", "ch"}:
            return self._from_clickhouse(conn)
        if conn_type in {"bigquery", "google_cloud_platform", "gcp", "google_cloud"}:
            return self._from_bigquery(conn)
        if conn_type in {"kafka", "confluent"}:
            return self._from_kafka(conn)
        if conn_type in {"aws", "s3", "amazon_web_services"}:
            return self._from_aws(conn)
        if conn_type in {"http", "https", "rest", "api", "generic_rest"}:
            return self._from_rest(conn)
        if conn_type == "generic":
            return self._from_generic(conn)
        raise NotImplementedError(f"Пока не поддерживаем {conn_type}")

    @staticmethod
    def _from_postgres(conn) -> CredentialsConfig:
        extra = AirflowCredentialsProvider._parse_extra(conn.extra)
        return CredentialsConfig(
            host=conn.host,
            port=conn.port,
            database=conn.schema,
            username=conn.login,
            password=conn.password,
            schema=conn.schema,
            additional_params=extra,
        )

    @staticmethod
    def _from_mysql(conn) -> CredentialsConfig:
        extra = AirflowCredentialsProvider._parse_extra(conn.extra)
        return CredentialsConfig(
            host=conn.host,
            port=conn.port,
            database=conn.schema,
            username=conn.login,
            password=conn.password,
            schema=conn.schema,
            additional_params=extra,
            connect_timeout=int(_extra_value(extra, "connect_timeout", "login_timeout", "LoginTimeout") or 10),
        )

    @staticmethod
    def _from_mssql(conn) -> CredentialsConfig:
        extra = AirflowCredentialsProvider._parse_extra(conn.extra)
        return CredentialsConfig(
            host=conn.host,
            port=conn.port,
            database=conn.schema,
            username=conn.login,
            password=conn.password,
            schema=conn.schema,
            additional_params=extra,
            driver=_extra_value(extra, "driver"),
            encrypt=_extra_value(extra, "encrypt"),
            trust_server_certificate=_extra_value(extra, "trust_server_certificate", "TrustServerCertificate"),
            connect_timeout=int(_extra_value(extra, "connect_timeout", "login_timeout", "LoginTimeout") or 10),
            query_timeout=_extra_value(extra, "query_timeout", "QueryTimeout"),
            bcp_path=_extra_value(extra, "bcp_path"),
        )

    @staticmethod
    def _from_kafka(conn) -> CredentialsConfig:
        extra = AirflowCredentialsProvider._parse_extra(conn.extra)
        return CredentialsConfig(
            host=conn.host,
            port=conn.port,
            username=conn.login,
            password=conn.password,
            additional_params=extra,
            bootstrap_servers=extra.get("bootstrap_servers") or extra.get("bootstrap.servers") or conn.host,
            security_protocol=extra.get("security_protocol") or extra.get("security.protocol"),
            sasl_mechanism=extra.get("sasl_mechanism") or extra.get("sasl.mechanism"),
            sasl_username=extra.get("sasl_username") or extra.get("sasl.username") or conn.login,
            sasl_password=extra.get("sasl_password") or extra.get("sasl.password") or conn.password,
            ssl_ca_location=extra.get("ssl_ca_location") or extra.get("ssl.ca.location"),
            client_id=extra.get("client_id"),
            schema_registry_url=extra.get("schema_registry_url"),
            schema_registry_username=extra.get("schema_registry_username"),
            schema_registry_password=extra.get("schema_registry_password"),
        )

    @staticmethod
    def _from_clickhouse(conn) -> CredentialsConfig:
        extra = AirflowCredentialsProvider._parse_extra(conn.extra)
        return CredentialsConfig(
            host=conn.host,
            port=conn.port,
            database=conn.schema,
            username=conn.login,
            password=conn.password,
            schema=conn.schema,
            additional_params=extra,
            driver=_extra_value(extra, "driver", "interface", "protocol", "transport", "client"),
            secure=AirflowCredentialsProvider._parse_bool(extra.get("secure"), default=False),
            compression=AirflowCredentialsProvider._parse_bool(extra.get("compression"), default=True),
            connect_timeout=int(extra.get("connect_timeout", 10) or 10),
            send_receive_timeout=int(extra.get("send_receive_timeout", 300) or 300),
            settings=extra.get("settings") if isinstance(extra.get("settings"), dict) else None,
        )

    @staticmethod
    def _from_generic(conn) -> CredentialsConfig:
        extra = AirflowCredentialsProvider._parse_extra(conn.extra)
        return CredentialsConfig(
            host=conn.host,
            port=conn.port,
            database=conn.schema,
            username=conn.login,
            password=conn.password,
            schema=conn.schema,
            endpoint=extra.get("endpoint") or extra.get("endpoint_url") or extra.get("base_url"),
            token=extra.get("token") or extra.get("bearer_token"),
            api_key=extra.get("api_key"),
            additional_params=extra,
            driver=_extra_value(extra, "driver", "interface", "protocol", "transport", "client"),
            secure=AirflowCredentialsProvider._parse_bool(extra.get("secure"), default=False),
            compression=AirflowCredentialsProvider._parse_bool(extra.get("compression"), default=True),
            connect_timeout=int(extra.get("connect_timeout", 10) or 10),
            send_receive_timeout=int(extra.get("send_receive_timeout", 300) or 300),
            settings=extra.get("settings") if isinstance(extra.get("settings"), dict) else None,
        )

    @staticmethod
    def _from_bigquery(conn) -> CredentialsConfig:
        extra = AirflowCredentialsProvider._parse_extra(conn.extra)
        service_account_info = (
            extra.get("service_account")
            or extra.get("service_account_info")
            or extra.get("keyfile_dict")
            or extra.get("extra__google_cloud_platform__keyfile_dict")
        )
        if isinstance(service_account_info, str):
            try:
                service_account_info = json.loads(service_account_info)
            except json.JSONDecodeError:
                service_account_info = None
        project_id = (
            extra.get("project_id")
            or extra.get("extra__google_cloud_platform__project")
            or extra.get("project")
            or conn.schema
        )
        return CredentialsConfig(
            project_id=project_id,
            service_account_info=service_account_info if isinstance(service_account_info, dict) else None,
            service_account_key_file=extra.get("key_path") or extra.get("keyfile_path"),
            additional_params=extra,
        )

    @staticmethod
    def _from_rest(conn) -> CredentialsConfig:
        extra = AirflowCredentialsProvider._parse_extra(conn.extra)
        endpoint = extra.get("endpoint") or extra.get("base_url")
        if not endpoint:
            schema = f":{conn.port}" if conn.port else ""
            endpoint = f"{conn.conn_type}://{conn.host}{schema}" if conn.host else None
        return CredentialsConfig(
            host=conn.host,
            port=conn.port,
            username=conn.login,
            password=conn.password,
            endpoint=endpoint,
            token=extra.get("token") or extra.get("bearer_token") or conn.password,
            api_key=extra.get("api_key"),
            additional_params=extra,
        )

    @staticmethod
    def _from_aws(conn) -> CredentialsConfig:
        extra = AirflowCredentialsProvider._parse_extra(conn.extra)
        endpoint = (
            extra.get("endpoint_url")
            or extra.get("endpoint")
            or extra.get("base_url")
            or (conn.host if str(conn.host or "").startswith(("http://", "https://")) else None)
        )
        return CredentialsConfig(
            host=conn.host,
            port=conn.port,
            username=conn.login or extra.get("aws_access_key_id"),
            password=conn.password or extra.get("aws_secret_access_key"),
            schema=conn.schema,
            endpoint=endpoint,
            token=extra.get("aws_session_token") or extra.get("session_token"),
            additional_params=extra,
        )

    @staticmethod
    def _parse_bool(value: Any, *, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _parse_extra(extra: str | None) -> dict[str, Any]:
        if not extra:
            return {}
        try:
            return json.loads(extra)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Не удалось распарсить extra параметры: %s", extra)
            return {}


def _extra_value(extra: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in extra:
            return extra[name]
    wanted = {_token(name) for name in names}
    for key, value in extra.items():
        if _token(key) in wanted:
            return value
    return None


def _token(value: object) -> str:
    return "".join(character for character in str(value or "").lower() if character.isalnum())
