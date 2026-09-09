from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from dpone.runtime.credentials.config import CredentialsConfig

if TYPE_CHECKING:
    from vault_kv_client import VaultManager
else:
    VaultManager = Any

logger = logging.getLogger(__name__)


def get_default_manager() -> VaultManager:
    try:
        from vault_kv_client import get_default_manager as load_default_manager
    except ImportError as exc:  # pragma: no cover - depends on optional runtime extra
        raise RuntimeError("vault-kv-client is not installed") from exc
    return load_default_manager()


class VaultCredentialsProvider:
    """Vault-backed credentials provider."""

    def __init__(self, vault_manager: VaultManager | None = None):
        self.vault_manager = vault_manager

    def get_credentials(self, connection_name: str, mount_point: str = None, path: str = None) -> CredentialsConfig:
        try:
            actual_mount_point = mount_point or "secret"
            actual_path = path or connection_name
            vault_manager = self.vault_manager or get_default_manager()
            secret_data = vault_manager.get_secret(mount_point=actual_mount_point, path=actual_path)
            service_account_info = secret_data.get("service_account") or secret_data.get("service_account_info")
            if not service_account_info and secret_data.get("credentials_json"):
                try:
                    service_account_info = json.loads(secret_data["credentials_json"])
                except json.JSONDecodeError as exc:
                    logger.error("Не удалось распарсить credentials_json из Vault: %s", exc)
                    raise ValueError("Некорректный JSON формата service account в credentials_json") from exc
            project_id = secret_data.get("project_id")
            if not project_id and isinstance(service_account_info, dict):
                project_id = service_account_info.get("project_id")

            return CredentialsConfig(
                host=secret_data.get("host") or secret_data.get("db_host"),
                port=self._extract_port(secret_data),
                database=secret_data.get("database") or secret_data.get("db_database"),
                username=secret_data.get("username") or secret_data.get("user") or secret_data.get("db_user"),
                password=secret_data.get("password") or secret_data.get("db_password"),
                schema=secret_data.get("schema"),
                project_id=project_id,
                service_account_key_file=secret_data.get("service_account_key_file") or secret_data.get("keyfile_path"),
                service_account_info=service_account_info,
                endpoint=secret_data.get("endpoint") or secret_data.get("base_url"),
                token=secret_data.get("token") or secret_data.get("bearer_token"),
                api_key=secret_data.get("api_key"),
                additional_params=self._extract_additional_params(secret_data),
                secure=self._extract_bool(secret_data.get("secure"), default=False),
                compression=self._extract_bool(secret_data.get("compression"), default=True),
                connect_timeout=self._extract_int(secret_data.get("connect_timeout")) or 10,
                send_receive_timeout=self._extract_int(secret_data.get("send_receive_timeout")) or 300,
                settings=secret_data.get("settings") if isinstance(secret_data.get("settings"), dict) else None,
                driver=secret_data.get("driver"),
                encrypt=secret_data.get("encrypt"),
                trust_server_certificate=secret_data.get("trust_server_certificate"),
                query_timeout=self._extract_int(secret_data.get("query_timeout")),
                bcp_path=secret_data.get("bcp_path"),
                bootstrap_servers=secret_data.get("bootstrap_servers") or secret_data.get("bootstrap.servers"),
                security_protocol=secret_data.get("security_protocol") or secret_data.get("security.protocol"),
                sasl_mechanism=secret_data.get("sasl_mechanism") or secret_data.get("sasl.mechanism"),
                sasl_username=secret_data.get("sasl_username") or secret_data.get("sasl.username"),
                sasl_password=secret_data.get("sasl_password") or secret_data.get("sasl.password"),
                ssl_ca_location=secret_data.get("ssl_ca_location") or secret_data.get("ssl.ca.location"),
                client_id=secret_data.get("client_id"),
                schema_registry_url=secret_data.get("schema_registry_url"),
                schema_registry_username=secret_data.get("schema_registry_username"),
                schema_registry_password=secret_data.get("schema_registry_password"),
            )

        except Exception as exc:
            logger.error("Ошибка получения креденшиалов из Vault; error_type=%s", type(exc).__name__)
            raise ValueError("Не удалось получить креденшиалы из Vault") from exc

    def _extract_int(self, value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def _extract_bool(self, value: Any, *, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    def _extract_port(self, secret_data: dict[str, Any]) -> int | None:
        port_value = secret_data.get("port") or secret_data.get("db_port")
        if port_value:
            return self._extract_int(port_value)
        return None

    def _extract_additional_params(self, secret_data: dict[str, Any]) -> dict[str, Any] | None:
        additional_params = {}
        standard_fields = {
            "host",
            "port",
            "database",
            "username",
            "password",
            "schema",
            "project_id",
            "service_account",
            "service_account_info",
            "service_account_key_file",
            "keyfile_path",
            "endpoint",
            "base_url",
            "token",
            "bearer_token",
            "api_key",
            "secure",
            "compression",
            "connect_timeout",
            "send_receive_timeout",
            "settings",
            "bootstrap_servers",
            "bootstrap.servers",
            "security_protocol",
            "security.protocol",
            "sasl_mechanism",
            "sasl.mechanism",
            "sasl_username",
            "sasl.username",
            "sasl_password",
            "sasl.password",
            "ssl_ca_location",
            "ssl.ca.location",
            "client_id",
            "schema_registry_url",
            "schema_registry_username",
            "schema_registry_password",
        }
        for key, value in secret_data.items():
            if key not in standard_fields:
                additional_params[key] = value
        return additional_params if additional_params else None
