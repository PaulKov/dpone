"""Менеджер креденшиалов."""

from __future__ import annotations

import json

from dpone.runtime.credentials.config import CredentialsConfig, CredentialsSource
from dpone.runtime.credentials.providers import (
    AirflowCredentialsProvider,
    EnvironmentCredentialsProvider,
    VaultCredentialsProvider,
)


class CredentialsManager:
    def __init__(
        self,
        env_provider: EnvironmentCredentialsProvider | None = None,
        airflow_provider: AirflowCredentialsProvider | None = None,
        vault_provider: VaultCredentialsProvider | None = None,
    ):
        self._env_provider = env_provider
        self._airflow_provider = airflow_provider
        self._vault_provider = vault_provider

    @property
    def env_provider(self) -> EnvironmentCredentialsProvider:
        if self._env_provider is None:
            self._env_provider = EnvironmentCredentialsProvider()
        return self._env_provider

    @env_provider.setter
    def env_provider(self, provider: EnvironmentCredentialsProvider) -> None:
        self._env_provider = provider

    @property
    def airflow_provider(self) -> AirflowCredentialsProvider:
        if self._airflow_provider is None:
            self._airflow_provider = AirflowCredentialsProvider()
        return self._airflow_provider

    @airflow_provider.setter
    def airflow_provider(self, provider: AirflowCredentialsProvider) -> None:
        self._airflow_provider = provider

    @property
    def vault_provider(self) -> VaultCredentialsProvider:
        if self._vault_provider is None:
            self._vault_provider = VaultCredentialsProvider()
        return self._vault_provider

    @vault_provider.setter
    def vault_provider(self, provider: VaultCredentialsProvider) -> None:
        self._vault_provider = provider

    def get_credentials(
        self,
        connection_name: str,
        source: CredentialsSource = CredentialsSource.AIRFLOW,
        mount_point: str | None = None,
        path: str | None = None,
    ) -> CredentialsConfig:
        if source == CredentialsSource.AIRFLOW:
            return self.airflow_provider.get_credentials(connection_name)
        if source == CredentialsSource.ENVIRONMENT:
            return self.env_provider.get_credentials(connection_name)
        if source == CredentialsSource.VAULT:
            return self.vault_provider.get_credentials(connection_name, mount_point, path)
        if source == CredentialsSource.PARAMS:
            return self._from_params(connection_name)
        raise NotImplementedError(f"Источник креденшиалов {source} пока не поддерживается")

    def _from_params(self, connection_name: str) -> CredentialsConfig:
        try:
            payload = json.loads(connection_name)
        except (TypeError, json.JSONDecodeError):
            payload = {"bootstrap_servers": connection_name}
        return CredentialsConfig(
            host=payload.get("host"),
            port=int(payload.get("port", 0) or 0) or None,
            database=payload.get("database"),
            username=payload.get("username"),
            password=payload.get("password"),
            schema=payload.get("schema"),
            project_id=payload.get("project_id") or payload.get("project"),
            service_account_key_file=payload.get("service_account_key_file") or payload.get("keyfile_path"),
            service_account_info=payload.get("service_account") or payload.get("service_account_info"),
            endpoint=payload.get("endpoint") or payload.get("base_url"),
            token=payload.get("token") or payload.get("bearer_token"),
            api_key=payload.get("api_key"),
            additional_params=payload,
            secure=self._parse_bool(payload.get("secure"), default=False),
            compression=self._parse_bool(payload.get("compression"), default=True),
            connect_timeout=int(payload.get("connect_timeout", 10) or 10),
            send_receive_timeout=int(payload.get("send_receive_timeout", 300) or 300),
            settings=payload.get("settings") if isinstance(payload.get("settings"), dict) else None,
            driver=payload.get("driver"),
            encrypt=payload.get("encrypt"),
            trust_server_certificate=payload.get("trust_server_certificate"),
            query_timeout=int(payload.get("query_timeout", 0) or 0) or None,
            bcp_path=payload.get("bcp_path"),
            bootstrap_servers=payload.get("bootstrap_servers") or payload.get("bootstrap.servers"),
            security_protocol=payload.get("security_protocol") or payload.get("security.protocol"),
            sasl_mechanism=payload.get("sasl_mechanism") or payload.get("sasl.mechanism"),
            sasl_username=payload.get("sasl_username") or payload.get("sasl.username"),
            sasl_password=payload.get("sasl_password") or payload.get("sasl.password"),
            ssl_ca_location=payload.get("ssl_ca_location") or payload.get("ssl.ca.location"),
            client_id=payload.get("client_id"),
            schema_registry_url=payload.get("schema_registry_url"),
            schema_registry_username=payload.get("schema_registry_username"),
            schema_registry_password=payload.get("schema_registry_password"),
        )

    @staticmethod
    def _parse_bool(value, *, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}
