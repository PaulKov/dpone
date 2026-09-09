from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vault_kv_client import VaultManager
else:
    VaultManager = Any


from dpone.config.env import get_env_code
from dpone.runtime.connectors.api.base import APICredentials


def get_default_manager() -> VaultManager:
    try:
        from vault_kv_client import get_default_manager as load_default_manager
    except ImportError as exc:  # pragma: no cover - depends on optional runtime extra
        raise RuntimeError("vault-kv-client is not installed") from exc
    return load_default_manager()


def _load_google_auth_modules() -> tuple[Any, Any, Any]:
    try:
        from google.auth.transport.requests import Request
        from google.oauth2 import service_account
        from google.oauth2.credentials import Credentials as UserCredentials
    except ImportError as exc:  # pragma: no cover - depends on optional runtime extra
        raise RuntimeError(
            "google-auth is not installed. Install dpone with the 'gcp' extra or add google-auth."
        ) from exc
    return Request, service_account, UserCredentials


@dataclass
class GoogleSheetsCredentials(APICredentials):
    endpoint: str = "https://sheets.googleapis.com"
    auth_type: str = field(default="service_account")
    scopes: list[str] = field(
        default_factory=lambda: [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
        ]
    )

    client_id: str | None = None
    client_secret: str | None = None
    refresh_token: str | None = None
    token_uri: str | None = None

    project_id: str | None = None
    private_key_id: str | None = None
    private_key: str | None = None
    client_email: str | None = None
    credentials_json: dict[str, Any] | None = None

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
        mount_point: str | None = None,
    ) -> GoogleSheetsCredentials:
        vm = vault_manager or get_default_manager()
        secret = vm.get_secret(mount_point=mount_point or get_env_code(), path=vault_path)
        return cls.from_dict(secret)

    @classmethod
    def from_dict(cls, config: Mapping[str, Any]) -> GoogleSheetsCredentials:
        auth_type = str(config.get("auth_type") or "service_account").strip()
        endpoint = str(config.get("endpoint") or "https://sheets.googleapis.com")
        scopes = cls._parse_scopes(config.get("scopes"))

        if auth_type == "published_csv":
            return cls(endpoint=endpoint, auth_type="published_csv", scopes=scopes)

        if auth_type == "oauth_refresh_token":
            return cls(
                endpoint=endpoint,
                auth_type="oauth_refresh_token",
                scopes=scopes,
                client_id=_optional_str(config.get("client_id")),
                client_secret=_optional_str(config.get("client_secret")),
                refresh_token=_optional_str(config.get("refresh_token")),
                token_uri=_optional_str(config.get("token_uri")) or "https://oauth2.googleapis.com/token",
            )

        service_account_info = cls._extract_service_account_info(config)
        private_key = service_account_info.get("private_key")
        if isinstance(private_key, str):
            private_key = private_key.replace("\\n", "\n")

        return cls(
            endpoint=endpoint,
            auth_type="service_account",
            scopes=scopes,
            token_uri=_optional_str(service_account_info.get("token_uri")) or "https://oauth2.googleapis.com/token",
            project_id=_optional_str(service_account_info.get("project_id")),
            private_key_id=_optional_str(service_account_info.get("private_key_id")),
            private_key=private_key,
            client_email=_optional_str(service_account_info.get("client_email")),
            credentials_json=service_account_info,
        )

    @staticmethod
    def _parse_scopes(raw_scopes: Any) -> list[str]:
        default_scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        if raw_scopes is None:
            return default_scopes
        if isinstance(raw_scopes, str):
            scopes = [scope.strip() for scope in raw_scopes.split(",") if scope.strip()]
            return scopes or default_scopes
        if isinstance(raw_scopes, list | tuple):
            scopes = [str(scope).strip() for scope in raw_scopes if str(scope).strip()]
            return scopes or default_scopes
        return default_scopes

    @classmethod
    def _extract_service_account_info(cls, config: Mapping[str, Any]) -> dict[str, Any]:
        for key in (
            "service_account",
            "service_account_info",
            "service_account_json",
            "credentials_json",
            "keyfile_json",
        ):
            info = cls._ensure_dict(config.get(key))
            if info:
                return cls._normalize_service_account_info(info)

        top_level_info = cls._build_service_account_from_top_level(config)
        if top_level_info:
            return cls._normalize_service_account_info(top_level_info)

        raise ValueError(
            "Не удалось собрать service account для Google Sheets. "
            "Ожидался один из форматов: service_account, service_account_info, "
            "service_account_json, credentials_json, keyfile_json или top-level поля service account."
        )

    @staticmethod
    def _ensure_dict(value: Any) -> dict[str, Any] | None:
        if value is None:
            return None
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                return None
            return parsed if isinstance(parsed, dict) else None
        return None

    @staticmethod
    def _build_service_account_from_top_level(config: Mapping[str, Any]) -> dict[str, Any] | None:
        project_id = _optional_str(config.get("project_id"))
        private_key = _optional_str(config.get("private_key"))
        client_email = _optional_str(config.get("client_email"))
        if isinstance(private_key, str):
            private_key = private_key.replace("\\n", "\n")
        if not (project_id and private_key and client_email):
            return None
        return {
            "type": _optional_str(config.get("type")) or "service_account",
            "project_id": project_id,
            "private_key_id": _optional_str(config.get("private_key_id")),
            "private_key": private_key,
            "client_email": client_email,
            "client_id": _optional_str(config.get("client_id")),
            "auth_uri": _optional_str(config.get("auth_uri")) or "https://accounts.google.com/o/oauth2/auth",
            "token_uri": _optional_str(config.get("token_uri")) or "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": _optional_str(config.get("auth_provider_x509_cert_url"))
            or "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": _optional_str(config.get("client_x509_cert_url")),
            "universe_domain": _optional_str(config.get("universe_domain")) or "googleapis.com",
        }

    @staticmethod
    def _normalize_service_account_info(info: Mapping[str, Any]) -> dict[str, Any]:
        normalized = dict(info)
        private_key = normalized.get("private_key")
        if isinstance(private_key, str):
            normalized["private_key"] = private_key.replace("\\n", "\n")
        normalized.setdefault("type", "service_account")
        normalized.setdefault("token_uri", "https://oauth2.googleapis.com/token")
        normalized.setdefault("auth_uri", "https://accounts.google.com/o/oauth2/auth")
        normalized.setdefault("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs")
        normalized.setdefault("universe_domain", "googleapis.com")
        return normalized


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)
