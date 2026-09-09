from __future__ import annotations

import json
import logging
import tempfile
import time
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vault_kv_client import VaultManager
else:
    VaultManager = Any


from dpone.config.env import get_env_code
from dpone.runtime.connectors.api.base import (
    AbstractAPIConnector,
    APICredentials,
    APIRateLimitConfig,
    APIRetryConfig,
)
from dpone.runtime.connectors.api.google_ads_resources import (
    GoogleAdsResourceSpec,
    list_google_ads_resources,
)

logger = logging.getLogger(__name__)


def get_default_manager() -> VaultManager:
    try:
        from vault_kv_client import get_default_manager as load_default_manager
    except ImportError as exc:  # pragma: no cover - depends on optional runtime extra
        raise RuntimeError("vault-kv-client is not installed") from exc
    return load_default_manager()


def _load_google_ads_sdk() -> tuple[Any, type[Exception]]:
    try:
        from google.ads.googleads.client import GoogleAdsClient
        from google.ads.googleads.errors import GoogleAdsException
    except ImportError as exc:  # pragma: no cover - depends on optional runtime extra
        raise RuntimeError("google-ads is not installed. Install dpone with the 'google_ads' extra.") from exc
    return GoogleAdsClient, GoogleAdsException


@dataclass
class GoogleAdsCredentials(APICredentials):
    endpoint: str = "https://googleads.googleapis.com"
    auth_type: str = field(default="oauth_refresh_token")

    developer_token: str | None = None
    login_customer_id: str | None = None
    customer_ids: list[str] = field(default_factory=list)

    client_id: str | None = None
    client_secret: str | None = None
    refresh_token: str | None = None

    json_key: str | None = None

    api_version: str = "v19"
    use_proto_plus: bool = False

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
        mount_point: str | None = None,
    ) -> GoogleAdsCredentials:
        vm = vault_manager or get_default_manager()
        secret = vm.get_secret(mount_point=mount_point or get_env_code(), path=vault_path)
        return cls.from_dict(secret)

    @classmethod
    def from_dict(cls, config: Mapping[str, Any]) -> GoogleAdsCredentials:
        auth_type = str(config.get("auth_type") or "oauth_refresh_token").strip()

        customer_ids_raw = config.get("customer_ids") or config.get("customer_id") or []
        if isinstance(customer_ids_raw, list | tuple):
            customer_ids = [str(item).strip().replace("-", "") for item in customer_ids_raw if str(item).strip()]
        else:
            customer_ids = [
                item.strip().replace("-", "") for item in str(customer_ids_raw).split(",") if item and item.strip()
            ]

        login_customer_id = config.get("login_customer_id")
        if login_customer_id is not None:
            login_customer_id = str(login_customer_id).strip().replace("-", "")

        endpoint = str(config.get("endpoint") or "https://googleads.googleapis.com")
        api_version = str(config.get("api_version") or "v19")
        use_proto_plus = bool(config.get("use_proto_plus", False))

        if auth_type == "service_account":
            return cls(
                endpoint=endpoint,
                auth_type="service_account",
                developer_token=_optional_str(config.get("developer_token")),
                login_customer_id=login_customer_id,
                customer_ids=customer_ids,
                json_key=cls._extract_json_key(config),
                api_version=api_version,
                use_proto_plus=use_proto_plus,
            )

        return cls(
            endpoint=endpoint,
            auth_type="oauth_refresh_token",
            developer_token=_optional_str(config.get("developer_token")),
            login_customer_id=login_customer_id,
            customer_ids=customer_ids,
            client_id=_optional_str(config.get("client_id")),
            client_secret=_optional_str(config.get("client_secret")),
            refresh_token=_optional_str(config.get("refresh_token")),
            api_version=api_version,
            use_proto_plus=use_proto_plus,
        )

    @staticmethod
    def _extract_json_key(config: Mapping[str, Any]) -> str | None:
        for key in ("json_key", "service_account", "service_account_json", "credentials_json", "keyfile_json"):
            value = config.get(key)
            if value is None:
                continue
            if isinstance(value, str):
                stripped = value.strip()
                if stripped:
                    return stripped
            if isinstance(value, dict):
                return json.dumps(value)
        return None

    def validate(self) -> None:
        if not self.developer_token:
            raise ValueError("GoogleAdsCredentials.developer_token is required")

        if not self.customer_ids:
            raise ValueError("GoogleAdsCredentials.customer_ids is empty")

        if self.auth_type == "oauth_refresh_token":
            missing = [
                key
                for key, value in {
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": self.refresh_token,
                }.items()
                if not value
            ]
            if missing:
                raise ValueError(f"Missing OAuth Google Ads credentials fields: {', '.join(missing)}")
            return

        if self.auth_type == "service_account":
            if not self.json_key:
                raise ValueError("GoogleAdsCredentials.json_key is required for service_account")
            return

        raise ValueError(
            f"Unsupported Google Ads auth_type={self.auth_type!r}. Expected 'oauth_refresh_token' or 'service_account'"
        )

    def to_google_ads_config(self) -> dict[str, Any]:
        config: dict[str, Any] = {
            "developer_token": self.developer_token,
            "use_proto_plus": self.use_proto_plus,
        }
        if self.login_customer_id:
            config["login_customer_id"] = self.login_customer_id
        if self.auth_type == "oauth_refresh_token":
            config.update(
                {
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": self.refresh_token,
                }
            )
        return config


class GoogleAdsConnector(AbstractAPIConnector):
    """Low-level Google Ads connector.

    Business-specific GAQL templates live in source strategies/specs. The connector
    is responsible only for credentials, health checks and raw query execution.
    """

    DEFAULT_TIMEOUT = 60
    DEFAULT_RATE_LIMIT = APIRateLimitConfig(requests_per_second=1.0, burst_limit=5)

    def __init__(
        self,
        credentials: GoogleAdsCredentials,
        retry_config: APIRetryConfig | None = None,
        rate_limit_config: APIRateLimitConfig | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        credentials.validate()
        super().__init__(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config or self.DEFAULT_RATE_LIMIT,
            timeout=timeout,
        )

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
        *,
        rate_limit_delay: float | None = None,
        max_retries: int | None = None,
        timeout: int = DEFAULT_TIMEOUT,
        **_: Any,
    ) -> GoogleAdsConnector:
        credentials = GoogleAdsCredentials.from_vault(vault_path=vault_path, vault_manager=vault_manager)
        retry_config = APIRetryConfig(max_retries=max_retries) if max_retries is not None else None
        rate_limit_config = None
        if rate_limit_delay is not None:
            rate_limit_config = APIRateLimitConfig(
                requests_per_second=1.0 / rate_limit_delay if rate_limit_delay > 0 else 1.0,
                burst_limit=5,
            )
        return cls(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config,
            timeout=timeout,
        )

    def get_resource_specs(self) -> list[GoogleAdsResourceSpec]:
        return list_google_ads_resources()

    def get_resources(self) -> list[str]:
        return [spec.name for spec in self.get_resource_specs()]

    def health_check(self) -> bool:
        try:
            client = self._build_client()
            customer_service = client.get_service("CustomerService")
            response = customer_service.list_accessible_customers()
            accessible = list(response.resource_names)
            logger.info(
                "google_ads health_check ok: accessible_customers_count=%s configured_customer_ids=%s",
                len(accessible),
                len(self.credentials.customer_ids),
            )
            return True
        except Exception as exc:
            logger.exception("google_ads health_check failed: %s", exc)
            return False

    def execute_query(self, *, customer_id: str, query: str) -> list[Any]:
        """Runs a GAQL query and returns raw Google Ads rows."""
        _, google_ads_exception = _load_google_ads_sdk()

        started = time.monotonic()
        normalized_customer_id = str(customer_id).replace("-", "").strip()
        if not normalized_customer_id:
            raise ValueError("customer_id is empty")

        client = self._build_client()
        service = client.get_service("GoogleAdsService")
        request = client.get_type("SearchGoogleAdsStreamRequest")
        request.customer_id = normalized_customer_id
        request.query = query

        rows: list[Any] = []
        try:
            stream = service.search_stream(request=request)
            for batch in stream:
                for row in batch.results:
                    rows.append(row)
            logger.info(
                "google_ads execute_query done: customer_id=%s rows=%s elapsed_sec=%.3f",
                normalized_customer_id,
                len(rows),
                time.monotonic() - started,
            )
            return rows
        except google_ads_exception as exc:
            logger.exception("google_ads execute_query failed: customer_id=%s error=%s", normalized_customer_id, exc)
            raise

    def _build_client(self) -> Any:
        google_ads_client, _ = _load_google_ads_sdk()
        config = self.credentials.to_google_ads_config()
        if self.credentials.auth_type == "service_account":
            with self._temporary_json_key_file(self.credentials.json_key or "") as json_key_path:
                config["json_key_file_path"] = json_key_path
                return google_ads_client.load_from_dict(config, version=self.credentials.api_version)
        return google_ads_client.load_from_dict(config, version=self.credentials.api_version)

    @contextmanager
    def _temporary_json_key_file(self, json_key: str):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=True) as temp_file:
            temp_file.write(json_key)
            temp_file.flush()
            yield temp_file.name


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


__all__ = [
    "GoogleAdsConnector",
    "GoogleAdsCredentials",
]
