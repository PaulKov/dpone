"""Public facade for the AppsFlyer API connector."""

from __future__ import annotations

from typing import Any

import dpone.runtime.connectors.api.appsflyer_connector as _connector
from dpone.runtime.connectors.api.appsflyer_connector import (
    _CANONICAL_EXPORT_PATH,
    _DATE_ONLY_RE,
    _NORMALIZE_RE,
    _QUOTA_EXCEEDED_RE,
    AppsflyerQuotaExceededError,
    _normalize_appsflyer_endpoint,
    get_default_manager,
    logger,
)
from dpone.runtime.connectors.api.appsflyer_connector import (
    AppsflyerConnector as _BaseAppsflyerConnector,
)
from dpone.runtime.connectors.api.appsflyer_connector import (
    AppsflyerCredentials as _BaseAppsflyerCredentials,
)

get_env_code = _connector.get_env_code


class AppsflyerCredentials(_BaseAppsflyerCredentials):
    """Backward-compatible credentials facade.

    Legacy tests and integrations monkeypatch ``dpone.runtime.connectors.api.appsflyer.get_env_code``.
    The connector implementation lives in ``appsflyer_connector``. This facade keeps that injection point
    working while delegating the actual parsing logic to the focused connector class.
    """

    @classmethod
    def from_vault(cls, vault_path: str, vault_manager: Any | None = None) -> AppsflyerCredentials:
        original_get_env_code = _connector.get_env_code
        original_get_default_manager = _connector.get_default_manager
        _connector.get_env_code = get_env_code
        _connector.get_default_manager = get_default_manager
        try:
            return super().from_vault(vault_path=vault_path, vault_manager=vault_manager)
        finally:
            _connector.get_env_code = original_get_env_code
            _connector.get_default_manager = original_get_default_manager


class AppsflyerConnector(_BaseAppsflyerConnector):
    """Backward-compatible connector facade using facade-aware credentials."""

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: Any | None = None,
        *,
        rate_limit_delay: float | None = None,
        max_retries: int | None = None,
        default_app_id: str | None = None,
        timeout: int = _BaseAppsflyerConnector.DEFAULT_TIMEOUT,
    ) -> AppsflyerConnector:
        original_credentials = _connector.AppsflyerCredentials
        _connector.AppsflyerCredentials = AppsflyerCredentials
        try:
            return super().from_vault(
                vault_path=vault_path,
                vault_manager=vault_manager,
                rate_limit_delay=rate_limit_delay,
                max_retries=max_retries,
                default_app_id=default_app_id,
                timeout=timeout,
            )
        finally:
            _connector.AppsflyerCredentials = original_credentials


__all__ = [
    "AppsflyerQuotaExceededError",
    "get_default_manager",
    "get_env_code",
    "AppsflyerCredentials",
    "_CANONICAL_EXPORT_PATH",
    "_DATE_ONLY_RE",
    "_NORMALIZE_RE",
    "_QUOTA_EXCEEDED_RE",
    "_normalize_appsflyer_endpoint",
    "AppsflyerConnector",
    "logger",
]
