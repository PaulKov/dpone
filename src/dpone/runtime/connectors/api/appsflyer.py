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
    """Credentials facade passing its resolver dependencies explicitly to the loader."""

    @classmethod
    def from_vault(cls, vault_path: str, vault_manager: Any | None = None) -> AppsflyerCredentials:
        return cls._from_vault_dependencies(
            vault_path, vault_manager, env_resolver=get_env_code, manager_factory=get_default_manager
        )


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
        return super().from_vault(
            vault_path=vault_path,
            vault_manager=vault_manager,
            rate_limit_delay=rate_limit_delay,
            max_retries=max_retries,
            default_app_id=default_app_id,
            timeout=timeout,
        )

    @classmethod
    def _credentials_from_vault(cls, vault_path: str, vault_manager: Any | None) -> AppsflyerCredentials:
        """Select facade credentials through the canonical construction hook."""
        return AppsflyerCredentials.from_vault(vault_path=vault_path, vault_manager=vault_manager)


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
