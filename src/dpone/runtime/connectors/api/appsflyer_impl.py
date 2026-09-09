"""Deprecated compatibility shim for AppsFlyer connector imports.

Use ``dpone.runtime.connectors.api.appsflyer`` for public imports or
``dpone.runtime.connectors.api.appsflyer_connector`` for focused connector imports.
"""

from __future__ import annotations

from dpone.runtime.connectors.api.appsflyer_connector import (
    _CANONICAL_EXPORT_PATH,
    _DATE_ONLY_RE,
    _NORMALIZE_RE,
    _QUOTA_EXCEEDED_RE,
    AppsflyerConnector,
    AppsflyerCredentials,
    AppsflyerQuotaExceededError,
    _normalize_appsflyer_endpoint,
    get_default_manager,
    get_env_code,
    logger,
)

__all__ = [
    "AppsflyerCredentials",
    "AppsflyerConnector",
    "AppsflyerQuotaExceededError",
    "get_default_manager",
    "get_env_code",
    "_CANONICAL_EXPORT_PATH",
    "_DATE_ONLY_RE",
    "_NORMALIZE_RE",
    "_QUOTA_EXCEEDED_RE",
    "_normalize_appsflyer_endpoint",
    "logger",
]
