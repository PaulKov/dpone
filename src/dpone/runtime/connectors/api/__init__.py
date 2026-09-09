"""API connectors for runtime execution.

Symbols are exposed lazily so lightweight tooling/tests can import the package
without pulling optional runtime dependencies too early.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "AbstractAPIConnector",
    "APICredentials",
    "APIRateLimitConfig",
    "APIRetryConfig",
    "PaginationConfig",
    "OmnideskConnector",
    "OmnideskCredentials",
    "OmnideskPagination",
    "AppsflyerConnector",
    "AppsflyerCredentials",
    "AppsflyerResourceSpec",
    "list_appsflyer_resources",
    "get_appsflyer_resource",
    "appsflyer_table_name",
    "CbrConnector",
    "MindboxConnector",
    "MindboxCredentials",
    "MindboxResourceSpec",
    "list_mindbox_resources",
    "get_mindbox_resource",
    "mindbox_table_name",
    "SimilarwebConnector",
    "SimilarwebCredentials",
    "SimilarwebResourceSpec",
    "list_similarweb_resources",
    "get_similarweb_resource",
    "similarweb_table_name",
    "OpenExchangeRatesConnector",
    "OpenExchangeRatesCredentials",
    "OpenExchangeRatesResourceSpec",
    "list_openexchangerates_resources",
    "get_openexchangerates_resource",
    "openexchangerates_table_name",
    "GoogleSheetsConnector",
    "GoogleSheetsCredentials",
    "GoogleSheetsResourceSpec",
    "list_google_sheets_resources",
    "get_google_sheets_resource",
    "google_sheets_table_name",
    "GoogleAdsConnector",
    "GoogleAdsCredentials",
    "GoogleAdsResourceSpec",
    "list_google_ads_resources",
    "get_google_ads_resource",
    "google_ads_table_name",
    "YandexWebmasterConnector",
    "YandexWebmasterCredentials",
    "YandexWebmasterResourceSpec",
    "list_yandex_webmaster_resources",
    "get_yandex_webmaster_resource",
    "yandex_webmaster_table_name",
    "FasttrackConnector",
    "FasttrackCredentials",
    "FasttrackResourceSpec",
    "list_fasttrack_resources",
    "get_fasttrack_resource",
    "fasttrack_table_name",
]

_EXPORTS: dict[str, str] = {
    "AbstractAPIConnector": "dpone.runtime.connectors.api.base:AbstractAPIConnector",
    "APICredentials": "dpone.runtime.connectors.api.base:APICredentials",
    "APIRateLimitConfig": "dpone.runtime.connectors.api.base:APIRateLimitConfig",
    "APIRetryConfig": "dpone.runtime.connectors.api.base:APIRetryConfig",
    "PaginationConfig": "dpone.runtime.connectors.api.base:PaginationConfig",
    "OmnideskConnector": "dpone.runtime.connectors.api.omnidesk:OmnideskConnector",
    "OmnideskCredentials": "dpone.runtime.connectors.api.omnidesk:OmnideskCredentials",
    "OmnideskPagination": "dpone.runtime.connectors.api.omnidesk:OmnideskPagination",
    "AppsflyerConnector": "dpone.runtime.connectors.api.appsflyer:AppsflyerConnector",
    "AppsflyerCredentials": "dpone.runtime.connectors.api.appsflyer:AppsflyerCredentials",
    "AppsflyerResourceSpec": "dpone.runtime.connectors.api.appsflyer_resources:AppsflyerResourceSpec",
    "list_appsflyer_resources": "dpone.runtime.connectors.api.appsflyer_resources:list_appsflyer_resources",
    "get_appsflyer_resource": "dpone.runtime.connectors.api.appsflyer_resources:get_appsflyer_resource",
    "appsflyer_table_name": "dpone.runtime.connectors.api.appsflyer_resources:appsflyer_table_name",
    "CbrConnector": "dpone.runtime.connectors.api.cbr:CbrConnector",
    "MindboxConnector": "dpone.runtime.connectors.api.mindbox:MindboxConnector",
    "MindboxCredentials": "dpone.runtime.connectors.api.mindbox:MindboxCredentials",
    "MindboxResourceSpec": "dpone.runtime.connectors.api.mindbox_resources:MindboxResourceSpec",
    "list_mindbox_resources": "dpone.runtime.connectors.api.mindbox_resources:list_mindbox_resources",
    "get_mindbox_resource": "dpone.runtime.connectors.api.mindbox_resources:get_mindbox_resource",
    "mindbox_table_name": "dpone.runtime.connectors.api.mindbox_resources:mindbox_table_name",
    "SimilarwebConnector": "dpone.runtime.connectors.api.similarweb:SimilarwebConnector",
    "SimilarwebCredentials": "dpone.runtime.connectors.api.similarweb:SimilarwebCredentials",
    "SimilarwebResourceSpec": "dpone.runtime.connectors.api.similarweb_resources:SimilarwebResourceSpec",
    "list_similarweb_resources": "dpone.runtime.connectors.api.similarweb_resources:list_similarweb_resources",
    "get_similarweb_resource": "dpone.runtime.connectors.api.similarweb_resources:get_similarweb_resource",
    "similarweb_table_name": "dpone.runtime.connectors.api.similarweb_resources:similarweb_table_name",
    "OpenExchangeRatesConnector": "dpone.runtime.connectors.api.openexchangerates:OpenExchangeRatesConnector",
    "OpenExchangeRatesCredentials": "dpone.runtime.connectors.api.openexchangerates:OpenExchangeRatesCredentials",
    "OpenExchangeRatesResourceSpec": (
        "dpone.runtime.connectors.api.openexchangerates_resources:OpenExchangeRatesResourceSpec"
    ),
    "list_openexchangerates_resources": (
        "dpone.runtime.connectors.api.openexchangerates_resources:list_openexchangerates_resources"
    ),
    "get_openexchangerates_resource": (
        "dpone.runtime.connectors.api.openexchangerates_resources:get_openexchangerates_resource"
    ),
    "openexchangerates_table_name": (
        "dpone.runtime.connectors.api.openexchangerates_resources:openexchangerates_table_name"
    ),
    "GoogleSheetsConnector": "dpone.runtime.connectors.api.google_sheets:GoogleSheetsConnector",
    "GoogleSheetsCredentials": "dpone.runtime.connectors.api.google_sheets:GoogleSheetsCredentials",
    "GoogleSheetsResourceSpec": "dpone.runtime.connectors.api.google_sheets_resources:GoogleSheetsResourceSpec",
    "list_google_sheets_resources": "dpone.runtime.connectors.api.google_sheets_resources:list_google_sheets_resources",
    "get_google_sheets_resource": "dpone.runtime.connectors.api.google_sheets_resources:get_google_sheets_resource",
    "google_sheets_table_name": "dpone.runtime.connectors.api.google_sheets_resources:google_sheets_table_name",
    "GoogleAdsConnector": "dpone.runtime.connectors.api.google_ads:GoogleAdsConnector",
    "GoogleAdsCredentials": "dpone.runtime.connectors.api.google_ads:GoogleAdsCredentials",
    "GoogleAdsResourceSpec": "dpone.runtime.connectors.api.google_ads_resources:GoogleAdsResourceSpec",
    "list_google_ads_resources": "dpone.runtime.connectors.api.google_ads_resources:list_google_ads_resources",
    "get_google_ads_resource": "dpone.runtime.connectors.api.google_ads_resources:get_google_ads_resource",
    "google_ads_table_name": "dpone.runtime.connectors.api.google_ads_resources:google_ads_table_name",
    "YandexWebmasterConnector": "dpone.runtime.connectors.api.yandex_webmaster:YandexWebmasterConnector",
    "YandexWebmasterCredentials": "dpone.runtime.connectors.api.yandex_webmaster:YandexWebmasterCredentials",
    "YandexWebmasterResourceSpec": "dpone.runtime.connectors.api.yandex_webmaster_resources:YandexWebmasterResourceSpec",
    "list_yandex_webmaster_resources": "dpone.runtime.connectors.api.yandex_webmaster_resources:list_yandex_webmaster_resources",
    "get_yandex_webmaster_resource": "dpone.runtime.connectors.api.yandex_webmaster_resources:get_yandex_webmaster_resource",
    "yandex_webmaster_table_name": "dpone.runtime.connectors.api.yandex_webmaster_resources:yandex_webmaster_table_name",
    "FasttrackConnector": "dpone.runtime.connectors.api.fasttrack:FasttrackConnector",
    "FasttrackCredentials": "dpone.runtime.connectors.api.fasttrack:FasttrackCredentials",
    "FasttrackResourceSpec": "dpone.runtime.connectors.api.fasttrack_resources:FasttrackResourceSpec",
    "list_fasttrack_resources": "dpone.runtime.connectors.api.fasttrack_resources:list_fasttrack_resources",
    "get_fasttrack_resource": "dpone.runtime.connectors.api.fasttrack_resources:get_fasttrack_resource",
    "fasttrack_table_name": "dpone.runtime.connectors.api.fasttrack_resources:fasttrack_table_name",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    mod = import_module(module_name)
    value = getattr(mod, attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(__all__)))
