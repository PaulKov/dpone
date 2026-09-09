"""Declarative runtime specs for API-backed providers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.api_sources import get_api_source_defaults
from dpone.runtime.api_providers.models import APIProviderRuntimeSpec


def _first_app_id(options: Mapping[str, Any]) -> str | None:
    app_ids = options.get("app_ids") or []
    if isinstance(app_ids, str):
        app_ids = [item.strip() for item in app_ids.split(",") if item.strip()]
    if isinstance(app_ids, list | tuple) and app_ids:
        return str(app_ids[0])
    return None


def _omnidesk_kwargs(options: Mapping[str, Any]) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "rate_limit_delay": options.get("rate_limit_delay", 0.5),
        "max_retries": options.get("max_retries", 3),
        "timeout": int(options.get("timeout", 30)),
    }
    parallel_workers = options.get("parallel_workers")
    if parallel_workers is not None:
        kwargs["parallel_workers"] = int(parallel_workers)
    return kwargs


def _appsflyer_kwargs(options: Mapping[str, Any]) -> dict[str, Any]:
    from dpone.runtime.connectors.api.appsflyer import AppsflyerConnector

    return {
        "rate_limit_delay": options.get("rate_limit_delay", 1.0),
        "max_retries": options.get("max_retries", 3),
        "default_app_id": _first_app_id(options),
        "timeout": int(options.get("timeout", AppsflyerConnector.DEFAULT_TIMEOUT)),
    }


def _mindbox_kwargs(options: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "rate_limit_delay": options.get("rate_limit_delay", 0.5),
        "max_retries": options.get("max_retries", 3),
        "timeout": int(options.get("timeout", 300)),
    }


def _similarweb_kwargs(options: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "rate_limit_delay": options.get("rate_limit_delay", 1.0),
        "max_retries": options.get("max_retries", 3),
        "timeout": int(options.get("timeout", 60)),
    }


def _openexchangerates_kwargs(options: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "rate_limit_delay": options.get("rate_limit_delay", 1.0),
        "max_retries": options.get("max_retries", 3),
        "retry_delay": float(options.get("retry_delay", 1.0)),
        "timeout": int(options.get("timeout", 60)),
    }


def _google_sheets_kwargs(options: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "rate_limit_delay": options.get("rate_limit_delay", 0.2),
        "max_retries": options.get("max_retries", 3),
        "timeout": int(options.get("timeout", 60)),
    }


def _google_ads_kwargs(options: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "rate_limit_delay": options.get("rate_limit_delay", 1.0),
        "max_retries": options.get("max_retries", 3),
        "timeout": int(options.get("timeout", 60)),
    }


def _yandex_webmaster_kwargs(options: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "rate_limit_delay": options.get("rate_limit_delay", 0.5),
        "max_retries": options.get("max_retries", 3),
        "timeout": int(options.get("timeout", 60)),
    }


def _cbr_kwargs(options: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "timeout": int(options.get("timeout", 60)),
        "retries": int(options.get("max_retries", options.get("retries", 3))),
        "retry_delay": float(options.get("retry_delay", 2.0)),
    }


def _rest_kwargs(options: Mapping[str, Any]) -> dict[str, Any]:
    return {"timeout": int(options.get("timeout", 60))}


def _fasttrack_kwargs(options: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "timeout": int(options.get("timeout", 60)),
        "max_retries": int(options.get("max_retries", 3)),
        "rate_limit_delay": float(options.get("rate_limit_delay", 0.2)),
    }


_RUNTIME_SPECS: dict[str, APIProviderRuntimeSpec] = {
    "omnidesk": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("omnidesk"),
        connector_target="dpone.runtime.connectors.api.omnidesk:OmnideskConnector",
        credentials_target="dpone.runtime.connectors.api.omnidesk_support:OmnideskCredentials",
        source_target="dpone.runtime.sources.api.omnidesk:OmnideskSource",
        connector_kwargs_factory=_omnidesk_kwargs,
        resolved_connector_mode="concurrent",
    ),
    "appsflyer": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("appsflyer"),
        connector_target="dpone.runtime.connectors.api.appsflyer:AppsflyerConnector",
        credentials_target="dpone.runtime.connectors.api.appsflyer_connector:AppsflyerCredentials",
        source_target="dpone.runtime.sources.api.appsflyer:AppsflyerSource",
        connector_kwargs_factory=_appsflyer_kwargs,
        resolved_connector_mode="appsflyer",
    ),
    "mindbox": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("mindbox"),
        connector_target="dpone.runtime.connectors.api.mindbox:MindboxConnector",
        credentials_target="dpone.runtime.connectors.api.mindbox:MindboxCredentials",
        source_target="dpone.runtime.sources.api.mindbox:MindboxSource",
        connector_kwargs_factory=_mindbox_kwargs,
        resolved_connector_mode="concurrent",
    ),
    "similarweb": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("similarweb"),
        connector_target="dpone.runtime.connectors.api.similarweb:SimilarwebConnector",
        credentials_target="dpone.runtime.connectors.api.similarweb:SimilarwebCredentials",
        source_target="dpone.runtime.sources.api.similarweb:SimilarwebSource",
        connector_kwargs_factory=_similarweb_kwargs,
        resolved_connector_mode="concurrent",
    ),
    "openexchangerates": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("openexchangerates"),
        connector_target="dpone.runtime.connectors.api.openexchangerates:OpenExchangeRatesConnector",
        credentials_target="dpone.runtime.connectors.api.openexchangerates:OpenExchangeRatesCredentials",
        source_target="dpone.runtime.sources.api.openexchangerates:OpenExchangeRatesSource",
        connector_kwargs_factory=_openexchangerates_kwargs,
    ),
    "google_sheets": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("google_sheets"),
        connector_target="dpone.runtime.connectors.api.google_sheets:GoogleSheetsConnector",
        credentials_target="dpone.runtime.connectors.api.google_sheets_credentials:GoogleSheetsCredentials",
        source_target="dpone.runtime.sources.api.google_sheets:GoogleSheetsSource",
        connector_kwargs_factory=_google_sheets_kwargs,
    ),
    "google_ads": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("google_ads"),
        connector_target="dpone.runtime.connectors.api.google_ads:GoogleAdsConnector",
        credentials_target="dpone.runtime.connectors.api.google_ads:GoogleAdsCredentials",
        source_target="dpone.runtime.sources.api.google_ads:GoogleAdsSource",
        connector_kwargs_factory=_google_ads_kwargs,
    ),
    "yandex_webmaster": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("yandex_webmaster"),
        connector_target="dpone.runtime.connectors.api.yandex_webmaster:YandexWebmasterConnector",
        credentials_target="dpone.runtime.connectors.api.yandex_webmaster:YandexWebmasterCredentials",
        source_target="dpone.runtime.sources.api.yandex_webmaster:YandexWebmasterSource",
        connector_kwargs_factory=_yandex_webmaster_kwargs,
    ),
    "cbr": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("cbr"),
        connector_target="dpone.runtime.connectors.api.cbr:CbrConnector",
        source_target="dpone.runtime.sources.api.cbr:CbrSource",
        connector_kwargs_factory=_cbr_kwargs,
    ),
    "rest": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("rest"),
        connector_target="dpone.runtime.connectors.api.rest:GenericRestConnector",
        credentials_target="dpone.runtime.connectors.api.rest:RestCredentials",
        source_target="dpone.runtime.sources.api.rest:GenericRestSource",
        connector_kwargs_factory=_rest_kwargs,
        resolved_connector_mode="direct",
    ),
    "amplitude": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("amplitude"),
        connector_target=None,
        source_target=None,
        connector_kwargs_factory=lambda options: {},
    ),
    "fasttrack": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("fasttrack"),
        connector_target="dpone.runtime.connectors.api.fasttrack:FasttrackConnector",
        credentials_target="dpone.runtime.connectors.api.fasttrack:FasttrackCredentials",
        source_target="dpone.runtime.sources.api.fasttrack:FasttrackSource",
        connector_kwargs_factory=_fasttrack_kwargs,
        resolved_connector_mode="concurrent",
    ),
    "fastrack": APIProviderRuntimeSpec(
        defaults=get_api_source_defaults("fastrack"),
        connector_target="dpone.runtime.connectors.api.fasttrack:FasttrackConnector",
        credentials_target="dpone.runtime.connectors.api.fasttrack:FasttrackCredentials",
        source_target="dpone.runtime.sources.api.fasttrack:FasttrackSource",
        connector_kwargs_factory=_fasttrack_kwargs,
        resolved_connector_mode="concurrent",
    ),
}


def load_runtime_specs() -> dict[str, APIProviderRuntimeSpec]:
    return dict(_RUNTIME_SPECS)


__all__ = ["load_runtime_specs"]
