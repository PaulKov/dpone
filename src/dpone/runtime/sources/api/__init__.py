"""API источники данных для dpone ETL pipeline."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "AbstractAPISource",
    "OmnideskSource",
    "AppsflyerSource",
    "CbrSource",
    "MindboxSource",
    "SimilarwebSource",
    "OpenExchangeRatesSource",
    "GoogleSheetsSource",
    "GoogleAdsSource",
    "YandexWebmasterSource",
    "FasttrackSource",
]

_EXPORTS: dict[str, str] = {
    "AbstractAPISource": "dpone.runtime.sources.api.base:AbstractAPISource",
    "OmnideskSource": "dpone.runtime.sources.api.omnidesk:OmnideskSource",
    "AppsflyerSource": "dpone.runtime.sources.api.appsflyer:AppsflyerSource",
    "CbrSource": "dpone.runtime.sources.api.cbr:CbrSource",
    "MindboxSource": "dpone.runtime.sources.api.mindbox:MindboxSource",
    "SimilarwebSource": "dpone.runtime.sources.api.similarweb:SimilarwebSource",
    "OpenExchangeRatesSource": "dpone.runtime.sources.api.openexchangerates:OpenExchangeRatesSource",
    "GoogleSheetsSource": "dpone.runtime.sources.api.google_sheets:GoogleSheetsSource",
    "GoogleAdsSource": "dpone.runtime.sources.api.google_ads:GoogleAdsSource",
    "YandexWebmasterSource": "dpone.runtime.sources.api.yandex_webmaster:YandexWebmasterSource",
    "FasttrackSource": "dpone.runtime.sources.api.fasttrack:FasttrackSource",
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
