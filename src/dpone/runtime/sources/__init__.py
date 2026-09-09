"""Runtime sources.

IMPORTANT
---------
Some sources require optional dependencies (e.g. ClickHouse driver).
To keep lightweight tooling usable in minimal environments, this package exports
symbols **lazily**.

Prefer explicit submodules for strict imports:
- :mod:`dpone.runtime.sources.postgres`
- :mod:`dpone.runtime.sources.clickhouse`
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    # Base
    "AbstractSource",
    "ExtractResult",
    # Database sources
    "PostgresSource",
    "ClickHouseSource",
    "MSSQLSource",
    "KafkaSource",
    "GoogleSheetsSource",
    "GoogleAdsSource",
    "YandexWebmasterSource",
    # Strategies
    "SourceStrategy",
    "PostgresFullExtractStrategy",
    "PostgresXMinExtractStrategy",
]

_EXPORTS: dict[str, str] = {
    # Base
    "AbstractSource": "dpone.runtime.sources.source_protocol:AbstractSource",
    "ExtractResult": "dpone.runtime.sources.extract_result:ExtractResult",
    # Sources
    "PostgresSource": "dpone.runtime.sources.postgres:PostgresSource",
    "ClickHouseSource": "dpone.runtime.sources.clickhouse:ClickHouseSource",
    "MSSQLSource": "dpone.runtime.sources.mssql:MSSQLSource",
    "KafkaSource": "dpone.runtime.sources.kafka:KafkaSource",
    "GoogleSheetsSource": "dpone.runtime.sources.api.google_sheets:GoogleSheetsSource",
    "GoogleAdsSource": "dpone.runtime.sources.api.google_ads:GoogleAdsSource",
    "YandexWebmasterSource": "dpone.runtime.sources.api.yandex_webmaster:YandexWebmasterSource",
    # Strategies
    "SourceStrategy": "dpone.runtime.sources.strategies.base:SourceStrategy",
    "PostgresFullExtractStrategy": "dpone.runtime.sources.strategies.postgres.postgres_full_extract:PostgresFullExtractStrategy",
    "PostgresXMinExtractStrategy": "dpone.runtime.sources.strategies.postgres.postgres_xmin_extract:PostgresXMinExtractStrategy",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if not target:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    mod = import_module(module_name)
    value = getattr(mod, attr)
    globals()[name] = value  # cache
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(__all__)))
