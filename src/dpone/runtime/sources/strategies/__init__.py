"""Стратегии извлечения данных для источников.

Exports are lazy so lightweight tooling/tests can import API subpackages without
pulling optional runtime dependencies (e.g. BigQuery-backed xmin state).
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "SourceStrategy",
    "PostgresFullExtractStrategy",
    "PostgresXMinExtractStrategy",
    "PostgresIncrementalExtractStrategy",
    "ClickHouseFullExtractStrategy",
    "ClickHouseIncrementalExtractStrategy",
    "APIBaseStrategy",
    "APIExtractConfig",
    "OmnideskFullExtractStrategy",
    "OmnideskIncrementalMergeExtractStrategy",
    "OmnideskIncrementalAppendExtractStrategy",
    "AppsflyerFullExtractStrategy",
    "AppsflyerIncrementalAppendExtractStrategy",
    "CbrFullExtractStrategy",
    "CbrIncrementalMergeExtractStrategy",
    "GoogleSheetsFullExtractStrategy",
    "GoogleAdsFullExtractStrategy",
    "GoogleAdsIncrementalMergeExtractStrategy",
    "MindboxFullExtractStrategy",
    "MindboxIncrementalMergeExtractStrategy",
    "MindboxReplaceExtractStrategy",
    "YandexWebmasterFullExtractStrategy",
    "YandexWebmasterIncrementalMergeExtractStrategy",
    "YandexWebmasterReplaceExtractStrategy",
    "FasttrackFullExtractStrategy",
    "FasttrackIncrementalMergeExtractStrategy",
]

_EXPORTS: dict[str, str] = {
    "SourceStrategy": "dpone.runtime.sources.strategies.base:SourceStrategy",
    "PostgresFullExtractStrategy": "dpone.runtime.sources.strategies.postgres.postgres_full_extract:PostgresFullExtractStrategy",
    "PostgresXMinExtractStrategy": "dpone.runtime.sources.strategies.postgres.postgres_xmin_extract:PostgresXMinExtractStrategy",
    "PostgresIncrementalExtractStrategy": "dpone.runtime.sources.strategies.postgres.postgres_incremental_extract:PostgresIncrementalExtractStrategy",
    "ClickHouseFullExtractStrategy": "dpone.runtime.sources.strategies.clickhouse.clickhouse_full_extract:ClickHouseFullExtractStrategy",
    "ClickHouseIncrementalExtractStrategy": "dpone.runtime.sources.strategies.clickhouse.clickhouse_incremental_extract:ClickHouseIncrementalExtractStrategy",
    "APIBaseStrategy": "dpone.runtime.sources.strategies.api.base:APIBaseStrategy",
    "APIExtractConfig": "dpone.runtime.sources.strategies.api.base:APIExtractConfig",
    "OmnideskFullExtractStrategy": "dpone.runtime.sources.strategies.api.omnidesk.full_extract:OmnideskFullExtractStrategy",
    "OmnideskIncrementalMergeExtractStrategy": "dpone.runtime.sources.strategies.api.omnidesk.incremental_merge_extract:OmnideskIncrementalMergeExtractStrategy",
    "OmnideskIncrementalAppendExtractStrategy": "dpone.runtime.sources.strategies.api.omnidesk.incremental_append_extract:OmnideskIncrementalAppendExtractStrategy",
    "AppsflyerFullExtractStrategy": "dpone.runtime.sources.strategies.api.appsflyer.full_extract:AppsflyerFullExtractStrategy",
    "AppsflyerIncrementalAppendExtractStrategy": "dpone.runtime.sources.strategies.api.appsflyer.incremental_append_extract:AppsflyerIncrementalAppendExtractStrategy",
    "CbrFullExtractStrategy": "dpone.runtime.sources.strategies.api.cbr.full_extract:CbrFullExtractStrategy",
    "CbrIncrementalMergeExtractStrategy": "dpone.runtime.sources.strategies.api.cbr.incremental_merge_extract:CbrIncrementalMergeExtractStrategy",
    "GoogleSheetsFullExtractStrategy": "dpone.runtime.sources.strategies.api.google_sheets.full_extract:GoogleSheetsFullExtractStrategy",
    "GoogleAdsFullExtractStrategy": "dpone.runtime.sources.strategies.api.google_ads.full_extract:GoogleAdsFullExtractStrategy",
    "GoogleAdsIncrementalMergeExtractStrategy": "dpone.runtime.sources.strategies.api.google_ads.incremental_merge_extract:GoogleAdsIncrementalMergeExtractStrategy",
    "MindboxFullExtractStrategy": "dpone.runtime.sources.strategies.api.mindbox.full_extract:MindboxFullExtractStrategy",
    "MindboxIncrementalMergeExtractStrategy": "dpone.runtime.sources.strategies.api.mindbox.incremental_merge_extract:MindboxIncrementalMergeExtractStrategy",
    "MindboxReplaceExtractStrategy": "dpone.runtime.sources.strategies.api.mindbox.replace_extract:MindboxReplaceExtractStrategy",
    "YandexWebmasterFullExtractStrategy": "dpone.runtime.sources.strategies.api.yandex_webmaster.full_extract:YandexWebmasterFullExtractStrategy",
    "YandexWebmasterIncrementalMergeExtractStrategy": "dpone.runtime.sources.strategies.api.yandex_webmaster.incremental_merge_extract:YandexWebmasterIncrementalMergeExtractStrategy",
    "YandexWebmasterReplaceExtractStrategy": "dpone.runtime.sources.strategies.api.yandex_webmaster.replace_extract:YandexWebmasterReplaceExtractStrategy",
    "FasttrackFullExtractStrategy": "dpone.runtime.sources.strategies.api.fasttrack.full_extract:FasttrackFullExtractStrategy",
    "FasttrackIncrementalMergeExtractStrategy": "dpone.runtime.sources.strategies.api.fasttrack.incremental_merge_extract:FasttrackIncrementalMergeExtractStrategy",
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
