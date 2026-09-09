from __future__ import annotations

import warnings
from importlib import import_module

import pytest


@pytest.mark.parametrize(
    ("legacy_module", "focused_module", "symbols"),
    [
        (
            "dpone.runtime.cdc.postgres_impl",
            "dpone.runtime.cdc.postgres_logical_reader",
            ["PostgresLogicalCDCReader", "PostgresLogicalCDCReaderConfig"],
        ),
        (
            "dpone.runtime.connectors.api.appsflyer_impl",
            "dpone.runtime.connectors.api.appsflyer_connector",
            ["AppsflyerConnector", "AppsflyerCredentials", "AppsflyerQuotaExceededError"],
        ),
        (
            "dpone.runtime.connectors.api.google_sheets_impl",
            "dpone.runtime.connectors.api.google_sheets_connector",
            ["GoogleSheetsConnector", "GoogleSheetsCredentials"],
        ),
        (
            "dpone.runtime.connectors.bigquery_impl",
            "dpone.runtime.connectors.bigquery_connector",
            ["BigQueryConnector"],
        ),
        (
            "dpone.runtime.sinks.clickhouse_impl",
            "dpone.runtime.sinks.clickhouse_sink",
            ["ClickHouseSink"],
        ),
        (
            "dpone.runtime.sinks.strategies.mssql.mssql_strategies_impl",
            "dpone.runtime.sinks.strategies.mssql.mssql_load_strategies",
            [
                "MSSQLStrategyBase",
                "MSSQLFullRefreshStrategy",
                "MSSQLIncrementAppendStrategy",
                "MSSQLIncrementMergeStrategy",
                "MSSQLPartitionReplaceStrategy",
                "MSSQLReplaceStrategy",
            ],
        ),
        (
            "dpone.runtime.sources.strategies.api.yandex_webmaster.common_impl",
            "dpone.runtime.sources.strategies.api.yandex_webmaster.support",
            [
                "YANDEX_WEBMASTER_SCHEMA",
                "YWM_SCHEMA",
                "YandexWebmasterRegion",
                "build_yandex_webmaster_rows",
            ],
        ),
        (
            "dpone.runtime.sources.strategies.postgres.postgres_base_impl",
            "dpone.runtime.sources.strategies.postgres.postgres_base_strategy",
            ["PostgresBaseStrategy"],
        ),
    ],
)
def test_runtime_deprecated_impl_shims_reexport_focused_modules(
    legacy_module: str,
    focused_module: str,
    symbols: list[str],
) -> None:
    legacy = import_module(legacy_module)
    focused = import_module(focused_module)

    for symbol in symbols:
        assert getattr(legacy, symbol) is getattr(focused, symbol)


def test_google_sheets_legacy_shims_reexport_canonical_runtime() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from dpone.lib.connectors.api.google_sheets import GoogleSheetsConnector as LegacyConnector
        from dpone.source.api.google_sheets import GoogleSheetsSource as LegacySource

    from dpone.runtime.connectors.api.google_sheets import GoogleSheetsConnector as RuntimeConnector
    from dpone.runtime.sources.api.google_sheets import GoogleSheetsSource as RuntimeSource

    assert LegacyConnector is RuntimeConnector
    assert LegacySource is RuntimeSource


def test_yandex_webmaster_legacy_shims_reexport_canonical_runtime() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from dpone.lib.connectors.api.yandex_webmaster import YandexWebmasterConnector as LegacyConnector
        from dpone.source.api.yandex_webmaster import YandexWebmasterSource as LegacySource

    from dpone.runtime.connectors.api.yandex_webmaster import YandexWebmasterConnector as RuntimeConnector
    from dpone.runtime.sources.api.yandex_webmaster import YandexWebmasterSource as RuntimeSource

    assert LegacyConnector is RuntimeConnector
    assert LegacySource is RuntimeSource


def test_google_ads_legacy_shims_reexport_canonical_runtime() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from dpone.lib.connectors.api.google_ads import GoogleAdsConnector as LegacyConnector
        from dpone.source.api.google_ads import GoogleAdsSource as LegacySource

    from dpone.runtime.connectors.api.google_ads import GoogleAdsConnector as RuntimeConnector
    from dpone.runtime.sources.api.google_ads import GoogleAdsSource as RuntimeSource

    assert LegacyConnector is RuntimeConnector
    assert LegacySource is RuntimeSource


def test_cbr_legacy_shims_reexport_canonical_runtime() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from dpone.lib.connectors.api.cbr import CbrConnector as LegacyConnector
        from dpone.source.api.cbr import CbrSource as LegacySource

    from dpone.runtime.connectors.api.cbr import CbrConnector as RuntimeConnector
    from dpone.runtime.sources.api.cbr import CbrSource as RuntimeSource

    assert LegacyConnector is RuntimeConnector
    assert LegacySource is RuntimeSource


def test_openexchangerates_legacy_shims_reexport_canonical_runtime() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from dpone.lib.connectors.api.openexchangerates import OpenExchangeRatesConnector as LegacyConnector
        from dpone.source.api.openexchangerates import OpenExchangeRatesSource as LegacySource

    from dpone.runtime.connectors.api.openexchangerates import OpenExchangeRatesConnector as RuntimeConnector
    from dpone.runtime.sources.api.openexchangerates import OpenExchangeRatesSource as RuntimeSource

    assert LegacyConnector is RuntimeConnector
    assert LegacySource is RuntimeSource
