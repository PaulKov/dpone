from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors.api.yandex_webmaster_resources import get_yandex_webmaster_resource

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.nightly]


@dataclass
class _LiveLogger:
    def info(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        del args, kwargs

    def warning(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        del args, kwargs

    def log_etl_progress(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        del args, kwargs


def _resolve_window(settings) -> tuple[str | None, str | None, str | None]:
    if settings.day:
        return settings.day, None, None
    if settings.date_from or settings.date_to:
        start = settings.date_from or settings.date_to
        end = settings.date_to or settings.date_from
        return None, start, end
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(settings.days_back - 1, 0))
    return None, start_date.isoformat(), end_date.isoformat()


def _build_load_config(settings) -> LoadConfig:
    day, date_from, date_to = _resolve_window(settings)
    spec = get_yandex_webmaster_resource(settings.resource)
    return LoadConfig(
        source_conn_id="api__yandex_webmaster",
        target_conn_id="bigquery-dwh",
        source_schema="app",
        source_table=settings.resource,
        target_schema="landing__yandex_webmaster__api",
        target_table=f"app__{settings.resource}",
        load_strategy=LoadStrategy(spec.default_load_strategy),
        options={
            "resource": settings.resource,
            "user_id": settings.user_id,
            "host_id": settings.host_id,
            "host_url": settings.host_url,
            "device_types": ",".join(settings.device_types),
            "region_ids": ",".join(str(item) for item in settings.region_ids),
            "day": day,
            "start_date": date_from,
            "end_date": date_to,
            "pages_in_search_daily_agg": settings.pages_in_search_daily_agg,
        },
    )


def test_yandex_webmaster_live_connector_builds_from_vault(yandex_webmaster_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    from dpone.runtime.connectors.api.yandex_webmaster import YandexWebmasterConnector

    connector = YandexWebmasterConnector.from_vault(
        vault_path=yandex_webmaster_live_settings.vault_path,
        timeout=yandex_webmaster_live_settings.timeout,
        max_retries=yandex_webmaster_live_settings.max_retries,
        rate_limit_delay=yandex_webmaster_live_settings.rate_limit_delay,
    )

    assert connector.health_check() is True


def test_yandex_webmaster_live_fetch_host_metrics_daily(yandex_webmaster_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    from dpone.runtime.connectors.api.yandex_webmaster import YandexWebmasterConnector
    from dpone.runtime.sources.api.yandex_webmaster import YandexWebmasterSource

    connector = YandexWebmasterConnector.from_vault(
        vault_path=yandex_webmaster_live_settings.vault_path,
        timeout=yandex_webmaster_live_settings.timeout,
        max_retries=yandex_webmaster_live_settings.max_retries,
        rate_limit_delay=yandex_webmaster_live_settings.rate_limit_delay,
    )
    source = YandexWebmasterSource(connector=connector, sink_connector=None, logger=_LiveLogger())
    result = source.extract(_build_load_config(yandex_webmaster_live_settings), None)
    rows = result.artifact._rows

    assert isinstance(rows, list)
    if rows:
        first = rows[0]
        assert isinstance(first, dict)
        for key in (
            "date",
            "http_2xx",
            "http_3xx",
            "http_4xx",
            "pages_in_search",
            "appeared_in_search",
            "removed_from_search",
        ):
            assert key in first
