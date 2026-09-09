from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pytest

from dpone.config import LoadConfig, LoadStrategy

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.nightly]


@dataclass
class _LiveLogger:
    def info(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        del args, kwargs

    def warning(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        del args, kwargs

    def log_etl_progress(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        del args, kwargs


def _resolve_window(settings) -> tuple[str, str]:
    if settings.date_from and settings.date_to:
        return settings.date_from, settings.date_to
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(settings.days_back - 1, 0))
    return start_date.isoformat(), end_date.isoformat()


def _build_load_config(settings) -> LoadConfig:
    date_from, date_to = _resolve_window(settings)
    options = {
        "resource": settings.resource,
        "start_date": date_from,
        "end_date": date_to,
    }
    if settings.customer_ids:
        options["customer_ids"] = list(settings.customer_ids)

    return LoadConfig(
        source_conn_id="api__google_ads",
        target_conn_id="bigquery-dwh",
        source_schema="app",
        source_table=settings.resource,
        target_schema="landing__google_ads__api",
        target_table=f"app__{settings.resource}",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options=options,
    )


def test_google_ads_live_connector_builds_from_vault(google_ads_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    pytest.importorskip("google.ads.googleads.client")
    from dpone.runtime.connectors.api.google_ads import GoogleAdsConnector

    connector = GoogleAdsConnector.from_vault(
        vault_path=google_ads_live_settings.vault_path,
        timeout=google_ads_live_settings.timeout,
        max_retries=google_ads_live_settings.max_retries,
        rate_limit_delay=google_ads_live_settings.rate_limit_delay,
    )

    assert connector.health_check() is True


def test_google_ads_live_fetch_ads_stats(google_ads_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    pytest.importorskip("google.ads.googleads.client")
    from dpone.runtime.connectors.api.google_ads import GoogleAdsConnector
    from dpone.runtime.sources.api.google_ads import GoogleAdsSource

    connector = GoogleAdsConnector.from_vault(
        vault_path=google_ads_live_settings.vault_path,
        timeout=google_ads_live_settings.timeout,
        max_retries=google_ads_live_settings.max_retries,
        rate_limit_delay=google_ads_live_settings.rate_limit_delay,
    )
    source = GoogleAdsSource(connector=connector, sink_connector=None, logger=_LiveLogger())
    result = source.extract(_build_load_config(google_ads_live_settings), None)
    rows = result.artifact._rows

    assert isinstance(rows, list)
    if rows:
        first = rows[0]
        assert isinstance(first, dict)
        for key in ("date", "login", "campaign", "campaign_id", "term", "impressions", "clicks", "cost", "report"):
            assert key in first
