from __future__ import annotations

from datetime import date, timedelta

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.nightly]


def _resolve_snapshot_month(explicit: str | None) -> str:
    if explicit:
        raw = explicit.strip()
        if len(raw) == 7:
            return f"{raw}-01"
        return raw[:10]

    today = date.today().replace(day=1)
    previous_month_last_day = today - timedelta(days=1)
    return previous_month_last_day.replace(day=1).isoformat()


def _month_bounds(snapshot_month: str) -> tuple[date, date]:
    first_day = date.fromisoformat(snapshot_month)
    if first_day.month == 12:
        next_month = date(first_day.year + 1, 1, 1)
    else:
        next_month = date(first_day.year, first_day.month + 1, 1)
    return first_day, next_month - timedelta(days=1)


def test_similarweb_live_connector_builds_from_vault(similarweb_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    from dpone.runtime.connectors.api.similarweb import SimilarwebConnector

    connector = SimilarwebConnector.from_vault(
        vault_path=similarweb_live_settings.vault_path,
        timeout=similarweb_live_settings.timeout,
        max_retries=similarweb_live_settings.max_retries,
        rate_limit_delay=similarweb_live_settings.rate_limit_delay,
    )

    assert connector.credentials.endpoint
    assert connector.credentials.api_key


def test_similarweb_live_fetch_small_sample(similarweb_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    from dpone.runtime.connectors.api.similarweb import SimilarwebConnector

    snapshot_month = _resolve_snapshot_month(similarweb_live_settings.snapshot_month)
    start_date, end_date = _month_bounds(snapshot_month)
    connector = SimilarwebConnector.from_vault(
        vault_path=similarweb_live_settings.vault_path,
        timeout=similarweb_live_settings.timeout,
        max_retries=similarweb_live_settings.max_retries,
        rate_limit_delay=similarweb_live_settings.rate_limit_delay,
    )

    rows = connector.fetch_resource_rows(
        resource_name=similarweb_live_settings.resource,
        url=similarweb_live_settings.default_domain,
        start_date=start_date,
        end_date=end_date,
        limit=similarweb_live_settings.limit,
        page_size=similarweb_live_settings.page_size,
        traffic_source=similarweb_live_settings.traffic_source,
        web_source=similarweb_live_settings.web_source,
        branded_type=similarweb_live_settings.branded_type,
        country=similarweb_live_settings.country,
    )

    assert isinstance(rows, list)
    if rows:
        first = rows[0]
        assert isinstance(first, dict)
        assert "keyword" in first
        assert "top_url" in first
