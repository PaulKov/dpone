from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.nightly]


def _resolve_window(settings: Any) -> tuple[date, date]:
    if settings.date_from and settings.date_to:
        return date.fromisoformat(settings.date_from), date.fromisoformat(settings.date_to)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(settings.days_back - 1, 0))
    return start_date, end_date


def test_appsflyer_live_health_check(appsflyer_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    from dpone.runtime.connectors.api.appsflyer import AppsflyerConnector

    connector = AppsflyerConnector.from_vault(
        vault_path=appsflyer_live_settings.vault_path,
        default_app_id=appsflyer_live_settings.default_app_id,
        max_retries=1,
        timeout=60,
    )
    assert connector.default_app_id == appsflyer_live_settings.default_app_id
    assert connector.credentials.endpoint.endswith("/api/raw-data/export/app")


def test_appsflyer_live_fetch_small_window(appsflyer_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    from dpone.runtime.connectors.api.appsflyer import AppsflyerConnector, AppsflyerQuotaExceededError

    start_date, end_date = _resolve_window(appsflyer_live_settings)
    connector = AppsflyerConnector.from_vault(
        vault_path=appsflyer_live_settings.vault_path,
        default_app_id=appsflyer_live_settings.default_app_id,
        max_retries=1,
        timeout=60,
    )

    try:
        rows = connector.fetch_resource_rows(
            resource_name=appsflyer_live_settings.resource,
            app_id=appsflyer_live_settings.default_app_id,
            from_value=start_date,
            to_value=end_date,
            timezone_name=appsflyer_live_settings.timezone,
            maximum_rows=appsflyer_live_settings.maximum_rows,
        )
    except AppsflyerQuotaExceededError as exc:
        pytest.skip(str(exc))

    assert isinstance(rows, list)
    if rows:
        first = rows[0]
        assert first["source_app_id"] == appsflyer_live_settings.default_app_id
        assert first["resource_name"] == appsflyer_live_settings.resource
        assert "date" in first
