from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.nightly]


def _resolve_request(settings: Any) -> dict[str, object]:
    if settings.day:
        return {"day": date.fromisoformat(settings.day)}
    if settings.date_from or settings.date_to:
        payload: dict[str, object] = {}
        if settings.date_from:
            payload["start_date"] = date.fromisoformat(settings.date_from)
        if settings.date_to:
            payload["end_date"] = date.fromisoformat(settings.date_to)
        return payload
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(settings.days_back - 1, 0))
    return {"start_date": start_date, "end_date": end_date}


def test_openexchangerates_live_health_check(openexchangerates_live_settings) -> None:
    from dpone.runtime.connectors.api.openexchangerates import OpenExchangeRatesConnector

    connector = OpenExchangeRatesConnector.from_vault(
        vault_path=openexchangerates_live_settings.vault_path,
        timeout=openexchangerates_live_settings.timeout,
        max_retries=openexchangerates_live_settings.max_retries,
        retry_delay=openexchangerates_live_settings.retry_delay,
        rate_limit_delay=openexchangerates_live_settings.rate_limit_delay,
    )
    assert connector.health_check() is True


def test_openexchangerates_live_fetch_small_window(openexchangerates_live_settings) -> None:
    from dpone.runtime.connectors.api.openexchangerates import OpenExchangeRatesConnector

    connector = OpenExchangeRatesConnector.from_vault(
        vault_path=openexchangerates_live_settings.vault_path,
        timeout=openexchangerates_live_settings.timeout,
        max_retries=openexchangerates_live_settings.max_retries,
        retry_delay=openexchangerates_live_settings.retry_delay,
        rate_limit_delay=openexchangerates_live_settings.rate_limit_delay,
    )
    rows = list(
        connector.get_resources(
            openexchangerates_live_settings.resource,
            filters={
                **_resolve_request(openexchangerates_live_settings),
                "symbols": openexchangerates_live_settings.symbols,
            },
        )
    )

    assert isinstance(rows, list)
    assert rows
    first = rows[0]
    assert first["symbol"]
    assert first["as_of_date"]
    assert first["rate"] is not None
