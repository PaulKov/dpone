from __future__ import annotations

import itertools
from datetime import date, datetime, timedelta
from typing import Any

import pytest

from dpone._compat import UTC

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.nightly]


def _parse_datetime_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is not None:
        return parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed.replace(tzinfo=None)


def _resolve_window(settings: Any) -> tuple[date | datetime | None, date | datetime | None]:
    since_dt = _parse_datetime_utc(getattr(settings, "since_datetime_utc", None))
    till_dt = _parse_datetime_utc(getattr(settings, "till_datetime_utc", None))
    if since_dt is not None or till_dt is not None:
        return since_dt, till_dt
    if settings.date_from and settings.date_to:
        return date.fromisoformat(settings.date_from), date.fromisoformat(settings.date_to)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=max(settings.days_back - 1, 0))
    return start_date, end_date


def test_mindbox_live_health_check(mindbox_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    from dpone.runtime.connectors.api.mindbox import MindboxConnector

    connector = MindboxConnector.from_vault(
        vault_path=mindbox_live_settings.vault_path,
        timeout=120,
        max_retries=1,
    )
    assert connector.health_check() is True


def test_mindbox_live_fetch_small_window(mindbox_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    from dpone.runtime.connectors.api.mindbox import MindboxConnector

    since, till = _resolve_window(mindbox_live_settings)
    connector = MindboxConnector.from_vault(
        vault_path=mindbox_live_settings.vault_path,
        timeout=120,
        max_retries=1,
    )
    rows_iter = connector.get_resources(
        mindbox_live_settings.resource,
        filters={
            "since": since,
            "till": till,
            "poll_interval": mindbox_live_settings.poll_interval,
            "export_timeout": mindbox_live_settings.export_timeout,
            "utc_boundary_time": mindbox_live_settings.utc_boundary_time,
        },
    )
    rows = list(itertools.islice(rows_iter, 25))
    close = getattr(rows_iter, "close", None)
    if callable(close):
        close()

    assert isinstance(rows, list)
    if rows:
        assert isinstance(rows[0], dict)
        assert rows[0]
