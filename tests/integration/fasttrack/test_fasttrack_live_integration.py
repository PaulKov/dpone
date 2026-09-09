from __future__ import annotations

from typing import Any

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.nightly]


def _build_filters(settings: Any) -> dict[str, object]:
    payload: dict[str, object] = {}
    if settings.dashboard_uuid:
        payload["uuid"] = settings.dashboard_uuid
    if settings.category:
        payload["category"] = settings.category
    if settings.limit is not None:
        payload["limit"] = settings.limit
    if settings.offset is not None:
        payload["offset"] = settings.offset
    if settings.page_size is not None:
        payload["page_size"] = settings.page_size
    return payload


def test_fasttrack_live_health_check(fasttrack_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    from dpone.runtime.connectors.api.fasttrack import FasttrackConnector

    connector = FasttrackConnector.from_vault(
        vault_path=fasttrack_live_settings.vault_path,
        timeout=fasttrack_live_settings.timeout,
        max_retries=fasttrack_live_settings.max_retries,
        rate_limit_delay=fasttrack_live_settings.rate_limit_delay,
    )
    assert connector.health_check() is True


def test_fasttrack_live_fetch_small_sample(fasttrack_live_settings) -> None:
    pytest.importorskip("vault_kv_client")
    from dpone.runtime.connectors.api.fasttrack import FasttrackConnector

    connector = FasttrackConnector.from_vault(
        vault_path=fasttrack_live_settings.vault_path,
        timeout=fasttrack_live_settings.timeout,
        max_retries=fasttrack_live_settings.max_retries,
        rate_limit_delay=fasttrack_live_settings.rate_limit_delay,
    )
    rows = connector.fetch_resource_rows(fasttrack_live_settings.resource, **_build_filters(fasttrack_live_settings))

    assert isinstance(rows, list)
    if rows:
        assert isinstance(rows[0], dict)
        assert rows[0]
