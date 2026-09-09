from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors.api.google_ads import GoogleAdsConnector, GoogleAdsCredentials
from dpone.runtime.sources.api.google_ads import GoogleAdsSource

pytestmark = [pytest.mark.integration]


class DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def log_etl_progress(self, *args, **kwargs):
        return None


class MockGoogleAdsConnector(GoogleAdsConnector):
    def __init__(self) -> None:
        super().__init__(
            credentials=GoogleAdsCredentials.from_dict(
                {
                    "developer_token": "dev-token",
                    "customer_ids": ["1234567890"],
                    "client_id": "cid",
                    "client_secret": "secret",
                    "refresh_token": "refresh",
                }
            ),
            timeout=5,
        )
        self.calls: list[tuple[str, str]] = []

    def health_check(self) -> bool:
        return True

    def execute_query(self, *, customer_id: str, query: str):
        self.calls.append((customer_id, query))

        def _row(term: str | None):
            keyword = None if term is None else SimpleNamespace(text=term)
            return SimpleNamespace(
                segments=SimpleNamespace(date="2026-03-01"),
                campaign=SimpleNamespace(name="Campaign", id=42),
                ad_group_criterion=SimpleNamespace(keyword=keyword),
                metrics=SimpleNamespace(impressions=10, clicks=2, cost_micros=5_000_000),
            )

        if "keyword_view" in query:
            return [_row("paris")]
        return [_row(None)]


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="api__google_ads",
        target_conn_id="bigquery-dwh",
        source_schema="app",
        source_table="ads_stats",
        target_schema="landing__google_ads__api",
        target_table="app__ads_stats",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "resource": "ads_stats",
            "customer_ids": ["1234567890"],
            "start_date": "2026-03-01",
            "end_date": "2026-03-02",
        },
    )


def test_google_ads_mock_full_extract_returns_rows_across_supported_reports() -> None:
    connector = MockGoogleAdsConnector()
    source = GoogleAdsSource(connector=connector, sink_connector=None, logger=DummyLogger())

    assert connector.health_check() is True

    result = source.extract(_load_config(), None)
    rows = result.artifact._rows

    assert len(rows) == 7
    assert sorted({row["report"] for row in rows}) == [
        "app",
        "discovery",
        "display",
        "keywords",
        "pmax",
        "shopping",
        "webpages",
    ]
    assert rows[0]["login"] == "1234567890"
    assert rows[0]["campaign_id"] == 42
    assert connector.calls
