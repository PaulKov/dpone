from __future__ import annotations

from dpone.runtime.connectors.api.yandex_webmaster import YandexWebmasterConnector, YandexWebmasterCredentials
from dpone.runtime.connectors.api.yandex_webmaster_resources import yandex_webmaster_table_name


class DummyVaultManager:
    def __init__(self, secret: dict[str, object]) -> None:
        self.secret = secret
        self.calls: list[tuple[str, str]] = []

    def get_secret(self, *, mount_point: str, path: str):
        self.calls.append((mount_point, path))
        return dict(self.secret)


class DummyConnector(YandexWebmasterConnector):
    def __init__(self) -> None:
        super().__init__(
            credentials=YandexWebmasterCredentials(
                endpoint="https://api.webmaster.yandex.net/v4",
                api_key="oauth-token",
            )
        )

    def get(self, endpoint: str, params=None, **kwargs):  # noqa: ANN001, ARG002
        if endpoint == "/user":
            return {"user_id": 42}
        raise AssertionError(f"Unexpected endpoint in DummyConnector.get(): {endpoint}")

    def get_user_id(self) -> int:
        return 42

    def list_hosts(self, user_id: int | None = None) -> list[dict[str, object]]:
        assert user_id == 42
        return [
            {
                "host_id": "host-1",
                "ascii_host_url": "https://travel.example.com",
                "unicode_host_url": "https://travel.example.com",
            }
        ]

    def get_indexing_history(self, *, user_id: int, host_id: str, date_from: str, date_to: str):  # noqa: ARG002
        return {"indicators": {"HTTP_2XX": [{"date": "2026-03-01", "value": 10}]}}

    def get_in_search_history(self, *, user_id: int, host_id: str, date_from: str, date_to: str):  # noqa: ARG002
        return {"history": [{"date": "2026-03-01T12:00:00+0300", "value": 7}]}

    def get_search_events_history(self, *, user_id: int, host_id: str, date_from: str, date_to: str):  # noqa: ARG002
        return {"indicators": {"APPEARED_IN_SEARCH": [{"date": "2026-03-01", "value": 2}]}}

    def get_search_queries_popular(
        self,
        *,
        user_id: int,
        host_id: str,
        date_from: str,
        date_to: str,
        order_by: str = "TOTAL_SHOWS",
        limit: int = 500,
        offset: int = 0,
        query_indicators=(),
    ):  # noqa: ANN001,ARG002
        return {"queries": [{"query_id": "q-1", "query_text": "example_travel"}]}

    def get_search_query_history(
        self,
        *,
        user_id: int,
        host_id: str,
        query_id: str,
        date_from: str,
        date_to: str,
        device_type: str,
        query_indicators=(),
    ):  # noqa: ANN001,ARG002
        return {"indicators": {"TOTAL_SHOWS": [{"date": "2026-03-01", "value": 12}]}}

    def post_query_analytics_list(
        self,
        *,
        user_id: int,
        host_id: str,
        region_ids,
        limit: int = 500,
        offset: int = 0,
        device_type_indicator: str = "ALL",
        search_location: str = "WEB_LOCATION",
        text_indicator: str = "QUERY",
        order_by: str = "TOTAL_SHOWS",
    ):  # noqa: ANN001,ARG002
        return {
            "text_indicator_to_statistics": [
                {
                    "text_indicator": {"value": "example_travel"},
                    "popular_complementary_indicator": {"value": "https://travel.example.com/"},
                    "statistics": [{"date": "2026-03-01", "field": "IMPRESSIONS", "value": 10}],
                }
            ]
        }


def test_yandex_webmaster_credentials_from_dict_and_vault() -> None:
    creds = YandexWebmasterCredentials.from_dict({"oauth_token": "oauth-token"})
    assert creds.api_key == "oauth-token"
    assert creds.endpoint == "https://api.webmaster.yandex.net/v4"

    manager = DummyVaultManager({"oauth_token": "oauth-token", "endpoint": "https://example.test/v4"})
    from_vault = YandexWebmasterCredentials.from_vault("api/yandex_webmaster", vault_manager=manager)
    assert from_vault.endpoint == "https://example.test/v4"
    assert manager.calls == [("dev", "api/yandex_webmaster")]


def test_yandex_webmaster_credentials_accept_token_aliases() -> None:
    token_creds = YandexWebmasterCredentials.from_dict({"token": "oauth-token"})
    assert token_creds.api_key == "oauth-token"

    api_key_creds = YandexWebmasterCredentials.from_dict({"api_key": "legacy-token"})
    assert api_key_creds.api_key == "legacy-token"


def test_yandex_webmaster_connector_resolves_host_and_raw_resources() -> None:
    connector = DummyConnector()

    assert connector.resolve_user_id() == 42
    assert connector.resolve_host_id(user_id=42, host_url="https://travel.example.com") == "host-1"
    assert list(connector.get_resources("user")) == [{"user_id": 42}]
    assert list(connector.get_resources("hosts", user_id=42))[0]["host_id"] == "host-1"
    assert (
        list(
            connector.get_resources(
                "indexing_history",
                user_id=42,
                host_url="https://travel.example.com",
                date_from="2026-03-01",
                date_to="2026-03-01",
            )
        )[0]["indicators"]["HTTP_2XX"][0]["value"]
        == 10
    )
    assert (
        list(
            connector.get_resources(
                "search_queries_popular",
                user_id=42,
                host_url="https://travel.example.com",
                date_from="2026-03-01",
                date_to="2026-03-01",
            )
        )[0]["queries"][0]["query_text"]
        == "example_travel"
    )
    assert (
        list(
            connector.get_resources(
                "query_analytics_list",
                user_id=42,
                host_url="https://travel.example.com",
                region_ids="225",
                date_from="2026-03-01",
                date_to="2026-03-01",
            )
        )[0]["text_indicator_to_statistics"][0]["text_indicator"]["value"]
        == "example_travel"
    )
    assert yandex_webmaster_table_name("host_metrics_daily") == "app__host_metrics_daily"
    assert yandex_webmaster_table_name("query_analytics_by_region_daily") == "app__query_analytics_by_region_daily"
