from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors.api.yandex_webmaster import YandexWebmasterConnector, YandexWebmasterCredentials
from dpone.runtime.sources.api.yandex_webmaster import YandexWebmasterSource

pytestmark = [pytest.mark.integration]


class DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def log_etl_progress(self, *args, **kwargs):
        return None


@pytest.fixture()
def yandex_webmaster_mock_server():
    requests_seen: list[dict[str, object]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            requests_seen.append(
                {
                    "path": parsed.path,
                    "query": {key: values[0] for key, values in parse_qs(parsed.query).items()},
                    "auth": self.headers.get("Authorization"),
                }
            )

            if parsed.path == "/user":
                payload = {"user_id": 42}
            elif parsed.path == "/user/42/hosts":
                payload = {
                    "hosts": [
                        {
                            "host_id": "host-1",
                            "ascii_host_url": "https://travel.example.com",
                            "unicode_host_url": "https://travel.example.com",
                        }
                    ]
                }
            elif parsed.path == "/user/42/hosts/host-1/indexing/history":
                payload = {"indicators": {"HTTP_2XX": [{"date": "2026-03-01", "value": 10}]}}
            elif parsed.path == "/user/42/hosts/host-1/search-urls/in-search/history":
                payload = {"history": [{"date": "2026-03-01T23:00:00+0300", "value": 8}]}
            elif parsed.path == "/user/42/hosts/host-1/search-urls/events/history":
                payload = {"indicators": {"APPEARED_IN_SEARCH": [{"date": "2026-03-01", "value": 2}]}}
            elif parsed.path == "/user/42/hosts/host-1/search-queries/popular":
                payload = {"queries": [{"query_id": "q-1", "query_text": "example_travel"}]}
            elif parsed.path == "/user/42/hosts/host-1/search-queries/q-1/history":
                payload = {
                    "indicators": {
                        "TOTAL_SHOWS": [{"date": "2026-03-01", "value": 12}],
                        "TOTAL_CLICKS": [{"date": "2026-03-01", "value": 3}],
                        "AVG_SHOW_POSITION": [{"date": "2026-03-01", "value": 4.5}],
                        "AVG_CLICK_POSITION": [{"date": "2026-03-01", "value": 2.0}],
                    }
                }
            else:
                self.send_response(404)
                self.end_headers()
                return

            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):  # noqa: N802
            parsed = urlparse(self.path)
            content_length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(content_length).decode("utf-8") if content_length else "{}"
            requests_seen.append(
                {
                    "path": parsed.path,
                    "body": json.loads(raw_body),
                    "auth": self.headers.get("Authorization"),
                }
            )

            if parsed.path == "/user/42/hosts/host-1/query-analytics/list":
                payload = {
                    "text_indicator_to_statistics": [
                        {
                            "text_indicator": {"value": "example_travel"},
                            "popular_complementary_indicator": {"value": "https://travel.example.com/"},
                            "statistics": [
                                {"date": "2026-03-01", "field": "IMPRESSIONS", "value": 10},
                                {"date": "2026-03-01", "field": "CLICKS", "value": 2},
                                {"date": "2026-03-01", "field": "CTR", "value": 0.2},
                            ],
                        }
                    ]
                }
            else:
                self.send_response(404)
                self.end_headers()
                return

            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, requests_seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="api__yandex_webmaster",
        target_conn_id="bigquery-dwh",
        source_schema="app",
        source_table="host_metrics_daily",
        target_schema="landing__yandex_webmaster__api",
        target_table="app__host_metrics_daily",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "resource": "host_metrics_daily",
            "host_url": "https://travel.example.com",
            "day": "2026-03-01",
        },
    )


def test_yandex_webmaster_mock_full_extract(yandex_webmaster_mock_server) -> None:
    server, requests_seen = yandex_webmaster_mock_server
    source = YandexWebmasterSource(
        connector=YandexWebmasterConnector(
            credentials=YandexWebmasterCredentials(
                endpoint=f"http://127.0.0.1:{server.server_port}",
                api_key="oauth-token",
            )
        ),
        sink_connector=None,
        logger=DummyLogger(),
    )

    result = source.extract(_load_config(), None)
    rows = result.artifact._rows

    assert len(rows) == 1
    assert rows[0]["date"] == "2026-03-01"
    assert rows[0]["http_2xx"] == 10
    assert rows[0]["pages_in_search"] == 8
    assert rows[0]["appeared_in_search"] == 2
    assert requests_seen[0]["auth"] == "OAuth oauth-token"
    assert any(item["path"] == "/user/42/hosts/host-1/search-urls/events/history" for item in requests_seen)


def test_yandex_webmaster_mock_replace_extract_for_query_resources(yandex_webmaster_mock_server) -> None:
    server, requests_seen = yandex_webmaster_mock_server
    source = YandexWebmasterSource(
        connector=YandexWebmasterConnector(
            credentials=YandexWebmasterCredentials(
                endpoint=f"http://127.0.0.1:{server.server_port}",
                api_key="oauth-token",
            )
        ),
        sink_connector=None,
        logger=DummyLogger(),
    )

    load_config = LoadConfig(
        source_conn_id="api__yandex_webmaster",
        target_conn_id="bigquery-dwh",
        source_schema="app",
        source_table="query_analytics_by_region_daily",
        target_schema="landing__yandex_webmaster__api",
        target_table="app__query_analytics_by_region_daily",
        load_strategy=LoadStrategy.REPLACE,
        options={
            "resource": "query_analytics_by_region_daily",
            "host_url": "https://travel.example.com",
            "start_date": "2026-03-01",
            "end_date": "2026-03-01",
            "region_ids": "225",
        },
    )

    result = source.extract(load_config, None)
    rows = result.artifact._rows

    assert result.force_full_refresh is False
    assert load_config.custom_predicate == "date = DATE '2026-03-01'"
    assert rows == [
        {
            "date": "2026-03-01",
            "region_id": 225,
            "region_name": "Россия",
            "query": "example_travel",
            "url": "https://travel.example.com/",
            "impressions": 10,
            "clicks": 2,
            "ctr": 0.2,
        }
    ]
    assert any(item["path"] == "/user/42/hosts/host-1/query-analytics/list" for item in requests_seen)
