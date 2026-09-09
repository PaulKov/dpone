from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors.api.similarweb import SimilarwebConnector, SimilarwebCredentials
from dpone.runtime.sources.api.similarweb import SimilarwebSource

pytestmark = [pytest.mark.integration]


class DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def log_etl_progress(self, *args, **kwargs):
        return None


class DummySinkConnector:
    def get_records(self, query, as_dict=False):
        del query, as_dict
        return []


@pytest.fixture()
def similarweb_mock_server():
    requests_seen: list[dict[str, object]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != "/v4/website-analysis/keywords":
                self.send_response(404)
                self.end_headers()
                return

            params = parse_qs(parsed.query)
            offset = int((params.get("offset") or ["0"])[0])
            requests_seen.append(
                {
                    "path": parsed.path,
                    "params": {key: values[0] for key, values in params.items()},
                }
            )
            pages = {
                0: [
                    {
                        "keyword": "example_travel paris",
                        "clicks": 10,
                        "traffic_share": 0.22,
                        "difficulty": 11,
                        "competition": 0.31,
                        "primary_intent": "Informational",
                        "secondary_intent": "Navigational",
                        "volume": 100,
                        "cpc": 1.5,
                        "cpc_low_bid": 1.0,
                        "cpc_high_bid": 2.0,
                        "zero_clicks_share": 0.11,
                        "position": 1,
                        "serp_features": ["images", "news"],
                        "top_url": "https://travel.example.com/paris",
                    },
                    {
                        "keyword": "example_travel berlin",
                        "clicks": 8,
                        "traffic_share": 0.18,
                        "difficulty": 9,
                        "competition": 0.27,
                        "primary_intent": "Informational",
                        "secondary_intent": "Commercial",
                        "volume": 70,
                        "cpc": 1.2,
                        "cpc_low_bid": 0.9,
                        "cpc_high_bid": 1.7,
                        "zero_clicks_share": 0.07,
                        "position": 2,
                        "serp_features": ["video"],
                        "top_url": "https://travel.example.com/berlin",
                    },
                ],
                2: [
                    {
                        "keyword": "example_travel rome",
                        "clicks": 6,
                        "traffic_share": 0.14,
                        "difficulty": 7,
                        "competition": 0.21,
                        "primary_intent": "Informational",
                        "secondary_intent": "Commercial",
                        "volume": 50,
                        "cpc": 1.1,
                        "cpc_low_bid": 0.7,
                        "cpc_high_bid": 1.4,
                        "zero_clicks_share": 0.05,
                        "position": 3,
                        "serp_features": [],
                        "top_url": "https://travel.example.com/rome",
                    }
                ],
            }

            payload = json.dumps({"keywords": pages.get(offset, [])}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(payload)

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
        source_conn_id="api__similarweb",
        target_conn_id="bigquery-dwh",
        source_schema="default",
        source_table="keywords",
        target_schema="landing__similarweb__api",
        target_table="default__keywords",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        options={
            "resource": "keywords",
            "domains": ["travel.example.com"],
            "snapshot_month": "2026-02",
            "limit": 3,
            "page_size": 2,
            "min_keywords_count": 1,
        },
    )


def test_similarweb_mock_incremental_append_keywords(similarweb_mock_server) -> None:
    server, requests_seen = similarweb_mock_server
    source = SimilarwebSource(
        connector=SimilarwebConnector(
            credentials=SimilarwebCredentials(
                endpoint=f"http://127.0.0.1:{server.server_port}",
                api_key="sw-token",
            )
        ),
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )

    result = source.extract(_load_config(), None)
    rows = result.artifact._rows

    assert len(rows) == 3
    assert rows[0]["domain"] == "travel.example.com"
    assert rows[0]["serp_features"] == ["images", "news"]
    assert len(requests_seen) == 2
    assert requests_seen[0]["params"]["api_key"] == "sw-token"
    assert requests_seen[0]["params"]["start_date"] == "2026-02"
