from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from dpone.runtime.connectors.api.similarweb import SimilarwebConnector, SimilarwebCredentials
from dpone.runtime.connectors.api.similarweb_resources import (
    get_similarweb_resource,
    list_similarweb_resources,
    similarweb_table_name,
)


def test_similarweb_resource_registry_has_expected_keywords_resource() -> None:
    resources = {item.name: item for item in list_similarweb_resources()}

    assert set(resources) == {"keywords"}
    assert resources["keywords"].endpoint_path == "/v4/website-analysis/keywords"
    assert resources["keywords"].unique_key == ("date", "domain", "keyword", "top_url")
    assert similarweb_table_name("keywords") == "default__keywords"


def test_similarweb_credentials_accept_token_alias_and_normalize_endpoints() -> None:
    creds = SimilarwebCredentials.from_dict(
        {
            "endpoint": "https://api.similarweb.com/v4/website-analysis/keywords",
            "token": "sw-token",
        }
    )

    assert creds.api_key == "sw-token"
    assert creds.endpoint == "https://api.similarweb.com"
    assert creds.get_headers()["Accept"] == "application/json"


def test_similarweb_credentials_from_vault_accepts_api_key_alias(monkeypatch) -> None:
    class DummyVault:
        def get_secret(self, mount_point: str, path: str):
            assert mount_point == "dev"
            assert path == "api/similarweb"
            return {
                "endpoint": "https://api.similarweb.com/v4",
                "api_key": "vault-sw-token",
            }

    monkeypatch.setattr("dpone.runtime.connectors.api.similarweb.get_env_code", lambda: "dev")
    monkeypatch.setattr("dpone.runtime.connectors.api.similarweb.get_default_manager", lambda: DummyVault())

    creds = SimilarwebCredentials.from_vault("api/similarweb")

    assert creds.api_key == "vault-sw-token"
    assert creds.endpoint == "https://api.similarweb.com"


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


def test_similarweb_connector_fetches_keywords_with_query_auth_and_pagination(similarweb_mock_server) -> None:
    server, requests_seen = similarweb_mock_server
    connector = SimilarwebConnector(
        credentials=SimilarwebCredentials(
            endpoint=f"http://127.0.0.1:{server.server_port}/v4",
            api_key="sw-token",
        )
    )

    rows = connector.fetch_resource_rows(
        resource_name="keywords",
        url="travel.example.com",
        start_date="2026-02-01",
        end_date="2026-02-28",
        limit=3,
        page_size=2,
        traffic_source="Organic",
        web_source="Total",
        branded_type="All",
        country="world",
        sort="traffic_share",
        asc=False,
    )

    assert len(rows) == 3
    assert rows[0]["keyword"] == "example_travel paris"
    assert rows[-1]["keyword"] == "example_travel rome"
    assert len(requests_seen) == 2
    first_params = requests_seen[0]["params"]
    assert first_params["api_key"] == "sw-token"
    assert first_params["URL"] == "travel.example.com"
    assert first_params["start_date"] == "2026-02"
    assert first_params["end_date"] == "2026-02"
    assert first_params["limit"] == "2"
    assert first_params["offset"] == "0"
    assert first_params["sort"] == "traffic_share"
    assert first_params["asc"] == "False"
    second_params = requests_seen[1]["params"]
    assert second_params["offset"] == "2"
    assert second_params["limit"] == "1"


def test_similarweb_connector_requires_single_month_window() -> None:
    with pytest.raises(ValueError, match="single month"):
        SimilarwebConnector._resolve_api_month("2026-02-01", "2026-03-01")


def test_similarweb_connector_health_check_uses_small_keywords_probe(monkeypatch) -> None:
    connector = SimilarwebConnector(
        credentials=SimilarwebCredentials(endpoint="https://api.similarweb.com", api_key="sw-token")
    )
    captured: dict[str, object] = {}

    def fake_fetch_resource_rows(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(connector, "fetch_resource_rows", fake_fetch_resource_rows)

    assert connector.health_check() is True
    assert captured["resource_name"] == "keywords"
    assert captured["url"] == "example.com"
    assert captured["limit"] == 1


def test_similarweb_unknown_resource_raises() -> None:
    with pytest.raises(KeyError):
        get_similarweb_resource("unknown")
