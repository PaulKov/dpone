from __future__ import annotations

import threading
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from dpone._compat import UTC
from dpone.runtime.connectors.api.openexchangerates import (
    DEFAULT_SYMBOLS,
    OpenExchangeRatesConnector,
    OpenExchangeRatesCredentials,
    normalize_openexchangerates_symbols,
    resolve_openexchangerates_date_option,
)
from dpone.runtime.connectors.api.openexchangerates_resources import (
    get_openexchangerates_resource,
    list_openexchangerates_resources,
    openexchangerates_table_name,
)


def test_openexchangerates_resource_registry_has_expected_resource() -> None:
    resources = {item.name: item for item in list_openexchangerates_resources()}
    assert set(resources) == {"historical_rates_daily"}
    assert resources["historical_rates_daily"].endpoint_path_template == "/historical/{as_of_date}.json"
    assert resources["historical_rates_daily"].unique_key == ("as_of_date", "symbol")
    assert openexchangerates_table_name("historical_rates_daily") == "default__historical_rates_daily"
    assert get_openexchangerates_resource("historical_rates_daily").name == "historical_rates_daily"


def test_openexchangerates_helpers_normalize_symbols_and_dates() -> None:
    assert normalize_openexchangerates_symbols("ars, rub,EUR") == DEFAULT_SYMBOLS
    assert normalize_openexchangerates_symbols(["usd", "eur", "usd"]) == ("USD", "EUR")
    assert resolve_openexchangerates_date_option("2026-03-30") == date(2026, 3, 30)
    assert resolve_openexchangerates_date_option(datetime(2026, 3, 30, 8, 0, 0)) == date(2026, 3, 30)


def test_openexchangerates_credentials_build_from_dict_and_vault(monkeypatch) -> None:
    creds = OpenExchangeRatesCredentials.from_dict(
        {
            "endpoint": "https://openexchangerates.org",
            "app_id": "app-1",
            "extra_headers": {"X-Test": "1"},
            "extra_params": {"show_bid_ask": "false"},
        }
    )
    assert creds.endpoint == "https://openexchangerates.org/api"
    assert creds.app_id == "app-1"
    assert creds.get_headers()["X-Test"] == "1"
    assert creds.extra_params == {"show_bid_ask": "false"}

    class DummyVault:
        def get_secret(self, mount_point: str, path: str):
            assert mount_point == "dev"
            assert path == "api/openexchangerates"
            return {
                "endpoint": "https://openexchangerates.org/api",
                "app_id": "vault-app-id",
            }

    monkeypatch.setattr("dpone.runtime.connectors.api.openexchangerates.get_env_code", lambda: "dev")
    monkeypatch.setattr("dpone.runtime.connectors.api.openexchangerates.get_default_manager", lambda: DummyVault())
    from_vault = OpenExchangeRatesCredentials.from_vault("api/openexchangerates")
    assert from_vault.app_id == "vault-app-id"


@pytest.fixture()
def openexchangerates_mock_server():
    requests_seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            requests_seen.append({"path": parsed.path, "params": {k: v[0] for k, v in params.items()}})

            if parsed.path == "/api/usage.json":
                body = b'{"status":"ok","data":{"plan":{"name":"developer"}}}'
            elif parsed.path == "/api/historical/2026-03-30.json":
                body = (
                    b'{"disclaimer":"usage terms","license":"https://license","timestamp":1774876800,'
                    b'"base":"USD","rates":{"ARS":123.45,"RUB":91.1,"EUR":0.92,"GBP":0.78}}'
                )
            elif parsed.path == "/api/historical/2026-03-29.json":
                body = (
                    b'{"disclaimer":"usage terms","license":"https://license","timestamp":1774790400,'
                    b'"base":"USD","rates":{"ARS":122.45,"RUB":90.1,"EUR":0.91}}'
                )
            else:
                self.send_response(404)
                self.end_headers()
                return

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


def test_openexchangerates_connector_fetches_historical_rows_and_health_check(openexchangerates_mock_server) -> None:
    server, requests_seen = openexchangerates_mock_server
    connector = OpenExchangeRatesConnector(
        credentials=OpenExchangeRatesCredentials(
            endpoint=f"http://127.0.0.1:{server.server_port}",
            app_id="app-id-1",
        ),
        timeout=10,
    )

    assert connector.health_check() is True
    rows = connector.fetch_historical_day(day=date(2026, 3, 30), symbols=("ARS", "RUB", "EUR"))
    assert len(rows) == 3
    assert rows[0]["as_of_date"] == "2026-03-30"
    assert rows[0]["base_currency"] == "USD"
    assert rows[0]["symbol"] == "ARS"
    assert rows[0]["rate"] == 123.45
    assert rows[0]["provider_timestamp_utc"] == datetime.fromtimestamp(1774876800, tz=UTC)
    assert requests_seen[0]["path"] == "/api/usage.json"
    assert requests_seen[1]["params"]["app_id"] == "app-id-1"
    assert requests_seen[1]["params"]["symbols"] == "ARS,RUB,EUR"


def test_openexchangerates_connector_get_resources_supports_ranges(openexchangerates_mock_server) -> None:
    server, _ = openexchangerates_mock_server
    connector = OpenExchangeRatesConnector(
        credentials=OpenExchangeRatesCredentials(
            endpoint=f"http://127.0.0.1:{server.server_port}",
            app_id="app-id-2",
        ),
        timeout=10,
    )

    rows = list(
        connector.get_resources(
            "historical_rates_daily",
            {
                "start_date": "2026-03-29",
                "end_date": "2026-03-30",
                "symbols": "ARS,RUB,EUR",
            },
        )
    )
    assert len(rows) == 6
    assert rows[0]["symbol"] == "ARS"
    assert rows[-1]["symbol"] == "EUR"
