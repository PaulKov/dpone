from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors.api.openexchangerates import OpenExchangeRatesConnector, OpenExchangeRatesCredentials
from dpone.runtime.sources.api.openexchangerates import OpenExchangeRatesSource

pytestmark = [pytest.mark.integration]


class DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def log_etl_progress(self, *args, **kwargs):
        return None


class DummySinkConnector:
    def get_max_column_value(self, schema, table, column):
        del schema, table, column
        return None


@pytest.fixture()
def openexchangerates_mock_server():
    requests_seen: list[dict[str, object]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            requests_seen.append(
                {
                    "path": parsed.path,
                    "params": {key: values[0] for key, values in params.items()},
                }
            )
            if parsed.path == "/api/historical/2026-03-30.json":
                payload = {
                    "disclaimer": "usage terms",
                    "license": "https://license",
                    "timestamp": 1774876800,
                    "base": "USD",
                    "rates": {"ARS": 123.45, "RUB": 91.10, "EUR": 0.92},
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
        source_conn_id="api__openexchangerates",
        target_conn_id="bigquery-dwh",
        source_schema="default",
        source_table="historical_rates_daily",
        target_schema="landing__openexchangerates__api",
        target_table="default__historical_rates_daily",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        options={
            "resource": "historical_rates_daily",
            "start_date": "2026-03-30",
            "end_date": "2026-03-30",
            "symbols": ["ARS", "RUB", "EUR"],
        },
        unique_key=["as_of_date", "symbol"],
    )


def test_openexchangerates_mock_incremental_merge_historical_rates(openexchangerates_mock_server) -> None:
    server, requests_seen = openexchangerates_mock_server
    source = OpenExchangeRatesSource(
        connector=OpenExchangeRatesConnector(
            credentials=OpenExchangeRatesCredentials(
                endpoint=f"http://127.0.0.1:{server.server_port}",
                app_id="oxr-token",
            ),
            timeout=10,
        ),
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )

    result = source.extract(_load_config(), None)
    rows = result.artifact._rows

    assert len(rows) == 3
    assert rows[0]["symbol"] == "ARS"
    assert rows[0]["as_of_date"] == "2026-03-30"
    assert rows[1]["symbol"] == "RUB"
    assert requests_seen[0]["path"] == "/api/historical/2026-03-30.json"
    assert requests_seen[0]["params"]["app_id"] == "oxr-token"
    assert requests_seen[0]["params"]["symbols"] == "ARS,RUB,EUR"
