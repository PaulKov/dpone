from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors.api.google_sheets import GoogleSheetsConnector, GoogleSheetsCredentials
from dpone.runtime.sources.api.google_sheets import GoogleSheetsSource

pytestmark = [pytest.mark.integration]


class DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def log_etl_progress(self, *args, **kwargs):
        return None


@pytest.fixture()
def google_sheets_mock_server():
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

            if parsed.path == "/v4/spreadsheets/sheet-123":
                payload = {
                    "properties": {"title": "Mock Workbook"},
                    "sheets": [
                        {
                            "properties": {
                                "sheetId": 0,
                                "title": "Leads",
                                "index": 0,
                                "gridProperties": {"rowCount": 4, "columnCount": 2},
                            }
                        }
                    ],
                }
            elif parsed.path.startswith("/v4/spreadsheets/sheet-123/values/"):
                assert self.headers.get("Authorization") == "Bearer dummy-token"
                assert unquote(parsed.path).endswith("'Leads'!A1:B3")
                payload = {
                    "values": [
                        ["Order ID", "City"],
                        [1, "Paris"],
                        [2, "Berlin"],
                    ]
                }
            elif parsed.path == "/published.csv":
                body = b"Order ID,City\n1,Paris\n2,Berlin\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/csv")
                self.end_headers()
                self.wfile.write(body)
                return
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


def _load_config(**options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="api__google_sheets",
        target_conn_id="bigquery-dwh",
        source_schema="app",
        source_table="worksheet_rows",
        target_schema="landing__google_sheets__api",
        target_table="app__worksheet_rows",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "resource": "worksheet_rows",
            "spreadsheet_url": "https://docs.google.com/spreadsheets/d/sheet-123/edit#gid=0",
            "worksheet_index": 0,
            "range_name": "A1:B3",
            "header_row": 1,
            **options,
        },
    )


def test_google_sheets_mock_full_extract_hits_metadata_and_values(google_sheets_mock_server) -> None:
    server, requests_seen = google_sheets_mock_server
    connector = GoogleSheetsConnector(
        credentials=GoogleSheetsCredentials(
            endpoint=f"http://127.0.0.1:{server.server_port}",
            auth_type="service_account",
            credentials_json={
                "project_id": "proj",
                "private_key": "pk",
                "client_email": "svc@example.com",
            },
        )
    )
    connector._get_access_token = lambda: "dummy-token"  # type: ignore[attr-defined]

    resources = connector.get_resources(spreadsheet_url="https://docs.google.com/spreadsheets/d/sheet-123/edit#gid=0")
    assert resources[0]["worksheet_title"] == "Leads"

    source = GoogleSheetsSource(connector=connector, sink_connector=None, logger=DummyLogger())
    result = source.extract(_load_config(), None)
    rows = result.artifact._rows

    assert len(rows) == 2
    assert rows[0]["city"] == "Paris"
    assert rows[0]["_meta_worksheet_title"] == "Leads"
    assert len(requests_seen) >= 2
    assert requests_seen[0]["path"] == "/v4/spreadsheets/sheet-123"


def test_google_sheets_mock_published_csv_extract(google_sheets_mock_server) -> None:
    server, requests_seen = google_sheets_mock_server

    class LocalPublishedCSVConnector(GoogleSheetsConnector):
        def _download_published_csv(self, spreadsheet_id, worksheet_gid):  # noqa: ANN001
            del spreadsheet_id, worksheet_gid
            response = self.session.get(f"http://127.0.0.1:{server.server_port}/published.csv", timeout=self.timeout)
            response.raise_for_status()
            return [["Order ID", "City"], [1, "Paris"], [2, "Berlin"]]

    connector = LocalPublishedCSVConnector(
        credentials=GoogleSheetsCredentials(
            endpoint=f"http://127.0.0.1:{server.server_port}",
            auth_type="published_csv",
        )
    )
    source = GoogleSheetsSource(connector=connector, sink_connector=None, logger=DummyLogger())

    result = source.extract(
        _load_config(worksheet_gid=123, worksheet_title="gid_123", spreadsheet_id="sheet-123"),
        None,
    )

    assert len(result.artifact._rows) == 2
    assert result.artifact._rows[1]["city"] == "Berlin"
    assert any(item["path"] == "/published.csv" for item in requests_seen)
