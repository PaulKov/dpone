from __future__ import annotations

import gzip
import json
import threading
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from dpone._compat import UTC
from dpone.runtime.connectors.api.mindbox import MindboxConnector, MindboxCredentials
from dpone.runtime.connectors.api.mindbox_resources import (
    get_mindbox_resource,
    list_mindbox_resources,
    mindbox_table_name,
)


def test_mindbox_resource_registry_has_expected_resources() -> None:
    resources = {item.name: item for item in list_mindbox_resources()}
    assert set(resources) == {
        "getclients",
        "getorders",
        "getactions",
        "getmailings",
        "getmessagingreport",
        "operationslogs",
    }
    assert resources["getactions"].default_replace_column == "creationDateTimeUtc"
    assert resources["getclients"].window_mode == "datetime_utc"
    assert resources["getmailings"].window_mode == "datetime_utc"
    assert resources["getmessagingreport"].window_mode == "date_project"
    assert resources["getmessagingreport"].recommended_strategy == "full_refresh"
    assert mindbox_table_name("getclients") == "app__getclients"


def test_mindbox_credentials_from_dict_and_headers() -> None:
    creds = MindboxCredentials.from_dict(
        {
            "endpoint": "https://api.mindbox.ru",
            "api_key": "secret",
            "endpoint_id": "example.com",
        }
    )
    headers = creds.get_headers()
    assert headers["Authorization"] == 'Mindbox secretKey="secret"'
    assert headers["Content-Type"].startswith("application/json")
    assert creds.endpoint_id == "example.com"


def test_mindbox_credentials_accept_token_alias_in_config() -> None:
    creds = MindboxCredentials.from_dict(
        {
            "endpoint": "https://api.mindbox.ru",
            "token": "secret-token",
            "endpoint_id": "example.com",
        }
    )
    assert creds.api_key == "secret-token"
    assert creds.get_headers()["Authorization"] == 'Mindbox secretKey="secret-token"'


def test_mindbox_credentials_from_vault_accepts_token_alias(monkeypatch) -> None:
    class DummyVault:
        def get_secret(self, mount_point: str, path: str):
            assert mount_point == "dev"
            assert path == "api/mindbox"
            return {
                "endpoint": "https://api.mindbox.ru",
                "token": "vault-secret",
                "endpoint_id": "example.com",
            }

    monkeypatch.setattr("dpone.runtime.connectors.api.mindbox.get_env_code", lambda: "dev")
    monkeypatch.setattr("dpone.runtime.connectors.api.mindbox.get_default_manager", lambda: DummyVault())
    creds = MindboxCredentials.from_vault("api/mindbox")
    assert creds.api_key == "vault-secret"


def test_mindbox_credentials_require_token_or_api_key() -> None:
    with pytest.raises(KeyError, match="either 'token' or 'api_key'"):
        MindboxCredentials.from_dict({"endpoint": "https://api.mindbox.ru", "endpoint_id": "example.com"})


def test_mindbox_build_date_body_and_normalization() -> None:
    creds = MindboxCredentials(endpoint="https://api.mindbox.ru", api_key="secret", endpoint_id="example.com")
    connector = MindboxConnector(credentials=creds)
    body = connector.build_date_body(date(2026, 3, 1), date(2026, 3, 2), "datetime_utc", "21:00:00")
    assert body == {
        "sinceDateTimeUtc": "2026-03-01 21:00:00",
        "tillDateTimeUtc": "2026-03-02 21:00:00",
    }
    datetime_body = connector.build_date_body(
        datetime(2026, 3, 1, 9, 15, 33, tzinfo=UTC),
        datetime(2026, 3, 1, 10, 45, 59, tzinfo=UTC),
        "datetime_utc",
        "21:00:00",
    )
    assert datetime_body == {
        "sinceDateTimeUtc": "2026-03-01 09:15",
        "tillDateTimeUtc": "2026-03-01 10:45",
    }
    date_project_body = connector.build_date_body(
        datetime(2026, 3, 1, 22, 15, tzinfo=UTC),
        datetime(2026, 3, 2, 20, 45, tzinfo=UTC),
        "date_project",
        "21:00:00",
    )
    assert date_project_body == {
        "sinceDate": "2026-03-02",
        "tillDate": "2026-03-02",
    }
    normalized = connector._normalize_record(
        {
            "id": 1,
            "nested": {"flag": True},
            "items": [1, 2],
            "price": 12.5,
        }
    )
    assert normalized["nested_flag"] is True
    assert normalized["items"] == "[1, 2]"
    assert normalized["price"] == 12.5


@pytest.fixture()
def mindbox_mock_server():
    posts: list[dict[str, object]] = []
    gets: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            body_raw = self.rfile.read(int(self.headers.get("Content-Length", "0") or "0"))
            body = json.loads(body_raw.decode("utf-8") or "{}")
            posts.append(
                {
                    "path": self.path,
                    "operation": params.get("operation", [""])[0],
                    "body": body,
                    "auth": self.headers.get("Authorization"),
                }
            )
            op = params.get("operation", [""])[0]
            if "exportId" not in body:
                export_id = f"exp-{op}"
                payload = {"exportId": export_id}
            else:
                export_id = body["exportId"]
                server_port = getattr(self.server, "server_port")
                if export_id == "exp-GetClients":
                    urls = [f"http://127.0.0.1:{server_port}/files/getclients.json"]
                elif export_id == "exp-GetMessagingReport":
                    urls = [f"http://127.0.0.1:{server_port}/files/getmessagingreport.csv.gz"]
                else:
                    urls = []
                payload: dict[str, object] = {"exportResult": {"processingStatus": "Ready", "urls": urls}}
            response = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(response)

        def do_GET(self):  # noqa: N802
            gets.append(self.path)
            if self.path.endswith("getclients.json"):
                payload = {
                    "customers": [
                        {"id": 1, "createdAt": "2026-03-10T10:00:00Z", "nested": {"flag": True}},
                        {"id": 2, "createdAt": "2026-03-10T11:00:00Z", "tags": ["a", "b"]},
                    ]
                }
                data = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data)
                return
            if self.path.endswith("getmessagingreport.csv.gz"):
                csv_data = b"messageId;sentAt\n1;2026-03-10 10:00:00\n2;2026-03-10 11:00:00\n"
                gz_data = gzip.compress(csv_data)
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.end_headers()
                self.wfile.write(gz_data)
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, format, *args):  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, posts, gets
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_mindbox_connector_mock_json_and_csv_export(mindbox_mock_server) -> None:
    server, posts, gets = mindbox_mock_server
    connector = MindboxConnector(
        credentials=MindboxCredentials(
            endpoint=f"http://127.0.0.1:{server.server_port}",
            api_key="token",
            endpoint_id="example.com",
        ),
    )
    rows = list(
        connector.get_resources(
            "getclients",
            {
                "since": date(2026, 3, 1),
                "poll_interval": 0,
                "export_timeout": 1,
            },
        )
    )
    assert len(rows) == 2
    assert rows[0]["nested_flag"] is True
    assert rows[1]["tags"] == '["a", "b"]'
    assert posts[0]["auth"] == 'Mindbox secretKey="token"'
    assert posts[0]["body"]["sinceDateTimeUtc"] == "2026-03-01 21:00:00"
    assert "tillDateTimeUtc" not in posts[0]["body"]
    assert any(path.endswith("getclients.json") for path in gets)

    csv_rows = list(
        connector.get_resources(
            "getmessagingreport",
            {
                "since": date(2026, 3, 1),
                "till": date(2026, 3, 2),
                "poll_interval": 0,
                "export_timeout": 1,
            },
        )
    )
    assert len(csv_rows) == 2
    assert csv_rows[0]["messageId"] == "1"
    assert posts[2]["body"] == {"sinceDate": "2026-03-01", "tillDate": "2026-03-02"}


def test_mindbox_connector_health_check_and_unknown_resource(mindbox_mock_server) -> None:
    server, _, _ = mindbox_mock_server
    connector = MindboxConnector(
        credentials=MindboxCredentials(
            endpoint=f"http://127.0.0.1:{server.server_port}",
            api_key="token",
            endpoint_id="example.com",
        ),
    )
    assert connector.health_check() is True
    with pytest.raises(KeyError):
        get_mindbox_resource("unknown")
