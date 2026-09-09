from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from dpone.runtime.connectors.api.fasttrack import FasttrackConnector, FasttrackCredentials
from dpone.runtime.connectors.api.fasttrack_resources import (
    fasttrack_table_name,
    get_fasttrack_resource,
    list_fasttrack_resources,
)


def test_fasttrack_resource_registry_has_expected_resources() -> None:
    resources = {item.name: item for item in list_fasttrack_resources()}
    assert set(resources) == {"cascade_transactions", "chat_sessions", "flex_cms_ratings"}
    assert resources["cascade_transactions"].transport == "dashboard_report"
    assert resources["flex_cms_ratings"].transport == "flex_ticket"
    assert resources["cascade_transactions"].default_load_strategy == "incremental_merge"
    assert resources["chat_sessions"].default_load_strategy == "incremental_merge"
    assert fasttrack_table_name("chat_sessions") == "default__chat_sessions"


def test_fasttrack_credentials_from_dict_supports_single_generic_api_key() -> None:
    creds = FasttrackCredentials.from_dict(
        {
            "endpoint": "https://dashboard.fstrk.io/api/partners",
            "api_key": "shared-token",
            "flex_endpoint": "https://example.flex.fstrk.io/api",
        }
    )
    assert creds.dashboard_bot_key == "shared-token"
    assert creds.flex_x_token == "shared-token"
    assert "Accept" not in creds.get_dashboard_headers()
    assert creds.get_dashboard_headers()["bot-key"] == "shared-token"
    assert creds.get_flex_headers()["X-Token"] == "shared-token"


def test_fasttrack_credentials_from_dict_prefers_token_over_api_key() -> None:
    creds = FasttrackCredentials.from_dict(
        {
            "endpoint": "https://dashboard.fstrk.io/api/partners",
            "token": "preferred-token",
            "api_key": "legacy-token",
            "flex_endpoint": "https://example.flex.fstrk.io/api",
        }
    )
    assert creds.api_key == "preferred-token"
    assert creds.dashboard_bot_key == "preferred-token"
    assert creds.flex_x_token == "preferred-token"


def test_fasttrack_credentials_from_vault(monkeypatch) -> None:
    class DummyVault:
        def get_secret(self, mount_point: str, path: str):
            assert mount_point == "dev"
            assert path == "api/fasttrack"
            return {
                "dashboard_endpoint": "https://dashboard.fstrk.io/api/partners",
                "dashboard_bot_key": "dashboard-secret",
                "flex_endpoint": "https://example.flex.fstrk.io/api",
                "flex_x_token": "flex-secret",
            }

    monkeypatch.setattr("dpone.runtime.connectors.api.fasttrack.get_env_code", lambda: "dev")
    monkeypatch.setattr("dpone.runtime.connectors.api.fasttrack.get_default_manager", lambda: DummyVault())
    creds = FasttrackCredentials.from_vault("api/fasttrack")
    assert creds.dashboard_bot_key == "dashboard-secret"
    assert creds.flex_x_token == "flex-secret"


def test_fasttrack_explicit_transport_keys_override_generic_token() -> None:
    creds = FasttrackCredentials.from_dict(
        {
            "endpoint": "https://dashboard.fstrk.io/api/partners",
            "token": "shared-token",
            "dashboard_bot_key": "dashboard-secret",
            "flex_endpoint": "https://example.flex.fstrk.io/api",
            "flex_x_token": "flex-secret",
        }
    )
    assert creds.api_key == "shared-token"
    assert creds.dashboard_bot_key == "dashboard-secret"
    assert creds.flex_x_token == "flex-secret"


def test_fasttrack_unknown_resource_raises() -> None:
    with pytest.raises(KeyError):
        get_fasttrack_resource("unknown")


def test_fasttrack_parse_dashboard_csv_rows_preserves_vendor_columns() -> None:
    rows = FasttrackConnector._parse_dashboard_csv_rows(
        "transaction_uuid;created_at;phone_number\nabc;2026-03-10 10:00:00;+79990000000\n"
    )
    assert rows == [
        {
            "transaction_uuid": "abc",
            "created_at": "2026-03-10 10:00:00",
            "phone_number": "+79990000000",
        }
    ]


def test_fasttrack_decode_dashboard_content_supports_gzip_payload() -> None:
    class DummyResponse:
        content = __import__("gzip").compress(b"col_a;col_b\n1;2\n")
        text = ""

    assert FasttrackConnector._decode_dashboard_content(DummyResponse()) == "col_a;col_b\n1;2\n"


@pytest.fixture()
def fasttrack_mock_server():
    requests_seen: list[tuple[str, str, str]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path.startswith("/api/partners/reports/"):
                requests_seen.append((self.path, self.headers.get("bot-key", ""), "dashboard"))
                body = "transaction_uuid;created_at;phone_number\nabc;2026-03-10 10:00:00;+79990000000\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/csv")
                self.end_headers()
                self.wfile.write(body.encode("utf-8"))
                return
            if self.path.startswith("/flex-api/ticket/"):
                requests_seen.append((self.path, self.headers.get("X-Token", ""), "flex"))
                body = '{"results": [{"id": 1, "created": "2026-03-10T10:00:00+03:00"}]}'
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body.encode("utf-8"))
                return
            self.send_response(404)
            self.end_headers()

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


def test_fasttrack_connector_mock_integration_dashboard_and_flex(fasttrack_mock_server) -> None:
    server, requests_seen = fasttrack_mock_server
    creds = FasttrackCredentials.from_dict(
        {
            "dashboard_endpoint": f"http://127.0.0.1:{server.server_port}/api/partners",
            "dashboard_bot_key": "dashboard-token",
            "flex_endpoint": f"http://127.0.0.1:{server.server_port}/flex-api",
            "flex_x_token": "flex-token",
        }
    )
    connector = FasttrackConnector(credentials=creds)

    dashboard_rows = list(connector.get_resources("cascade_transactions"))
    flex_rows = list(connector.get_resources("flex_cms_ratings"))

    assert dashboard_rows[0]["transaction_uuid"] == "abc"
    assert flex_rows[0]["id"] == 1
    assert any(
        path.startswith("/api/partners/reports/") and token == "dashboard-token"
        for path, token, kind in requests_seen
        if kind == "dashboard"
    )
    assert any(
        path.startswith("/flex-api/ticket/") and token == "flex-token"
        for path, token, kind in requests_seen
        if kind == "flex"
    )


def test_fasttrack_connector_follows_flex_pagination() -> None:
    requests_seen: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            requests_seen.append(self.path)
            if self.path.startswith("/api/ticket/?category=test-category&page_size=1000&page=2"):
                body = (
                    '{"count": 2, "next": "", "previous": "/api/ticket/?category=test-category&page_size=1000", '
                    '"results": [{"id": 2, "created": "2026-03-11T10:00:00+03:00"}]}'
                )
            elif self.path.startswith("/flex-api/ticket/?category=test-category&page_size=1000"):
                body = (
                    '{"count": 2, "next": "/api/ticket/?category=test-category&page_size=1000&page=2", '
                    '"previous": "", "results": [{"id": 1, "created": "2026-03-10T10:00:00+03:00"}]}'
                )
            else:
                self.send_response(404)
                self.end_headers()
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

        def log_message(self, format, *args):  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        creds = FasttrackCredentials.from_dict(
            {
                "flex_endpoint": f"http://127.0.0.1:{server.server_port}/flex-api",
                "flex_x_token": "flex-token",
            }
        )
        connector = FasttrackConnector(credentials=creds)
        rows = list(connector.get_resources("flex_cms_ratings", {"category": "test-category", "page_size": 1000}))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert [row["id"] for row in rows] == [1, 2]
    assert requests_seen == [
        "/flex-api/ticket/?category=test-category&page_size=1000",
        "/api/ticket/?category=test-category&page_size=1000&page=2",
    ]


def test_fasttrack_connector_health_check_uses_dashboard_transport(monkeypatch) -> None:
    creds = FasttrackCredentials.from_dict(
        {
            "dashboard_endpoint": "https://dashboard.fstrk.io/api/partners",
            "dashboard_bot_key": "token",
        }
    )
    connector = FasttrackConnector(credentials=creds)
    monkeypatch.setattr(connector, "fetch_resource_rows", lambda *args, **kwargs: [])
    assert connector.health_check() is True
