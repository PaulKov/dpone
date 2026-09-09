from __future__ import annotations

import threading
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest
import requests

from dpone.runtime.connectors.api.appsflyer import (
    AppsflyerConnector,
    AppsflyerCredentials,
    AppsflyerQuotaExceededError,
)
from dpone.runtime.connectors.api.appsflyer_resources import (
    appsflyer_table_name,
    get_appsflyer_resource,
    list_appsflyer_resources,
)


def test_appsflyer_resource_registry_has_expected_resources() -> None:
    resources = {item.name: item for item in list_appsflyer_resources()}
    assert set(resources) == {
        "daily_report",
        "installs_report",
        "in_app_events_report",
        "uninstall_events_report",
        "installs_retarget",
        "in_app_events_retarget",
        "organic_installs_report",
        "organic_in_app_events_report",
        "organic_uninstall_events_report",
    }
    assert resources["daily_report"].endpoint_base_path == "/api/agg-data/export/app"
    assert resources["daily_report"].supports_maximum_rows is False
    assert resources["daily_report"].supports_timezone is False
    assert resources["daily_report"].uses_date_only_window is True
    assert resources["uninstall_events_report"].default_lookback_days == 14
    assert resources["installs_report"].endpoint_path == "installs_report/v5"
    assert appsflyer_table_name("installs_report") == "app__installs_report"


def test_appsflyer_credentials_build_headers_and_params() -> None:
    creds = AppsflyerCredentials.from_dict(
        {
            "endpoint": "https://hq1.appsflyer.com/api/raw-data/export/app",
            "api_key": "token-1",
            "extra_headers": {"Accept": "text/csv"},
            "extra_params": {"timezone": "Europe/Moscow"},
        }
    )
    headers = creds.get_headers()
    assert headers["Authorization"] == "Bearer token-1"
    assert headers["Accept"] == "text/csv"
    assert creds.extra_params == {"timezone": "Europe/Moscow"}


def test_appsflyer_credentials_accept_token_alias_in_config() -> None:
    creds = AppsflyerCredentials.from_dict(
        {
            "endpoint": "https://hq1.appsflyer.com/api/raw-data/export/app",
            "token": "token-2",
        }
    )
    assert creds.api_key == "token-2"
    assert creds.get_headers()["Authorization"] == "Bearer token-2"


def test_appsflyer_credentials_normalize_short_api_base_in_config() -> None:
    creds = AppsflyerCredentials.from_dict(
        {
            "endpoint": "https://hq1.appsflyer.com/api",
            "token": "token-3",
        }
    )
    assert creds.endpoint == "https://hq1.appsflyer.com/api/raw-data/export/app"


def test_appsflyer_credentials_from_vault(monkeypatch) -> None:
    class DummyVault:
        def get_secret(self, mount_point: str, path: str):
            assert mount_point == "dev"
            assert path == "api/appsflyer"
            return {
                "endpoint": "https://hq1.appsflyer.com/api/raw-data/export/app",
                "api_key": "vault-token",
                "extra_headers": {"X-Test": "1"},
                "extra_params": {"timezone": "Europe/Moscow"},
            }

    monkeypatch.setattr("dpone.runtime.connectors.api.appsflyer.get_env_code", lambda: "dev")
    monkeypatch.setattr("dpone.runtime.connectors.api.appsflyer.get_default_manager", lambda: DummyVault())
    creds = AppsflyerCredentials.from_vault("api/appsflyer")
    assert creds.api_key == "vault-token"
    assert creds.extra_headers["X-Test"] == "1"
    assert creds.extra_params["timezone"] == "Europe/Moscow"


def test_appsflyer_credentials_from_vault_accepts_token_alias(monkeypatch) -> None:
    class DummyVault:
        def get_secret(self, mount_point: str, path: str):
            assert mount_point == "dev"
            assert path == "api/appsflyer"
            return {
                "endpoint": "https://hq1.appsflyer.com/api/raw-data/export/app",
                "token": "vault-token-2",
            }

    monkeypatch.setattr("dpone.runtime.connectors.api.appsflyer.get_env_code", lambda: "dev")
    monkeypatch.setattr("dpone.runtime.connectors.api.appsflyer.get_default_manager", lambda: DummyVault())
    creds = AppsflyerCredentials.from_vault("api/appsflyer")
    assert creds.api_key == "vault-token-2"


def test_appsflyer_credentials_from_vault_normalize_short_api_base(monkeypatch) -> None:
    class DummyVault:
        def get_secret(self, mount_point: str, path: str):
            assert mount_point == "dev"
            assert path == "api/appsflyer"
            return {
                "endpoint": "https://hq1.appsflyer.com/api",
                "token": "vault-token-3",
            }

    monkeypatch.setattr("dpone.runtime.connectors.api.appsflyer.get_env_code", lambda: "dev")
    monkeypatch.setattr("dpone.runtime.connectors.api.appsflyer.get_default_manager", lambda: DummyVault())
    creds = AppsflyerCredentials.from_vault("api/appsflyer")
    assert creds.endpoint == "https://hq1.appsflyer.com/api/raw-data/export/app"
    assert creds.api_key == "vault-token-3"


def test_appsflyer_credentials_require_token_or_api_key() -> None:
    with pytest.raises(KeyError, match="either 'token' or 'api_key'"):
        AppsflyerCredentials.from_dict({"endpoint": "https://hq1.appsflyer.com/api/raw-data/export/app"})


def test_appsflyer_connector_parses_csv_and_adds_metadata() -> None:
    creds = AppsflyerCredentials(
        endpoint="https://hq1.appsflyer.com/api/raw-data/export/app",
        api_key="token",
        extra_params={"timezone": "Europe/Moscow"},
    )
    connector = AppsflyerConnector(credentials=creds, default_app_id="com.example.travel")
    rows = connector._parse_csv_rows(
        """Event Time,Media Source
2026-03-10 10:00:00,googleadwords_int
""",
        app_id="com.example.travel",
        resource_name="installs_report",
        start_dt=connector._coerce_from_value("2026-03-10"),
    )
    assert rows == [
        {
            "event_time": "2026-03-10 10:00:00",
            "media_source": "googleadwords_int",
            "source_app_id": "com.example.travel",
            "resource_name": "installs_report",
            "date": "2026-03-10",
        }
    ]


def test_appsflyer_connector_preserves_date_column_for_daily_report() -> None:
    creds = AppsflyerCredentials(
        endpoint="https://hq1.appsflyer.com/api/raw-data/export/app",
        api_key="token",
    )
    connector = AppsflyerConnector(credentials=creds, default_app_id="id1234567890")
    rows = connector._parse_csv_rows(
        """Date,Installs\n2026-03-10,42\n""",
        app_id="id1234567890",
        resource_name="daily_report",
        start_dt=connector._coerce_from_value("2026-03-01"),
    )
    assert rows == [
        {
            "date": "2026-03-10",
            "installs": "42",
            "source_app_id": "id1234567890",
            "resource_name": "daily_report",
        }
    ]


def test_appsflyer_connector_unknown_resource_raises() -> None:
    with pytest.raises(KeyError):
        get_appsflyer_resource("unknown")


def test_appsflyer_connector_splits_window_on_row_limit(monkeypatch) -> None:
    creds = AppsflyerCredentials(endpoint="https://hq1.appsflyer.com/api/raw-data/export/app", api_key="token")
    connector = AppsflyerConnector(credentials=creds, default_app_id="app1", row_limit_split_ratio=1.0)
    calls: list[tuple[str, str]] = []

    def fake_fetch_rows_for_window(*, spec, app_id, start_dt, end_dt, timezone_name, maximum_rows, extra_params):
        calls.append((connector._format_datetime(start_dt), connector._format_datetime(end_dt)))
        if start_dt.date() == date(2026, 3, 1) and end_dt.date() == date(2026, 3, 2):
            return [
                {
                    "event_time": "2026-03-01 00:00:00",
                    "source_app_id": app_id,
                    "resource_name": spec.name,
                    "date": "2026-03-01",
                },
                {
                    "event_time": "2026-03-01 12:00:00",
                    "source_app_id": app_id,
                    "resource_name": spec.name,
                    "date": "2026-03-01",
                },
            ]
        return [
            {
                "event_time": "2026-03-01 00:00:00",
                "source_app_id": app_id,
                "resource_name": spec.name,
                "date": "2026-03-01",
            }
        ]

    monkeypatch.setattr(connector, "_fetch_rows_for_window", fake_fetch_rows_for_window)
    rows = list(
        connector.iter_resource_rows(
            resource_name="installs_report",
            app_id="app1",
            from_value="2026-03-01",
            to_value="2026-03-02",
            maximum_rows=2,
        )
    )
    assert len(rows) == 2
    assert len(calls) == 3  # initial + two split windows


def test_appsflyer_connector_health_check_uses_default_app_id(monkeypatch) -> None:
    creds = AppsflyerCredentials(endpoint="https://hq1.appsflyer.com/api/raw-data/export/app", api_key="token")
    connector = AppsflyerConnector(credentials=creds, default_app_id="app1")
    captured = {}

    def fake_fetch_resource_rows(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(connector, "fetch_resource_rows", fake_fetch_resource_rows)
    assert connector.health_check() is True
    assert captured["maximum_rows"] == 1000


def test_appsflyer_connector_health_check_returns_false_on_quota_exceeded(monkeypatch) -> None:
    creds = AppsflyerCredentials(endpoint="https://hq1.appsflyer.com/api/raw-data/export/app", api_key="token")
    connector = AppsflyerConnector(credentials=creds, default_app_id="app1")

    def fake_fetch_resource_rows(**kwargs):
        del kwargs
        raise AppsflyerQuotaExceededError(
            "AppsFlyer daily install-report quota is exhausted for the current app/token."
        )

    monkeypatch.setattr(connector, "fetch_resource_rows", fake_fetch_resource_rows)
    assert connector.health_check() is False


def test_appsflyer_connector_maps_known_quota_http_error_to_specific_exception(monkeypatch) -> None:
    creds = AppsflyerCredentials(endpoint="https://hq1.appsflyer.com/api/raw-data/export/app", api_key="token")
    connector = AppsflyerConnector(credentials=creds, default_app_id="app1")

    class DummyResponse:
        status_code = 400
        text = "You've reached your maximum number of install reports that can be downloaded today for this app."

    def fake_request(*args, **kwargs):
        del args, kwargs
        raise requests.HTTPError("quota", response=DummyResponse())

    monkeypatch.setattr(connector, "_request", fake_request)

    with pytest.raises(AppsflyerQuotaExceededError, match="quota is exhausted"):
        connector.fetch_resource_rows(
            resource_name="installs_report",
            app_id="app1",
            from_value="2026-03-01",
            to_value="2026-03-01",
            maximum_rows=1000,
        )


def test_appsflyer_connector_maps_daily_report_403_quota_to_specific_exception(monkeypatch) -> None:
    creds = AppsflyerCredentials(endpoint="https://hq1.appsflyer.com/api/raw-data/export/app", api_key="token")
    connector = AppsflyerConnector(credentials=creds, default_app_id="app1")

    class DummyResponse:
        status_code = 403
        text = "Limit reached for daily-report"

    def fake_request(*args, **kwargs):
        del args, kwargs
        raise requests.HTTPError("quota", response=DummyResponse())

    monkeypatch.setattr(connector, "_request", fake_request)

    with pytest.raises(AppsflyerQuotaExceededError, match="Limit reached for daily-report"):
        connector.fetch_resource_rows(
            resource_name="daily_report",
            app_id="app1",
            from_value="2026-03-01",
            to_value="2026-03-01",
            maximum_rows=1000,
        )


@pytest.fixture()
def appsflyer_mock_server():
    requests_seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            requests_seen.append(self.path)
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            if "/daily_report/v5" in parsed.path:
                assert "maximum_rows" not in params
                assert "timezone" not in params
                assert params["from"] == ["2026-03-10"]
                assert params["to"] == ["2026-03-10"]
            else:
                assert params["maximum_rows"] == ["1000000"]
            body = """event_time,media_source
2026-03-10 10:00:00,googleadwords_int
"""
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

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


def test_appsflyer_connector_mock_integration(appsflyer_mock_server) -> None:
    server, requests_seen = appsflyer_mock_server
    creds = AppsflyerCredentials(
        endpoint=f"http://127.0.0.1:{server.server_port}/api/raw-data/export/app",
        api_key="token",
        extra_params={"timezone": "Europe/Moscow"},
    )
    connector = AppsflyerConnector(credentials=creds, default_app_id="com.example.travel")
    rows = list(
        connector.get_resources(
            "installs_report",
            {
                "app_id": "com.example.travel",
                "from": "2026-03-10",
                "to": "2026-03-10",
            },
        )
    )
    assert len(rows) == 1
    assert rows[0]["source_app_id"] == "com.example.travel"
    assert rows[0]["resource_name"] == "installs_report"
    assert rows[0]["date"] == "2026-03-10"
    assert any("/com.example.travel/installs_report/v5" in path for path in requests_seen)


def test_appsflyer_connector_daily_report_uses_aggregate_endpoint_without_raw_only_params(
    appsflyer_mock_server,
) -> None:
    server, requests_seen = appsflyer_mock_server
    creds = AppsflyerCredentials(
        endpoint=f"http://127.0.0.1:{server.server_port}/api/raw-data/export/app",
        api_key="token",
        extra_params={"timezone": "Europe/Moscow"},
    )
    connector = AppsflyerConnector(credentials=creds, default_app_id="id1234567890")
    rows = list(
        connector.get_resources(
            "daily_report",
            {
                "app_id": "id1234567890",
                "from": "2026-03-10",
                "to": "2026-03-10",
            },
        )
    )
    assert len(rows) == 1
    assert rows[0]["resource_name"] == "daily_report"
    assert rows[0]["date"] == "2026-03-10"
    assert any("/api/agg-data/export/app/id1234567890/daily_report/v5" in path for path in requests_seen)
    assert all("maximum_rows=" not in path for path in requests_seen)
