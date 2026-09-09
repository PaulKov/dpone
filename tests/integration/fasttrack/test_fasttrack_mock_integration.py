from __future__ import annotations

import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors.api.fasttrack import FasttrackConnector, FasttrackCredentials
from dpone.runtime.sources.api.fasttrack import FasttrackSource

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
        return None


@pytest.fixture()
def fasttrack_server():
    requests_seen: list[tuple[str, str, str]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/partners/reports/"):
                params = parse_qs(parsed.query)
                report_type = (params.get("type") or [""])[0]
                requests_seen.append((self.path, self.headers.get("bot-key", ""), report_type))
                if report_type == "SESSIONS":
                    body = "UUID пользователя;Дата начала;Имя пользователя\nchat-1;12-03-2026 10:15:00;Иван\n"
                else:
                    body = "transaction_uuid;created_at;done_at\ntx-1;2026-03-12 10:00:00;2026-03-12 11:00:00\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/csv")
                self.end_headers()
                self.wfile.write(body.encode("utf-8"))
                return
            if parsed.path.startswith("/flex-api/ticket/"):
                requests_seen.append((self.path, self.headers.get("X-Token", ""), "flex"))
                body = '{"results": [{"id": 1, "created": "2026-03-12T10:00:00+03:00", "modified": "2026-03-12T11:00:00+03:00", "score": 5}]}'
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


def _connector_for(server_port: int) -> FasttrackConnector:
    creds = FasttrackCredentials.from_dict(
        {
            "dashboard_endpoint": f"http://127.0.0.1:{server_port}/api/partners",
            "dashboard_bot_key": "dashboard-token",
            "flex_endpoint": f"http://127.0.0.1:{server_port}/flex-api",
            "flex_x_token": "flex-token",
        }
    )
    return FasttrackConnector(credentials=creds)


def _load_config(resource: str, *, load_strategy: LoadStrategy, **options) -> LoadConfig:
    return LoadConfig(
        source_conn_id="api__fasttrack",
        target_conn_id="bigquery-dwh",
        source_schema="default",
        source_table=resource,
        target_schema="landing__fasttrack__api",
        target_table=f"default__{resource}",
        load_strategy=load_strategy,
        options={"resource": resource, **options},
    )


def test_fasttrack_mock_incremental_merge_dashboard_resource(fasttrack_server) -> None:
    server, requests_seen = fasttrack_server
    source = FasttrackSource(
        connector=_connector_for(server.server_port),
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    result = source.extract(
        _load_config(
            "cascade_transactions", load_strategy=LoadStrategy.INCREMENTAL_MERGE, unique_key=["transaction_uuid"]
        ),
        None,
    )
    row = next(result.artifact._iterator)
    assert row["transaction_uuid"] == "tx-1"
    assert any(kind == "CASCADE_TRANSACTIONS" and token == "dashboard-token" for _path, token, kind in requests_seen)


def test_fasttrack_mock_incremental_merge_chat_sessions_keeps_sanitized_raw_columns(fasttrack_server) -> None:
    server, _requests_seen = fasttrack_server
    source = FasttrackSource(
        connector=_connector_for(server.server_port),
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    result = source.extract(
        _load_config(
            "chat_sessions",
            load_strategy=LoadStrategy.INCREMENTAL_MERGE,
            unique_key=["uuid_polzovatelya", "data_nachala"],
        ),
        None,
    )
    row = next(result.artifact._iterator)
    assert row["uuid_polzovatelya"] == "chat-1"
    assert row["imya_polzovatelya"] == "Иван"
    assert isinstance(row["data_nachala"], datetime)
    assert "UUID пользователя" not in row
    assert "chat_uuid" not in row


def test_fasttrack_mock_full_refresh_flex_resource(fasttrack_server) -> None:
    server, requests_seen = fasttrack_server
    source = FasttrackSource(
        connector=_connector_for(server.server_port),
        sink_connector=DummySinkConnector(),
        logger=DummyLogger(),
    )
    result = source.extract(_load_config("flex_cms_ratings", load_strategy=LoadStrategy.FULL_REFRESH), None)
    row = next(result.artifact._iterator)
    assert row["id"] == 1
    assert row["score"] == 5
    assert any(kind == "flex" and token == "flex-token" for _path, token, kind in requests_seen)
