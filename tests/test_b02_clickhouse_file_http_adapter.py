"""Real loopback HTTP proves framing, bounded response and cancellation semantics."""

import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from time import monotonic, sleep
from urllib.parse import parse_qs, urlsplit

import pytest

from dpone.runtime.clickhouse_file_stage_contract import ClickHouseFilePlan, IdentifiedStageQuery, QueryIdentity
from dpone.runtime.connectors.clickhouse_file_stage_http import ClickHouseFileHttpRunner
from dpone.runtime.connectors.clickhouse_http_bulk import ClickHouseHttpCredentials, ClickHouseHttpOptions


@pytest.fixture
def http_fixture():
    state = {"body": b"", "behavior": "success", "queries": []}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            query = parse_qs(urlsplit(self.path).query)["query"][0]
            state["queries"].append(query)
            data = bytearray()
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                size = int(line, 16)
                if not size:
                    self.rfile.readline()
                    break
                data.extend(self.rfile.read(size))
                self.rfile.read(2)
            body = b""
            if "system.databases" in query:
                body = (
                    json.dumps(
                        [
                            "11111111-1111-4111-8111-111111111111",
                            "sample",
                            "22222222-2222-4222-8222-222222222222",
                            "Atomic",
                        ]
                    ).encode()
                    + b"\n"
                )
            elif query.startswith("INSERT"):
                state["body"] = bytes(data)
                if state["behavior"] == "error":
                    body = b"Code: 27. DB::Exception: synthetic failure"
                if state["behavior"] == "oversized":
                    body = b"x" * 65537
                if state["behavior"] == "hang":
                    sleep(0.5)
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        runner = ClickHouseFileHttpRunner(
            ClickHouseHttpCredentials("127.0.0.1", server.server_port, "sample", "synthetic"),
            ClickHouseHttpOptions(input_format="RowBinary", timeout_seconds=2),
            clock=monotonic,
        )
        plan = ClickHouseFilePlan((("v", "int"),), (("v", "Int32"),), "http", "none", 2, "a" * 64, "sample", "rows")
        endpoint = runner.preflight(plan)
        request = IdentifiedStageQuery(
            QueryIdentity("dpone-b02-" + "a" * 32 + "-insert", "insert", endpoint),
            "INSERT INTO sample.stage FORMAT RowBinary",
        )
        yield runner, request, state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_http_exact_chunked_body_and_complete_ack(http_fixture):
    runner, request, state = http_fixture
    data = bytes(range(256)) * 1000
    result = runner.execute(request, chunks=[data[:13], data[13:]], deadline_monotonic=monotonic() + 2)
    assert state["body"] == data
    assert result.emitted_bytes == len(data)
    assert result.emitted_sha256 == hashlib.sha256(data).hexdigest()
    assert runner.local_stopped


@pytest.mark.parametrize("behavior", ["error", "oversized"])
def test_http_200_is_not_sufficient_for_success(http_fixture, behavior):
    runner, request, state = http_fixture
    state["behavior"] = behavior
    with pytest.raises(RuntimeError):
        runner.execute(request, chunks=[b"x"], deadline_monotonic=monotonic() + 2)
    assert runner.local_stopped
    assert runner.cancel_and_observe(request.identity, deadline_monotonic=monotonic() + 2).remote_state == "unknown"


def test_http_timeout_retains_remote_unknown(http_fixture):
    runner, request, state = http_fixture
    state["behavior"] = "hang"
    with pytest.raises(TimeoutError):
        runner.execute(request, chunks=[b"x"], deadline_monotonic=monotonic() + 0.1)
    assert runner.local_stopped
    assert runner.cancel_and_observe(request.identity, deadline_monotonic=monotonic() + 2).remote_state == "unknown"
