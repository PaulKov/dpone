"""Real loopback HTTP proves framing, bounded response and cancellation semantics."""

import hashlib
import http.client
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
    state = {"body": b"", "behavior": "success", "queries": [], "raw_response": None, "close_failure": False}

    class Connection(http.client.HTTPConnection):
        """Constructor-injected close fault with an actual loopback socket."""

        def close(self):
            super().close()
            if state["close_failure"]:
                raise OSError("synthetic_close_failure")

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
            elif state["raw_response"] is not None:
                state["body"] = bytes(data)
                self.close_connection = True
                try:
                    for segment, delay in state["raw_response"]:
                        self.connection.sendall(segment)
                        if delay:
                            sleep(delay)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return
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
    thread = Thread(target=lambda: server.serve_forever(poll_interval=0.02), daemon=True)
    thread.start()
    try:
        runner = ClickHouseFileHttpRunner(
            ClickHouseHttpCredentials("127.0.0.1", server.server_port, "sample", "synthetic"),
            ClickHouseHttpOptions(input_format="RowBinary", timeout_seconds=2),
            clock=monotonic,
            connection_factory=Connection,
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


_OK = b"HTTP/1.1 200 OK\r\n"
_CHUNKED = _OK + b"Transfer-Encoding: chunked\r\n\r\n"
_TRICKLES = {
    "status": (b"HTTP/1.1 200 ", b" " * 30, b"\r\nContent-Length: 0\r\n\r\n"),
    "headers": (_OK + b"X-Slow: ", b" " * 30, b"\r\nContent-Length: 0\r\n\r\n"),
    "fixed_body": (_OK + b"Content-Length: 30\r\n\r\n", b" " * 30, b""),
    "chunk_size": (_CHUNKED + b"0;", b"x" * 30, b"\r\n\r\n"),
    "chunk_body": (_CHUNKED + b"1e\r\n", b" " * 30, b"\r\n0\r\n\r\n"),
    "trailer": (_CHUNKED + b"0\r\nX-Slow: ", b" " * 30, b"\r\n\r\n"),
}


@pytest.mark.parametrize("phase", _TRICKLES)
def test_http_absolute_deadline_bounds_every_response_phase(http_fixture, phase):
    runner, request, state = http_fixture
    prefix, slow, suffix = _TRICKLES[phase]
    state["raw_response"] = [(prefix, 0), *((bytes([byte]), 0.02) for byte in slow), (suffix, 0)]
    start = monotonic()
    with pytest.raises(TimeoutError):
        runner.execute(request, chunks=[b"data"], deadline_monotonic=start + 0.12)
    assert monotonic() - start < 0.4, "relative receive timeouts must not extend the absolute deadline"
    assert runner.local_stopped


def test_http_trickled_kill_response_is_bounded_and_remains_unknown(http_fixture):
    runner, request, state = http_fixture
    prefix, slow, suffix = _TRICKLES["headers"]
    state["raw_response"] = [(prefix, 0), *((bytes([byte]), 0.02) for byte in slow), (suffix, 0)]
    start = monotonic()
    observation = runner.cancel_and_observe(request.identity, deadline_monotonic=start + 0.12)
    assert monotonic() - start < 0.4
    assert observation.local_state == "stopped"
    assert observation.remote_state == "unknown"
    assert state["queries"][-1] == (
        f"KILL QUERY WHERE query_id = '{request.identity.query_id}' SYNC FORMAT JSONCompactEachRow"
    )


@pytest.mark.parametrize(
    "wire",
    [
        _OK + b"Content-Length: 32\r\n\r\n",
        _OK + b"Content-Length: 32\r\n\r\n  ",
        _CHUNKED + b"2\r\n ",
        _CHUNKED + b"1\r\n ",
        _CHUNKED + b"1\r\n XX0\r\n\r\n",
        _CHUNKED + b"-1\r\n",
        _CHUNKED + b"+1\r\n \r\n0\r\n\r\n",
        _CHUNKED + b"0\r\n",
        _CHUNKED + b"0\r\nX-Partial: yes\r\n",
        _CHUNKED + b"0\r\nX-Partial: yes",
        _OK + b"Content-Length: 0\r\n",
        _OK + b"Content-Length: -1\r\n\r\n",
        _OK + b"Content-Length: bad\r\n\r\n",
        _OK + b"Content-Length: 0\r\nContent-Length: 32\r\n\r\n",
        _OK + b"Transfer-Encoding: gzip\r\n\r\n",
        _OK + b"Transfer-Encoding: chunked\r\nContent-Length: 0\r\n\r\n0\r\n\r\n",
        _CHUNKED + b"0;\r\n\r\n",
        _CHUNKED + b'0;x="unterminated\r\n\r\n',
        _CHUNKED + b"0\r\nMalformed trailer\r\n\r\n",
        _OK + b"Content-Length : 32\r\n\r\n",
        _OK + b"Malformed header\r\nContent-Length: 32\r\n\r\n",
        _OK + b"X-Header: yes\r\n Content-Length: 32\r\n\r\n",
        _OK + b"Content-Length: " + b"9" * 5000 + b"\r\n\r\n",
    ],
    ids=[
        "length_empty",
        "length_partial",
        "chunk_body",
        "chunk_crlf_missing",
        "chunk_crlf_invalid",
        "chunk_negative",
        "chunk_signed",
        "trailer_empty",
        "trailer_unterminated",
        "trailer_partial",
        "headers_unterminated",
        "length_negative",
        "length_malformed",
        "length_conflict",
        "transfer_unsupported",
        "framing_ambiguous",
        "extension_empty",
        "extension_unterminated",
        "trailer_malformed",
        "header_whitespace_before_colon",
        "header_defect_hides_length",
        "header_fold_hides_length",
        "length_integer_overflow",
    ],
)
def test_http_incomplete_or_malformed_framing_never_completes(http_fixture, wire):
    runner, request, state = http_fixture
    state["raw_response"] = [(wire, 0)]
    with pytest.raises((http.client.HTTPException, RuntimeError)):
        runner.execute(request, chunks=[b"data"], deadline_monotonic=monotonic() + 2)
    assert runner.local_stopped
    state["raw_response"] = [(_OK + b"Content-Length: 0\r\n\r\n", 0)]
    assert runner.cancel_and_observe(request.identity, deadline_monotonic=monotonic() + 2).remote_state == "unknown"


@pytest.mark.parametrize("status", [200, 500])
def test_http_close_failure_preserves_primary_and_never_claims_completion(http_fixture, status):
    runner, request, state = http_fixture
    state["raw_response"] = [(f"HTTP/1.1 {status} Result\r\nContent-Length: 0\r\n\r\n".encode(), 0)]
    state["close_failure"] = True
    expected = OSError if status == 200 else RuntimeError
    message = "synthetic_close_failure" if status == 200 else "clickhouse_file_http_status:500"
    with pytest.raises(expected, match=message) as captured:
        runner.execute(request, chunks=[b"data"], deadline_monotonic=monotonic() + 2)
    if status == 500:
        assert "clickhouse_file_http_local_close_failed" in captured.value.__notes__
    assert not runner.local_stopped
    assert runner.cancel_and_observe(request.identity, deadline_monotonic=monotonic() + 2).remote_state == "unknown"


@pytest.mark.parametrize("placement", ["headers", "trailers", "both", "chunks"])
@pytest.mark.parametrize("excess", [0, 1])
def test_http_metadata_has_one_aggregate_budget(http_fixture, placement, excess):
    runner, request, state = http_fixture
    fields = [b"X-Fill: " + b"x" * 20_000 + b"\r\n"] * 3
    if placement == "chunks":
        prefix, suffix = _CHUNKED, b"\r\n\r\n"
        lines = b"0;" + b"x" * (65536 + excess - len(prefix) - len(suffix) - 2)
    else:
        prefix, suffix = _CHUNKED, b"0\r\n\r\n"
        if placement in {"headers", "both"}:
            prefix = _OK + b"Transfer-Encoding: chunked\r\n" + fields.pop() + b"\r\n"
        padding = 65536 + excess - len(prefix) - len(suffix) - sum(map(len, fields)) - len(b"X-Pad: \r\n")
        lines = b"".join(fields) + b"X-Pad: " + b"x" * padding + b"\r\n"
        if placement == "headers":
            prefix, lines, suffix = prefix[:-2] + lines + b"\r\n", b"", suffix
        else:
            prefix, suffix = prefix + b"0\r\n", b"\r\n"
    state["raw_response"] = [(prefix + lines + suffix, 0)]
    if excess:
        with pytest.raises(RuntimeError, match="clickhouse_file_response_metadata_limit"):
            runner.execute(request, chunks=[], deadline_monotonic=monotonic() + 2)
    else:
        assert runner.execute(request, chunks=[], deadline_monotonic=monotonic() + 2).remote_state == "completed"
    assert runner.local_stopped


@pytest.mark.parametrize(
    "wire",
    [
        b"HTTP/1.0 200 OK\r\n\r\n \t\n",
        _OK + b"Connection: close\r\nContent-Length: 3\r\n\r\n \t\n",
        _CHUNKED + b"1\r\n \r\n2\r\n\t\n\r\n0\r\nX-Result: yes\r\n\r\n",
        _OK + b"Content-Length: 65536\r\n\r\n" + b" " * 65536,
        _CHUNKED + b'3 ; ignored="quoted\\"value";token=yes\r\n \t\n\r\n0\r\n\r\n',
    ],
    ids=["http10_close", "explicit_close", "chunked_trailers", "body_at_limit", "chunk_extensions"],
)
def test_http_complete_framing_preserves_supported_success(http_fixture, wire):
    runner, request, state = http_fixture
    state["raw_response"] = [(wire, 0)]
    result = runner.execute(request, chunks=[b"data"], deadline_monotonic=monotonic() + 2)
    assert result.remote_state == "completed"
    assert result.emitted_bytes == 4
    assert result.emitted_sha256 == hashlib.sha256(b"data").hexdigest()
    assert runner.local_stopped
