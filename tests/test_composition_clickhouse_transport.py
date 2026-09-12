"""Real loopback HTTP framing tests; no live ClickHouse certification."""

from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Event, Thread
from urllib.parse import parse_qs, urlsplit

import pytest

from dpone.adapters.composition_clickhouse_transport import (
    ClickHouseDispatchTransport,
    ClickHouseDispatchTransportError,
    ClickHouseTransportCredentials,
)
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_clickhouse_dispatch import ExchangeSnapshotDispatch
from tests.composition_snapshot_helpers import intent
from tests.test_composition_clickhouse_dispatch import create, insert


class Journal:
    def __init__(self):
        self.claims = []

    def claim_once(self, dispatch):
        if dispatch.claim_key in self.claims:
            raise CompositionAdmissionError("dispatch_replay")
        self.claims.append(dispatch.claim_key)


@pytest.fixture
def server():
    requests = []
    behavior = {"status": 200, "body": b"", "declared": None, "chunked": None, "error": None}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            requests.append((self.path, dict(self.headers), body))
            if behavior.get("disconnect"):
                self.close_connection = True
                return
            if "header_entered" in behavior:
                behavior["header_entered"].set()
                assert behavior["header_release"].wait(3)
            if "raw_response" in behavior:
                response = behavior["raw_response"]
                self.wfile.write(response(self.path) if callable(response) else response)
                self.wfile.flush()
                self.close_connection = True
                return
            self.send_response(behavior["status"])
            if behavior.get("query_id", "echo") != "absent":
                self.send_header(
                    "X-ClickHouse-Query-Id",
                    parse_qs(urlsplit(self.path).query)["query_id"][0]
                    if behavior.get("query_id", "echo") == "echo"
                    else behavior["query_id"],
                )
            if behavior["chunked"] is not None:
                self.send_header("Transfer-Encoding", "chunked")
            elif behavior["declared"] != "absent":
                self.send_header(
                    "Content-Length",
                    str(len(behavior["body"]) if behavior["declared"] is None else behavior["declared"]),
                )
            if behavior["error"]:
                self.send_header("X-ClickHouse-Exception-Code", behavior["error"])
            if behavior["status"] == 302:
                self.send_header("Location", "/redirect-target")
            for name, value in behavior.get("extra_headers", []):
                self.send_header(name, value)
            self.end_headers()
            if "response_entered" in behavior:
                behavior["response_entered"].set()
                assert behavior["response_release"].wait(3)
            self.wfile.write(behavior["body"] if behavior["chunked"] is None else behavior["chunked"])
            self.wfile.flush()
            self.close_connection = True

        def log_message(self, *args):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=lambda: http.serve_forever(poll_interval=0.02), daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{http.server_port}", requests, behavior
    finally:
        http.shutdown()
        http.server_close()
        thread.join()


def transport(endpoint, journal=None, **kwargs):
    return ClickHouseDispatchTransport(
        endpoint=endpoint,
        credentials=ClickHouseTransportCredentials("issued", "secret-token"),
        journal=journal or Journal(),
        timeout_seconds=kwargs.pop("timeout_seconds", 2),
        **kwargs,
    )


@pytest.mark.parametrize(
    "dispatch,payload", [(create(), b""), (insert(), b"native"), (ExchangeSnapshotDispatch(intent()), b"")]
)
def test_closed_sync_request_acknowledges_complete_body_only(server, dispatch, payload):
    endpoint, requests, _ = server
    journal = Journal()
    result = transport(endpoint, journal).execute(dispatch, payload=payload)
    assert journal.claims == [dispatch.claim_key]
    assert result.dispatch_sha256 == dispatch.dispatch_sha256
    assert result.query_id == dispatch.query_id
    assert result.request_body_bytes == len(payload)
    assert result.response_body_bytes == 0
    path, headers, body = requests[0]
    query = parse_qs(urlsplit(path).query)
    assert query["wait_end_of_query"] == ["1"]
    assert query["async_insert"] == ["0"]
    assert query["query_id"] == [dispatch.query_id]
    assert "session_id" not in query and "session_check" not in query
    assert headers["Connection"] == "close" and headers["Accept-Encoding"] == "identity"
    assert body == payload and "secret-token" not in path
    assert len(requests) == 1


@pytest.mark.parametrize(
    "mode", ["redirect", "error200", "errorheader", "partial", "no_framing", "bad_chunk", "oversize", "http_error"]
)
def test_uncertain_response_never_acknowledges_or_retries(server, mode):
    endpoint, requests, behavior = server
    changes = {
        "redirect": {"status": 302},
        "error200": {"body": b"Code: 1. DB::Exception: failed"},
        "errorheader": {"error": "241"},
        "partial": {"declared": 12, "body": b"part"},
        "no_framing": {"declared": "absent"},
        "bad_chunk": {"chunked": b"4\r\npart\r\n"},
        "oversize": {"body": b"x" * 20},
        "http_error": {"status": 500},
    }
    behavior.update(changes[mode])
    journal = Journal()
    client = transport(endpoint, journal, max_response_bytes=16)
    with pytest.raises(ClickHouseDispatchTransportError) as failed:
        client.execute(insert(), payload=b"native")
    assert failed.value.request_body_bytes == 6
    assert "secret-token" not in str(failed.value)
    assert len(requests) == 1
    with pytest.raises(CompositionAdmissionError, match="dispatch_replay"):
        client.execute(insert(), payload=b"native")
    assert len(requests) == 1


def test_complete_empty_chunked_response_is_accepted(server):
    endpoint, _, behavior = server
    behavior["chunked"] = b"0\r\n\r\n"
    assert transport(endpoint).execute(create()).response_body_bytes == 0


def test_pre_send_hash_length_and_body_budget_checked_before_claim(server):
    endpoint, requests, _ = server
    journal = Journal()
    client = transport(endpoint, journal, max_payload_bytes=6)
    for dispatch, payload in [
        (insert(), b"change"),
        (insert(), b"nativ"),
        (insert(b"native7"), b"native7"),
        (create(), b"native"),
    ]:
        with pytest.raises(CompositionAdmissionError):
            client.execute(dispatch, payload=payload)
    assert not journal.claims and not requests


def test_paused_claim_cannot_send_before_durable_ack(server):
    endpoint, requests, _ = server
    entered, release = Event(), Event()

    class PausedJournal(Journal):
        def claim_once(self, dispatch):
            super().claim_once(dispatch)
            entered.set()
            assert release.wait(3)

    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(transport(endpoint, PausedJournal()).execute, create())
        assert entered.wait(2)
        try:
            assert requests == [] and not pending.done()
        finally:
            release.set()
        assert pending.result(timeout=3).request_body_bytes == 0


def test_lost_claim_ack_sends_nothing(server):
    endpoint, requests, _ = server

    class LostAck(Journal):
        def claim_once(self, dispatch):
            super().claim_once(dispatch)
            raise RuntimeError("commit acknowledgement lost")

    with pytest.raises(RuntimeError, match="acknowledgement"):
        transport(endpoint, LostAck()).execute(create())
    assert not requests


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://example.com",
        "https://user:pass@example.com",
        "https://example.com/path",
        "https://example.com?async_insert=1",
        "https://example.com#fragment",
    ],
)
def test_endpoint_cannot_override_protected_transport_profile(endpoint):
    with pytest.raises(CompositionAdmissionError):
        transport(endpoint)


def test_credentials_repr_hides_token():
    assert "secret-token" not in repr(ClickHouseTransportCredentials("issued", "secret-token"))


def test_response_ack_waits_for_complete_chunk_framing(server):
    endpoint, requests, behavior = server
    entered, release = Event(), Event()
    behavior.update(chunked=b"0\r\n\r\n", response_entered=entered, response_release=release)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(transport(endpoint).execute, create())
        assert entered.wait(2)
        try:
            assert len(requests) == 1 and not pending.done()
        finally:
            release.set()
        assert pending.result(timeout=3).response_framing == "chunked"


def test_incomplete_framing_hits_absolute_response_deadline(server):
    endpoint, requests, behavior = server
    entered, release = Event(), Event()
    behavior.update(chunked=b"0\r\n\r\n", response_entered=entered, response_release=release)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(transport(endpoint, timeout_seconds=0.1).execute, create())
        assert entered.wait(2)
        try:
            with pytest.raises(ClickHouseDispatchTransportError):
                pending.result(timeout=2)
        finally:
            release.set()
    assert len(requests) == 1


def test_partial_socket_send_reports_only_accepted_bytes_without_retry(monkeypatch):
    calls = []

    class PartialSocket:
        def settimeout(self, value):
            pass

        def send(self, value):
            calls.append(bytes(value))
            if len(calls) == 1:
                return 2
            raise OSError("sensitive detail secret-token")

    class Connection:
        sock = PartialSocket()

        def connect(self):
            pass

        def putrequest(self, *args, **kwargs):
            pass

        def putheader(self, *args):
            pass

        def endheaders(self):
            pass

        def close(self):
            pass

    journal = Journal()
    client = transport("http://127.0.0.1", journal)
    monkeypatch.setattr(client._http, "_connection", Connection)
    with pytest.raises(ClickHouseDispatchTransportError) as result:
        client.execute(insert(), payload=b"native")
    assert calls == [b"native", b"tive"]
    assert result.value.request_body_bytes == 2
    assert "secret-token" not in str(result.value)
    assert journal.claims == [insert().claim_key]


@pytest.mark.parametrize("query_id", ["absent", "different-query"])
def test_response_requires_exact_query_attribution(server, query_id):
    endpoint, requests, behavior = server
    behavior["query_id"] = query_id
    with pytest.raises(ClickHouseDispatchTransportError):
        transport(endpoint).execute(create())
    assert len(requests) == 1


@pytest.mark.parametrize("chunked", [b"0\r\n", b"0\n\r\n", b"0\r\nX-ClickHouse-Exception-Code: 1\r\n\r\n"])
def test_incomplete_or_uninspected_chunk_terminator_never_acknowledges(server, chunked):
    endpoint, _, behavior = server
    behavior["chunked"] = chunked
    with pytest.raises(ClickHouseDispatchTransportError):
        transport(endpoint).execute(create())


@pytest.mark.parametrize("ending", [b"", b"\n", b"\r"])
def test_truncated_header_block_never_acknowledges(server, ending):
    endpoint, _, behavior = server

    def response(path):
        query_id = parse_qs(urlsplit(path).query)["query_id"][0]
        return f"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-ClickHouse-Query-Id: {query_id}\r\n".encode() + ending

    behavior["raw_response"] = response
    with pytest.raises(ClickHouseDispatchTransportError):
        transport(endpoint).execute(create())


@pytest.mark.parametrize("localhost", [False, True])
def test_numeric_endpoint_performs_no_dns_resolution(server, monkeypatch, localhost):
    import socket

    endpoint, requests, _ = server

    def forbidden(*args, **kwargs):
        raise AssertionError("DNS resolution is forbidden")

    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    if localhost:
        endpoint = endpoint.replace("127.0.0.1", "localhost")
    assert transport(endpoint).execute(create()).response_body_bytes == 0
    assert len(requests) == 1


@pytest.mark.parametrize(
    "endpoint", ["https://example.com", "http://127.0.0.1.nip.io", "https://localhost.example.com"]
)
def test_hostname_endpoint_cannot_enter_unbounded_resolution(endpoint):
    with pytest.raises(CompositionAdmissionError):
        transport(endpoint)


def test_tls_profile_keeps_certificate_and_pinned_ip_verification():
    import ssl

    context = transport("https://127.0.0.1")._http._context
    assert context is not None and context.verify_mode == ssl.CERT_REQUIRED and context.check_hostname


@pytest.mark.parametrize("raw", [b"1\r\nxXX0\r\n\r\n", b"1\r\nx\r\n0;ignored=extension\r\n\r\n"])
def test_noncanonical_chunk_boundaries_reject(server, raw):
    endpoint, _, behavior = server
    behavior["chunked"] = raw
    with pytest.raises(ClickHouseDispatchTransportError):
        transport(endpoint).execute(create())


@pytest.mark.parametrize(
    "malformed", [b"MalformedHeader", b"Bad Header: value", b": empty-name", b"Bad\x00Name: value"]
)
def test_malformed_header_cannot_hide_server_exception(server, malformed):
    endpoint, _, behavior = server

    def response(path):
        query_id = parse_qs(urlsplit(path).query)["query_id"][0]
        return (
            f"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-ClickHouse-Query-Id: {query_id}\r\n".encode()
            + malformed
            + b"\r\nX-ClickHouse-Exception-Code: 241\r\n\r\n"
        )

    behavior["raw_response"] = response
    with pytest.raises(ClickHouseDispatchTransportError):
        transport(endpoint).execute(create())


@pytest.mark.parametrize("endpoint", ["http://127.0.0.1:0", "https://127.0.0.1:0"])
def test_explicit_zero_port_rejected_before_dispatch(endpoint):
    journal = Journal()
    with pytest.raises(CompositionAdmissionError):
        transport(endpoint, journal)
    assert journal.claims == []
