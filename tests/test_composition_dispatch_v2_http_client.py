"""Real loopback TLS and bounded deadline faults; no business/live certification."""

import socket
import ssl
import threading
from secrets import token_urlsafe

import pytest

from dpone.adapters import composition_dispatch_v2_http_client as module
from dpone.adapters.composition_dispatch_http_client import DispatchHttpError, receive_exact, receive_headers
from dpone.contracts.composition_dispatch_rpc import RPC_CONTENT_TYPE
from dpone.contracts.composition_dispatch_v2 import DispatchV2Response, decode_request
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from tests.test_composition_dispatch_v2 import request
from tests.test_composition_dispatch_v2_http_server import service as service
from tests.test_composition_dispatch_v2_http_server import tls as tls
from tests.test_composition_remote_transfer_result import status_body


def client(tls, endpoint, token, **kwargs):
    return module.DispatchV2HttpClient(endpoint, ssl_context=tls[1], bearer_token=token, **kwargs)


def test_real_tls_v2_metadata_only_and_no_v1_fallback(service, tls):
    server, token, _, calls = service()
    value = client(tls, f"https://127.0.0.1:{server.server_address[1]}", token).call(request())
    assert value.status == "IN_PROGRESS" and len(calls) == 1
    assert calls[0][0] == request() and calls[0][0].payload_spec == (0, None)
    legacy, token, _, calls = service(v2=False)
    with pytest.raises(DispatchHttpError, match="^rpc_v2_transport_unknown$"):
        client(tls, f"https://127.0.0.1:{legacy.server_address[1]}", token).call(request())
    assert calls == []


@pytest.mark.parametrize(
    "endpoint", ["http://127.0.0.1", "https://localhost", "https://user:secret@127.0.0.1", "https://127.0.0.1/path"]
)
def test_requires_numeric_direct_https_endpoint(tls, endpoint):
    with pytest.raises(DispatchHttpError):
        client(tls, endpoint, token_urlsafe(32))


@pytest.mark.parametrize(
    "option,value",
    [
        ("accept_timeout_seconds", 31),
        ("execution_timeout_seconds", 0.9),
        ("execution_timeout_seconds", 1.5),
        ("execution_timeout_seconds", 900.0),
        ("execution_timeout_seconds", 901),
        ("cleanup_timeout_seconds", 61),
        ("execution_timeout_seconds", True),
    ],
)
def test_budget_configuration_is_bounded(tls, option, value):
    with pytest.raises(DispatchHttpError):
        client(tls, "https://127.0.0.1", token_urlsafe(32), **{option: value})


def test_unverified_tls_configuration_is_rejected():
    context = ssl._create_unverified_context()
    with pytest.raises(DispatchHttpError, match="rpc_tls_configuration"):
        module.DispatchV2HttpClient("https://127.0.0.1", ssl_context=context, bearer_token=token_urlsafe(32))


@pytest.fixture
def raw_service(tls):
    running = []

    def start(transform):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(2)
        calls, errors = [], []

        def serve():
            try:
                raw, _ = listener.accept()
                with raw, tls[0].wrap_socket(raw, server_side=True) as connection:
                    deadline = module.time.monotonic() + 2
                    _, headers = receive_headers(connection, deadline)
                    frame = receive_exact(connection, int(headers["content-length"]), deadline)
                    value = decode_request(frame)
                    calls.append(value)
                    _, body = status_body()
                    document = DispatchV2Response.for_request(
                        value, canonical_json_bytes(body), status="IN_PROGRESS"
                    ).to_bytes()
                    response = (
                        f"HTTP/1.1 200 OK\r\nContent-Type: {RPC_CONTENT_TYPE}\r\n"
                        f"Content-Length: {len(document)}\r\nConnection: close\r\n\r\n"
                    ).encode() + document
                    connection.sendall(transform(response))
            except Exception as error:
                errors.append(type(error).__name__)
            finally:
                listener.close()

        thread = threading.Thread(target=serve)
        thread.start()
        running.append((listener, thread, errors))
        return f"https://127.0.0.1:{listener.getsockname()[1]}", calls

    yield start
    for listener, thread, errors in running:
        thread.join(3)
        assert not thread.is_alive() and errors == []


@pytest.mark.parametrize("fault", ["redirect", "extra", "truncated", "length", "correlation", "evidence"])
def test_complete_framing_and_correlation_fail_closed_without_retry(raw_service, tls, fault):
    def corrupt(wire):
        head, body = wire.split(b"\r\n\r\n", 1)
        if fault == "redirect":
            return wire.replace(b"200 OK", b"302 Found")
        if fault == "extra":
            return wire + b"x"
        if fault == "truncated":
            return wire[:-1]
        if fault == "length":
            return head + b"\r\nContent-Length: 1\r\n\r\n" + body
        value = strict_json_object(body)
        if fault == "correlation":
            value["request_id"] = "22222222-2222-4222-8222-222222222222"
        else:
            value["evidence_sha256"] = "sha256:" + "f" * 64
        changed = canonical_json_bytes(value)
        assert len(changed) == len(body)
        return head + b"\r\n\r\n" + changed

    endpoint, calls = raw_service(corrupt)
    with pytest.raises(DispatchHttpError, match="^rpc_v2_transport_unknown$"):
        client(tls, endpoint, token_urlsafe(32)).call(request())
    assert calls == [request()]


def test_unknown_transport_does_not_invent_status_receipt(tls, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("credential secret")

    monkeypatch.setattr(module.socket, "socket", fail)
    with pytest.raises(DispatchHttpError, match="^rpc_v2_transport_unknown$"):
        client(tls, "https://127.0.0.1", token_urlsafe(32)).call(request())


@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED", "IN_PROGRESS", "UNKNOWN"])
def test_late_terminal_rejected_but_valid_nonterminal_evidence_returned(service, tls, monkeypatch, status):
    from dataclasses import replace

    from tests.test_composition_remote_transfer_result import result_body

    def handler(value, budget):
        wire_status = "SUCCEEDED" if status == "FAILED" else status
        _, body = (
            result_body()
            if wire_status == "SUCCEEDED"
            else status_body({"IN_PROGRESS": "RUNNING", "UNKNOWN": "COMMIT_UNKNOWN"}[wire_status])
        )
        return DispatchV2Response.for_request(value, canonical_json_bytes(body), status=wire_status)

    server, token, _, _ = service(handler)
    decode = module.decode_response

    def late(document, value):
        response = decode(document, value)
        now = module.time.monotonic()
        monkeypatch.setattr(module.time, "monotonic", lambda: now + 2)
        # This substitution isolates the FAILED deadline branch, not proof validation.
        return replace(response, status="FAILED") if status == "FAILED" else response

    monkeypatch.setattr(module, "decode_response", late)
    selected = client(tls, f"https://127.0.0.1:{server.server_address[1]}", token, execution_timeout_seconds=1)
    if status in {"SUCCEEDED", "FAILED"}:
        with pytest.raises(DispatchHttpError, match="^rpc_v2_transport_unknown$"):
            selected.call(request())
    else:
        assert selected.call(request()).status == status
