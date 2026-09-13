"""Real local TLS exercises authentication order and no-retry behavior."""

import socket
import ssl
import subprocess
import threading
import time
from secrets import token_urlsafe

import pytest

from dpone.adapters.composition_dispatch_http_client import DispatchHttpClient, DispatchHttpError
from dpone.adapters.composition_dispatch_http_server import create_dispatch_http_server, make_bearer_authenticator
from dpone.contracts.composition_dispatch_rpc import RPC_CONTENT_TYPE, RPC_ROUTE, DispatchRpcResponse, encode_request
from dpone.contracts.strict_json import strict_json_object
from tests.test_composition_clickhouse_dispatch import insert
from tests.test_composition_dispatch_rpc import request


@pytest.fixture(scope="module")
def tls(tmp_path_factory):
    root = tmp_path_factory.mktemp("dispatcher-tls")
    cert, key = root / "cert.pem", root / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
        timeout=10,
    )
    key.chmod(0o600)
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.minimum_version = ssl.TLSVersion.TLSv1_2
    server.load_cert_chain(cert, key)
    client = ssl.create_default_context(cafile=str(cert))
    return server, client


@pytest.fixture
def service(tls):
    servers = []

    def start(handler=None, authorize_context=None, **kwargs):
        token = token_urlsafe(32)
        contexts = []
        calls = []

        def authorize(context):
            contexts.append(context)
            if authorize_context is not None:
                return authorize_context(context)

        def handle(value, payload, deadline):
            calls.append((value, payload))
            return handler(value) if handler else DispatchRpcResponse.for_request(value, b'{"observed":true}')

        server = create_dispatch_http_server(
            ("127.0.0.1", 0),
            ssl_context=tls[0],
            authenticator=make_bearer_authenticator(token, authorize),
            handler=handle,
            **kwargs,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append((server, thread))
        return server, token, contexts, calls

    yield start
    for server, thread in servers:
        server.shutdown()
        server.server_close()
        thread.join(2)
        assert not thread.is_alive()


def raw(tls, server, headers):
    connection = tls[1].wrap_socket(
        socket.create_connection(server.server_address, timeout=2), server_hostname="127.0.0.1"
    )
    connection.sendall(headers)
    result = connection.recv(4096)
    connection.close()
    return result


def headers(server, token, *, length="100", route=RPC_ROUTE, extra=""):
    return (
        f"POST {route} HTTP/1.1\r\nHost: 127.0.0.1:{server.server_address[1]}\r\n"
        f"Authorization: Bearer {token}\r\nContent-Type: {RPC_CONTENT_TYPE}\r\nContent-Length: {length}\r\n"
        f"Connection: close\r\n{extra}\r\n"
    ).encode()


def test_verified_tls_roundtrip_and_authentication_phases(service, tls, monkeypatch):
    server, token, contexts, calls = service()
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
    client = DispatchHttpClient(f"https://127.0.0.1:{server.server_address[1]}", ssl_context=tls[1], bearer_token=token)
    value = request()
    assert client.call(value).evidence_document == b'{"observed":true}'
    assert [context.request for context in contexts] == [None, value]
    assert calls == [(value, b"")]
    assert token not in repr(client)


def test_reject_unauthenticated_headers_before_waiting_for_body(service, tls):
    server, token, contexts, calls = service(timeout_seconds=0.5)
    response = raw(tls, server, headers(server, "invalid"))
    assert b" 401 " in response
    assert contexts == calls == []
    assert token.encode() not in response


@pytest.mark.parametrize(
    "kwargs",
    [
        {"extra": "Content-Length: 100\r\n"},
        {"extra": "Transfer-Encoding: chunked\r\n"},
        {"route": RPC_ROUTE + "?sql=x"},
        {"length": "999999999999"},
        {"length": "0100"},
    ],
)
def test_reject_ambiguous_http_before_body(service, tls, kwargs):
    server, token, _, calls = service()
    assert b" 400 " in raw(tls, server, headers(server, token, **kwargs))
    assert not calls


def test_foreign_ack_rejected_without_retry(service, tls):
    server, token, _, calls = service(lambda value: DispatchRpcResponse.for_request(request(), b"{}"))
    client = DispatchHttpClient(f"https://127.0.0.1:{server.server_address[1]}", ssl_context=tls[1], bearer_token=token)
    with pytest.raises(DispatchHttpError):
        client.call(request())
    assert len(calls) == 1


def test_client_requires_numeric_https_and_verified_cert(tls):
    for endpoint in (
        "http://127.0.0.1",
        "https://localhost",
        "https://127.0.0.1/?query=x",
        "https://user:pass@127.0.0.1",
        "https://127.0.0.1:0",
    ):
        with pytest.raises(DispatchHttpError):
            DispatchHttpClient(endpoint, ssl_context=tls[1], bearer_token=token_urlsafe(32))
    with pytest.raises(DispatchHttpError):
        DispatchHttpClient(
            "https://127.0.0.1", ssl_context=ssl._create_unverified_context(), bearer_token=token_urlsafe(32)
        )


def test_untrusted_certificate_rejected(service):
    server, token, _, calls = service()
    client = DispatchHttpClient(
        f"https://127.0.0.1:{server.server_address[1]}", ssl_context=ssl.create_default_context(), bearer_token=token
    )
    with pytest.raises(DispatchHttpError):
        client.call(request())
    assert not calls


def native_request():
    dispatch = insert()
    return request(
        "DISPATCH",
        {"dispatch_document": strict_json_object(dispatch.to_bytes()), "dispatch_sha256": dispatch.dispatch_sha256},
    )


def test_native_payload_reaches_handler_only_after_validation(service, tls):
    server, token, _, calls = service()
    client = DispatchHttpClient(f"https://127.0.0.1:{server.server_address[1]}", ssl_context=tls[1], bearer_token=token)
    value = native_request()
    client.call(value, b"native")
    assert calls == [(value, b"native")]
    frame = encode_request(value, b"native")[:-1] + b"E"
    assert b" 400 " in raw(tls, server, headers(server, token, length=str(len(frame))) + frame)
    assert len(calls) == 1


def test_subject_policy_denial_precedes_native_body(service, tls):
    def deny_subject(context):
        if context.request is not None:
            raise ValueError("private policy diagnostic")

    server, token, contexts, calls = service(authorize_context=deny_subject)
    frame = encode_request(native_request(), b"native")
    response = raw(tls, server, headers(server, token, length=str(len(frame))) + frame[:-6])
    assert b" 401 " in response
    assert len(contexts) == 2 and not calls
    assert b"private" not in response


def test_absolute_deadline_releases_slow_body_connection(service, tls):
    server, token, _, calls = service(timeout_seconds=0.15)
    start = time.monotonic()
    assert raw(tls, server, headers(server, token)) == b""
    assert time.monotonic() - start < 1.5
    assert not calls


def test_concurrency_limit_includes_body_wait_and_tls_handshake(service, tls):
    admitted = threading.Event()

    def authorize(context):
        admitted.set()

    server, token, _, calls = service(authorize_context=authorize, max_concurrency=1, timeout_seconds=1)
    with tls[1].wrap_socket(
        socket.create_connection(server.server_address, timeout=2), server_hostname="127.0.0.1"
    ) as first:
        first.sendall(headers(server, token))
        assert admitted.wait(1)
        with pytest.raises((OSError, ssl.SSLError)):
            with tls[1].wrap_socket(
                socket.create_connection(server.server_address, timeout=2), server_hostname="127.0.0.1"
            ):
                pytest.fail("saturated server accepted a second TLS handshake")
    assert not calls


@pytest.mark.parametrize("status", ["UNKNOWN", "REJECTED"])
def test_non_ack_never_retries(service, tls, status):
    server, token, _, calls = service(lambda value: DispatchRpcResponse.for_request(value, b"{}", status=status))
    client = DispatchHttpClient(f"https://127.0.0.1:{server.server_address[1]}", ssl_context=tls[1], bearer_token=token)
    with pytest.raises(DispatchHttpError):
        client.call(request())
    assert len(calls) == 1


@pytest.mark.parametrize(
    "change",
    [
        {"authenticator": None},
        {"handler": None},
        {"ssl_context": None},
        {"max_concurrency": 0},
        {"max_payload_bytes": 0},
        {"timeout_seconds": float("inf")},
    ],
)
def test_server_has_no_optional_security_fallback(tls, change):
    arguments = {
        "ssl_context": tls[0],
        "authenticator": lambda context, token: None,
        "handler": lambda value, payload, deadline: DispatchRpcResponse.for_request(value, b"{}"),
    }
    arguments.update(change)
    with pytest.raises((ValueError, DispatchHttpError)):
        create_dispatch_http_server(("127.0.0.1", 0), **arguments)
