"""Real loopback TLS v2 routing; no deployment or business authority certification."""

import socket
import threading
import time
from secrets import token_urlsafe

import pytest

from dpone.adapters.composition_dispatch_http_server import create_dispatch_http_server, make_bearer_authenticator
from dpone.contracts.composition_dispatch_rpc import DispatchRpcResponse
from dpone.contracts.composition_dispatch_v2 import DispatchV2Response, decode_response, encode_request
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.runtime.composition_execution_budget import CompositionExecutionBudget, ExecutionStopSignal
from tests import test_composition_dispatch_http as v1_tls
from tests.test_composition_dispatch_http import headers, raw
from tests.test_composition_dispatch_v2 import request
from tests.test_composition_remote_transfer_result import result_body, status_body


@pytest.fixture(scope="module")
def tls(tmp_path_factory):
    return v1_tls.tls.__wrapped__(tmp_path_factory)


def response(value, status="IN_PROGRESS"):
    _, body = status_body()
    return DispatchV2Response.for_request(value, canonical_json_bytes(body), status=status)


@pytest.fixture
def service(tls):
    running = []

    def start(handler=None, *, v2=True, **kwargs):
        token, contexts, calls = token_urlsafe(32), [], []

        def handle(value, budget):
            calls.append((value, budget))
            return handler(value, budget) if handler else response(value)

        stop = ExecutionStopSignal()

        def budget_factory(seconds):
            return CompositionExecutionBudget(seconds, stop_event=stop)

        options = (
            {
                "v2_handler": handle,
                "execution_timeout_seconds": 1,
                "budget_factory": budget_factory,
                "stop_signal": stop,
            }
            if v2
            else {}
        )
        options.update(kwargs)
        server = create_dispatch_http_server(
            ("127.0.0.1", 0),
            ssl_context=tls[0],
            authenticator=make_bearer_authenticator(token, lambda c: contexts.append(c)),
            handler=lambda value, payload, deadline: DispatchRpcResponse.for_request(value, b"{}"),
            **options,
        )
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01))
        thread.start()
        running.append((server, thread))
        return server, token, contexts, calls

    yield start
    for server, thread in running:
        server.shutdown()
        server.server_close()
        thread.join(2)
        assert not thread.is_alive()


def exchange(tls, server, token, *, frame=None):
    frame = encode_request(request()) if frame is None else frame
    connection = tls[1].wrap_socket(
        socket.create_connection(server.server_address, timeout=3), server_hostname="127.0.0.1"
    )
    try:
        connection.sendall(headers(server, token, route="/v2/composition-dispatch", length=str(len(frame))) + frame)
        chunks = []
        while chunk := connection.recv(65536):
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        connection.close()


def test_v2_requires_explicit_enable_and_authenticates_twice(service, tls):
    server, token, contexts, calls = service()
    wire = exchange(tls, server, token)
    assert b"200 OK" in wire
    assert decode_response(wire.split(b"\r\n\r\n", 1)[1], request()).status == "IN_PROGRESS"
    assert [c.request for c in contexts] == [None, request()]
    assert len(calls) == 1
    legacy, token, _, calls = service(v2=False)
    assert b"400 Bad Request" in exchange(tls, legacy, token)
    assert calls == []


def test_v2_rejects_unauthenticated_headers_without_waiting_for_body(service, tls):
    server, _, contexts, calls = service(timeout_seconds=0.5)
    wire = raw(tls, server, headers(server, "invalid", route="/v2/composition-dispatch"))
    assert b"401 Unauthorized" in wire and not contexts and not calls


def test_v2_never_accepts_payload_bytes(service, tls):
    server, token, _, calls = service()
    assert b"400 Bad Request" in exchange(tls, server, token, frame=encode_request(request()) + b"native")
    assert not calls


def test_execution_budget_starts_after_short_accept_budget(service, tls):
    def execute(value, budget):
        time.sleep(0.15)
        budget.require_effect()
        return response(value)

    server, token, _, _ = service(execute, timeout_seconds=0.1)
    assert b"200 OK" in exchange(tls, server, token)


def test_expired_running_receipt_maps_unknown_and_late_success_rejects(service, tls):
    def expired(value, budget):
        budget._clock = lambda: budget.execution_deadline + 0.01
        return response(value)

    server, token, _, _ = service(expired)
    wire = exchange(tls, server, token)
    assert decode_response(wire.split(b"\r\n\r\n", 1)[1], request()).status == "UNKNOWN"

    def success(value, budget):
        budget._clock = lambda: budget.execution_deadline + 0.01
        _, body = result_body()
        return DispatchV2Response.for_request(value, canonical_json_bytes(body), status="SUCCEEDED")

    server, token, _, _ = service(success)
    assert b"500 Internal Server Error" in exchange(tls, server, token)


def test_shutdown_closes_effect_admission_and_retains_slot_until_handler_unwinds(service, tls):
    entered, resume = threading.Event(), threading.Event()

    def blocked(value, budget):
        entered.set()
        assert resume.wait(3)
        assert budget.execution_expired
        with pytest.raises(RuntimeError):
            budget.require_effect()
        return response(value)

    server, token, _, _ = service(blocked, max_concurrency=1)
    errors = []

    def caller():
        try:
            exchange(tls, server, token)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=caller)
    thread.start()
    try:
        assert entered.wait(2)
        server.stop_admission()
        assert not server._slots.acquire(blocking=False)
        assert len(server._budgets) == 1
    finally:
        resume.set()
        thread.join(3)
    assert not thread.is_alive() and not errors


@pytest.mark.parametrize(
    "options",
    [
        {"v2_handler": lambda *_: None},
        {"execution_timeout_seconds": 1},
        {"v2_handler": lambda *_: None, "execution_timeout_seconds": True},
        {"v2_handler": lambda *_: None, "execution_timeout_seconds": 901},
    ],
)
def test_v2_configuration_is_paired_and_bounded(tls, options):
    with pytest.raises(RuntimeError):
        create_dispatch_http_server(
            ("127.0.0.1", 0), ssl_context=tls[0], authenticator=lambda *_: None, handler=lambda *_: None, **options
        )


def test_repeated_signal_notification_never_acquires_server_or_budget_locks(service):
    server, _, _, _ = service()
    budget = CompositionExecutionBudget(900, stop_event=server.admission_stop)
    server._budgets.add(budget)
    finished = threading.Event()

    def notify():
        server.stop_admission()
        server.stop_admission()
        finished.set()

    server._budget_lock.acquire()
    budget._lock.acquire()
    thread = threading.Thread(target=notify)
    try:
        thread.start()
        assert finished.wait(0.5), "signal notification acquired a held lifecycle lock"
    finally:
        budget._lock.release()
        server._budget_lock.release()
        thread.join(2)
        server._budgets.discard(budget)
    assert not thread.is_alive() and budget.execution_expired
    assert budget.begin_cleanup() == server.admission_stop.stopped_at + 60


@pytest.mark.parametrize("missing", ["v2_handler", "execution_timeout_seconds", "budget_factory", "stop_signal"])
def test_partial_budget_injection_rejects_before_bind_or_factory(tls, monkeypatch, missing):
    import socketserver

    monkeypatch.setattr(socketserver.TCPServer, "server_bind", lambda _: pytest.fail("listener bound"))
    options = {
        "v2_handler": lambda *_: None,
        "execution_timeout_seconds": 1,
        "budget_factory": lambda _: pytest.fail("budget constructed"),
        "stop_signal": ExecutionStopSignal(),
    }
    del options[missing]
    with pytest.raises(RuntimeError):
        create_dispatch_http_server(
            ("127.0.0.1", 0), ssl_context=tls[0], authenticator=lambda *_: None, handler=lambda *_: None, **options
        )


def test_budget_is_built_once_after_complete_authenticated_body(service, tls):
    calls = []
    stop = ExecutionStopSignal()

    def make_budget(seconds):
        calls.append(seconds)
        return CompositionExecutionBudget(seconds, stop_event=stop)

    server, token, contexts, handled = service(budget_factory=make_budget, stop_signal=stop)
    assert calls == []
    assert b"401 Unauthorized" in raw(tls, server, headers(server, "wrong", route="/v2/composition-dispatch"))
    assert b"400 Bad Request" in exchange(tls, server, token, frame=encode_request(request()) + b"x")
    assert calls == []
    assert b"200 OK" in exchange(tls, server, token)
    assert calls == [1] and len(handled) == 1
    assert contexts[-1].request == request()
    assert handled[0][1]._stop is stop
    assert server.admission_stop is stop


@pytest.mark.parametrize("field,value", [("budget_factory", True), ("stop_signal", object())])
def test_invalid_budget_collaborator_rejects_before_bind(tls, monkeypatch, field, value):
    import socketserver

    monkeypatch.setattr(socketserver.TCPServer, "server_bind", lambda _: pytest.fail("listener bound"))
    stop = ExecutionStopSignal()
    options = dict(
        v2_handler=lambda *_: None,
        execution_timeout_seconds=1,
        budget_factory=lambda seconds: CompositionExecutionBudget(seconds, stop_event=stop),
        stop_signal=stop,
    )
    options[field] = value
    with pytest.raises(RuntimeError):
        create_dispatch_http_server(
            ("127.0.0.1", 0), ssl_context=tls[0], authenticator=lambda *_: None, handler=lambda *_: None, **options
        )


@pytest.mark.parametrize("seconds", [True, 0, 901])
def test_fully_injected_v2_still_requires_bounded_integer_timeout(service, seconds):
    with pytest.raises(RuntimeError):
        service(execution_timeout_seconds=seconds)


def test_v1_admission_flag_stops_without_execution_policy(service):
    server, _, _, _ = service(v2=False)
    assert not server.admission_stop.is_set()
    server.stop_admission()
    server.stop_admission()
    assert server.admission_stop.is_set()
    assert server.budget_factory is None
