"""Retained permission transport owns bytes and cleanup without SQL authority."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

import dpone.adapters.mssql_sqlclient_permission_grant_process as transport
from dpone.adapters.mssql_tds_child_process import TdsChildProcess
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.contracts.mssql_sqlclient_permission_grant_wire import (
    CONTROL_LIMIT,
    CREDENTIAL_LIMIT,
    TOTAL_LIMIT,
    PermissionWireBinding,
    PermissionWireKind,
    encode_permission_message,
    frame_permission_payload,
)
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.contracts.strict_json import strict_json_object
from tests.test_mssql_sqlclient_permission_grant_wire import fixture, transcript


class FakeExecutor:
    instances = []

    def __init__(self, custody, identity, deadline, allowance):
        self.custody, self.identity = custody, identity
        self.deadline, self.allowance = deadline, allowance
        self.cleanup_deadline = None
        self.exit = None
        self.failed = False
        self.ready = threading.Event()
        self.done = threading.Event()
        self.requests = []
        self.settlements = []
        custody.retain_handle(SimpleNamespace(identity=identity))
        self.ready.set()
        self.instances.append(self)

    def request(self, deadline=None):
        self.requests.append(deadline)
        self.cleanup_deadline = time.monotonic() + 1.0 if deadline is None else deadline
        return self.cleanup_deadline

    def request_settlement(self, *, natural_deadline, containment_deadline=None):
        self.settlements.append((natural_deadline, containment_deadline))
        self.cleanup_deadline = time.monotonic() + 1.0
        self.exit = TdsChildExit(self.identity, 0, True)
        self.custody.exit = self.exit
        self.custody._closed = True
        self.done.set()
        return self.cleanup_deadline


@pytest.fixture
def adopted(monkeypatch):
    FakeExecutor.instances.clear()
    monkeypatch.setattr(transport, "TdsChildContainmentExecutor", FakeExecutor)
    binding, *_ = fixture()
    monkeypatch.setattr(transport, "deadline_nanoseconds", lambda value: binding.operation_deadline_ns)
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    child = SimpleNamespace(stdout=None, returncode=None, pid=binding.startup.process.pid)
    custody = TdsChildProcess(child, None, ())
    process = transport.SqlClientPermissionGrantProcess.adopt(
        custody,
        left,
        binding,
        startup_deadline=time.monotonic() + 5.0,
        operation_deadline=time.monotonic() + 10.0,
        termination_timeout=1.0,
    )
    process.await_custody(deadline=process._startup_deadline)
    try:
        yield process, right, custody, binding, FakeExecutor.instances[0]
    finally:
        for channel in (left, right):
            try:
                channel.close()
            except OSError:
                pass


def _recv_frame(channel):
    data = bytearray()
    deadline = time.monotonic() + 2.0
    while len(data) < 4:
        if time.monotonic() >= deadline:
            raise AssertionError("prefix timeout")
        try:
            data.extend(channel.recv(4 - len(data)))
        except BlockingIOError:
            continue
    length = int.from_bytes(data, "big")
    body = bytearray()
    while len(body) < length:
        try:
            body.extend(channel.recv(length - len(body)))
        except BlockingIOError:
            continue
    return bytes(body)


def test_adopt_retains_exact_socket_and_constructs_one_same_custody_executor(adopted):
    process, _, custody, binding, executor = adopted
    assert executor.custody is custody
    assert executor.identity == binding.startup.process
    assert [resource.value for resource in custody._resources if resource.kind == "socket"] == [process._channel]
    assert not process.failed


def test_natural_settlement_rebinds_equal_observed_identity_to_exact_wire_identity(adopted):
    process, _, _, binding, executor = adopted
    executor.identity = replace(binding.startup.process)
    process._eof = True

    result = process.settle(natural_deadline=time.monotonic() + 0.5)

    assert result.identity is binding.startup.process
    assert result == executor.exit


def test_protocol_is_blocked_until_exact_custody_is_acknowledged(monkeypatch):
    FakeExecutor.instances.clear()
    monkeypatch.setattr(transport, "TdsChildContainmentExecutor", FakeExecutor)
    binding, *_ = fixture()
    monkeypatch.setattr(transport, "deadline_nanoseconds", lambda value: binding.operation_deadline_ns)
    left, right = socket.socketpair()
    left.setblocking(False)
    custody = TdsChildProcess(SimpleNamespace(stdout=None, pid=binding.startup.process.pid), None, ())
    process = transport.SqlClientPermissionGrantProcess.adopt(
        custody,
        left,
        binding,
        startup_deadline=time.monotonic() + 5.0,
        operation_deadline=time.monotonic() + 10.0,
        termination_timeout=1.0,
    )
    try:
        with pytest.raises(transport.PermissionGrantProcessUnknown):
            process.receive_public(PermissionWireKind.STARTUP, 0)
        assert process.failed and len(FakeExecutor.instances[0].requests) == 1
    finally:
        left.close()
        right.close()


@pytest.mark.parametrize(
    "failure",
    ["timeout", "expired_ready", "failed", "missing", "closed", "duplicate", "stale_duplicate", "identity", "owner"],
)
def test_custody_uncertainty_is_sticky_and_requests_containment_once(monkeypatch, failure):
    FakeExecutor.instances.clear()
    monkeypatch.setattr(transport, "TdsChildContainmentExecutor", FakeExecutor)
    binding, *_ = fixture()
    monkeypatch.setattr(transport, "deadline_nanoseconds", lambda value: binding.operation_deadline_ns)
    left, right = socket.socketpair()
    left.setblocking(False)
    custody = TdsChildProcess(SimpleNamespace(stdout=None, pid=binding.startup.process.pid), None, ())
    process = transport.SqlClientPermissionGrantProcess.adopt(
        custody,
        left,
        binding,
        startup_deadline=time.monotonic() + 5.0,
        operation_deadline=time.monotonic() + 10.0,
        termination_timeout=1.0,
    )
    executor = FakeExecutor.instances[0]
    if failure == "timeout":
        executor.ready.clear()
    elif failure == "expired_ready":
        pass
    elif failure == "failed":
        executor.failed = True
    elif failure == "missing":
        custody._resources = [r for r in custody._resources if r.kind != "pidfd"]
    elif failure == "closed":
        next(r for r in custody._resources if r.kind == "pidfd").state = "CLOSED"
    elif failure == "duplicate":
        custody._resources.append(SimpleNamespace(kind="pidfd", value=custody.handle, state="OWNED"))
    elif failure == "stale_duplicate":
        custody._resources.append(SimpleNamespace(kind="pidfd", value=object(), state="CLOSED"))
    elif failure == "identity":
        custody.handle.identity = replace(binding.startup.process, pid=binding.startup.process.pid + 1)
    else:
        custody._owner = (os.getpid() + 1, threading.current_thread())
    try:
        deadline = time.monotonic() + (0.01 if failure == "timeout" else -1.0 if failure == "expired_ready" else 1.0)
        with pytest.raises(transport.PermissionGrantProcessUnknown):
            process.await_custody(deadline=deadline)
        with pytest.raises(transport.PermissionGrantProcessUnknown):
            process.await_custody(deadline=deadline)
        assert process.failed and len(executor.requests) == 1
    finally:
        left.close()
        right.close()


@pytest.mark.parametrize("change", ["blocking", "deadline", "type", "retained", "pid", "handle"])
def test_invalid_adoption_is_effect_free(monkeypatch, change):
    monkeypatch.setattr(transport, "TdsChildContainmentExecutor", FakeExecutor)
    binding, *_ = fixture()
    monkeypatch.setattr(transport, "deadline_nanoseconds", lambda value: binding.operation_deadline_ns)
    left, right = socket.socketpair()
    left.setblocking(change == "blocking")
    custody = TdsChildProcess(SimpleNamespace(stdout=None, pid=binding.startup.process.pid), None, ())
    if change == "retained":
        custody.retain_socket(left)
    if change == "pid":
        custody.process.pid += 1
    if change == "handle":
        custody.handle = object()
    args = dict(
        custody=custody,
        channel=left,
        binding=binding,
        startup_deadline=1.0,
        operation_deadline=2.0,
        termination_timeout=1.0,
    )
    if change == "deadline":
        args["startup_deadline"] = True
    if change == "type":
        args["channel"] = object()
    before = len(custody._resources)
    try:
        with pytest.raises(ValueError):
            transport.SqlClientPermissionGrantProcess.adopt(**args)
    finally:
        left.close()
        right.close()
    assert len(custody._resources) == before


@pytest.mark.parametrize("checks", range(7))
@pytest.mark.parametrize("terminal_tail", [b"", b"x"])
def test_complete_and_early_release_transcripts(adopted, checks, terminal_tail):
    process, peer, _, binding, _ = adopted
    frozen_binding, events = transcript()
    assert frozen_binding == binding
    release_body = strict_json_object(events[-2][0])["body"]
    release = encode_permission_message(binding, PermissionWireKind.RELEASE, checks + 4, release_body)
    released = encode_permission_message(binding, PermissionWireKind.RELEASED, checks + 4, release_body)
    selected = events[: 7 + checks * 2] + [
        (release, "PARENT_TO_CHILD"),
        (released, "CHILD_TO_PARENT"),
    ]
    credentials = b"PRIVATE_CREDENTIAL_CANARY"
    terminal_closed = False
    for payload, direction in selected:
        if direction == "CHILD_TO_PARENT":
            value = strict_json_object(payload)
            is_released = value["kind"] == PermissionWireKind.RELEASED.value
            peer.sendall(frame_permission_payload(payload) + (terminal_tail if is_released else b""))
            if is_released and not terminal_tail:
                peer.shutdown(socket.SHUT_WR)
                terminal_closed = True
            if is_released and terminal_tail:
                with pytest.raises(transport.PermissionGrantProcessUnknown):
                    process.receive_public(PermissionWireKind.RELEASED, value["ordinal"])
                assert process.failed
                return
            observed = process.receive_public(PermissionWireKind(value["kind"]), value["ordinal"])
            assert observed.kind.value == value["kind"]
        elif direction == "PARENT_TO_CHILD":
            value = strict_json_object(payload)
            observed = process.send_public(PermissionWireKind(value["kind"]), value["ordinal"], value["body"])
            assert _recv_frame(peer) == payload
            assert observed.kind.value == value["kind"]
        else:
            process.send_credentials(credentials)
            assert _recv_frame(peer) == credentials
    if not terminal_closed:
        peer.shutdown(socket.SHUT_WR)
    process.observe_eof()
    assert process._eof
    assert len(process.public_attempts) == len(selected) - 1
    assert all(attempt.complete for attempt in process.public_attempts)
    assert process.credential_attempt.complete
    rendered = repr((process, process.public_attempts, process.credential_attempt))
    assert "PRIVATE_CREDENTIAL_CANARY" not in rendered
    assert credentials.hex() not in rendered


def test_partial_prefix_timeout_is_retained_and_failure_is_sticky(adopted):
    process, peer, custody, _, executor = adopted
    peer.send(b"\x00\x01")
    with pytest.raises(transport.PermissionGrantProcessUnknown) as captured:
        process.receive_public(PermissionWireKind.STARTUP, 0, deadline=time.monotonic() + 0.02)
    assert captured.value.process is process
    assert process.public_attempts[-1].prefix == b"\x00\x01"
    assert process.public_attempts[-1].transferred == 2
    assert process.failed and custody._poisoned
    assert len(executor.requests) == 1
    original_cause = process._cause
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.receive_public(PermissionWireKind.STARTUP, 0)
    assert process._cause is original_cause
    assert len(executor.requests) == 1


def test_declared_cap_is_checked_before_body_receive(adopted):
    process, peer, _, _, _ = adopted
    peer.send((CONTROL_LIMIT + 1).to_bytes(4, "big"))
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.receive_public(PermissionWireKind.STARTUP, 0)
    attempt = process.public_attempts[-1]
    assert attempt.prefix == (CONTROL_LIMIT + 1).to_bytes(4, "big")
    assert attempt.payload == b""
    assert attempt.transferred == 4


def test_queued_second_frame_is_a_sticky_framing_failure(adopted):
    process, peer, _, _, _ = adopted
    _, events = transcript()
    startup = events[0][0]
    peer.sendall(frame_permission_payload(startup) + b"x")
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.receive_public(PermissionWireKind.STARTUP, 0)
    assert process.public_attempts[-1].payload == startup
    assert not process.public_attempts[-1].complete


def test_outbound_attempt_is_admitted_before_transport_failure(adopted, monkeypatch):
    process, _, _, _, executor = adopted
    _, events = transcript()
    startup, request = events[0][0], events[1][0]
    process._wire.accept(startup, direction="CHILD_TO_PARENT")
    value = strict_json_object(request)
    monkeypatch.setattr(transport, "_socket_ready", lambda *args, **kwargs: False)
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.send_public(PermissionWireKind.REQUEST, 1, value["body"])
    attempt = process.public_attempts[-1]
    assert attempt.payload == request and attempt.encoded and not attempt.complete
    assert attempt.transferred == 0
    assert process._wire.failed
    assert len(executor.requests) == 1


@pytest.mark.parametrize("supplied,ceiling", [(None, "startup"), (1.0, "supplied")])
def test_startup_deadline_uses_the_original_ceiling_or_shorter_supply(adopted, monkeypatch, supplied, ceiling):
    process, _, _, _, _ = adopted
    captured = []
    monkeypatch.setattr(transport, "_socket_ready", lambda channel, deadline: captured.append(deadline) or False)
    deadline = None if supplied is None else process._startup_deadline - supplied
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.receive_public(PermissionWireKind.STARTUP, 0, deadline=deadline)
    assert captured == [process._startup_deadline if ceiling == "startup" else deadline]


def test_operation_deadline_cannot_be_renewed(adopted, monkeypatch):
    process, _, _, _, _ = adopted
    _, events = transcript()
    process._wire.accept(events[0][0], direction="CHILD_TO_PARENT")
    request = strict_json_object(events[1][0])
    captured = []
    monkeypatch.setattr(
        transport, "_socket_ready", lambda channel, deadline, **kwargs: captured.append(deadline) or False
    )
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.send_public(
            PermissionWireKind.REQUEST, 1, request["body"], deadline=process._operation_deadline + 100.0
        )
    assert captured == [process._operation_deadline]


def test_partial_outbound_progress_is_exact_and_never_replayed(adopted, monkeypatch):
    process, _, _, _, _ = adopted
    _, events = transcript()
    process._wire.accept(events[0][0], direction="CHILD_TO_PARENT")
    request = strict_json_object(events[1][0])
    readiness = iter((True, False))
    sends = []
    monkeypatch.setattr(transport, "_socket_ready", lambda *args, **kwargs: next(readiness))

    def partial(channel, value):
        sends.append(bytes(value))
        return 3

    monkeypatch.setattr(socket.socket, "send", partial)
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.send_public(PermissionWireKind.REQUEST, 1, request["body"])
    assert process.public_attempts[-1].transferred == 3 and len(sends) == 1
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.send_public(PermissionWireKind.REQUEST, 1, request["body"])
    assert len(sends) == 1


@pytest.mark.parametrize("wire", [b"\x00\x00\x00\x03x", b"\x00\x00\x00\x03x\x00"])
def test_partial_body_and_early_eof_are_retained(adopted, wire):
    process, peer, _, _, _ = adopted
    peer.sendall(wire)
    peer.shutdown(socket.SHUT_WR)
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.receive_public(PermissionWireKind.STARTUP, 0)
    attempt = process.public_attempts[-1]
    assert attempt.prefix == b"\x00\x00\x00\x03" and attempt.payload == wire[4:]
    assert attempt.transferred == len(wire)


@pytest.mark.parametrize("length", [0, 1])
def test_zero_and_aggregate_caps_precede_body_allocation(adopted, length):
    process, peer, _, _, _ = adopted
    if length:
        process._wire.total = TOTAL_LIMIT - 4
    peer.sendall(length.to_bytes(4, "big"))
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.receive_public(PermissionWireKind.STARTUP, 0)
    attempt = process.public_attempts[-1]
    assert attempt.payload == b"" and attempt.transferred == 4


@pytest.mark.parametrize("failure_point", ["codec", "state", "frame", "select", "send", "recv", "bookkeeping"])
def test_each_fallible_outbound_or_inbound_dependency_closes_sticky_unknown(adopted, monkeypatch, failure_point):
    process, peer, custody, _, executor = adopted
    _, events = transcript()
    marker = RuntimeError(failure_point)

    def fail(*args, **kwargs):
        raise marker

    if failure_point in {"codec", "state", "frame", "select", "send", "bookkeeping"}:
        process._wire.accept(events[0][0], direction="CHILD_TO_PARENT")
        request = strict_json_object(events[1][0])
        if failure_point == "codec":
            monkeypatch.setattr(transport.permission_wire, "encode_permission_message", fail)
        elif failure_point == "state":
            monkeypatch.setattr(process._wire, "accept", fail)
        elif failure_point == "frame":
            monkeypatch.setattr(transport.permission_wire, "frame_permission_payload", fail)
        elif failure_point == "select":
            monkeypatch.setattr(transport, "_socket_ready", fail)
        elif failure_point == "send":
            monkeypatch.setattr(socket.socket, "send", fail)
        else:
            monkeypatch.setattr(process, "_replace_attempt", fail)

        def call():
            process.send_public(PermissionWireKind.REQUEST, 1, request["body"])
    else:
        peer.sendall(b"\x00\x00\x00\x01")
        monkeypatch.setattr(socket.socket, "recv", fail)

        def call():
            process.receive_public(PermissionWireKind.STARTUP, 0)

    with pytest.raises(transport.PermissionGrantProcessUnknown):
        call()
    assert process._cause is marker and process.failed and custody._poisoned and len(executor.requests) == 1


def test_clock_and_settlement_event_failures_close_sticky_unknown(adopted, monkeypatch):
    process, _, custody, _, executor = adopted
    clock_failure = RuntimeError("clock")
    monkeypatch.setattr(transport, "_socket_ready", lambda *args, **kwargs: (_ for _ in ()).throw(clock_failure))
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.receive_public(PermissionWireKind.STARTUP, 0)
    assert process._cause is clock_failure and custody._poisoned and len(executor.requests) == 1

    executor.exit = None
    executor.done.set()
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.contain()
    assert process._cause is clock_failure and len(executor.requests) == 1


def test_settlement_event_failure_closes_sticky_unknown(adopted, monkeypatch):
    process, _, custody, _, executor = adopted
    marker = RuntimeError("event")
    process._eof = True

    def fail(*args, **kwargs):
        raise marker

    monkeypatch.setattr(executor.done, "wait", fail)
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.settle(natural_deadline=time.monotonic() + 0.5)
    assert process._cause is marker and custody._poisoned and len(executor.requests) == 1


def test_credentials_retain_only_counts_on_partial_failure(adopted, monkeypatch, caplog):
    process, _, _, _, _ = adopted
    _, events = transcript()
    process._wire.accept(events[0][0], direction="CHILD_TO_PARENT")
    process._wire.accept(events[1][0], direction="PARENT_TO_CHILD")
    process._wire.accept(events[2][0], direction="CHILD_TO_PARENT")
    sentinel = b"PRIVATE_CREDENTIAL_CANARY"
    monkeypatch.setattr(transport, "_socket_ready", lambda *args, **kwargs: False)
    with pytest.raises(transport.PermissionGrantProcessUnknown) as captured:
        process.send_credentials(sentinel)
    attempt = process.credential_attempt
    assert (attempt.payload_size, attempt.framed_size, attempt.transferred, attempt.complete) == (
        len(sentinel),
        len(sentinel) + 4,
        0,
        False,
    )
    rendered = repr((process, captured.value, attempt, process.public_attempts, caplog.text))
    assert sentinel.decode() not in rendered
    assert sentinel.hex() not in rendered


def test_credential_progress_is_replaced_after_every_positive_write(adopted, monkeypatch):
    process, peer, _, _, _ = adopted
    _, events = transcript()
    process._wire.accept(events[0][0], direction="CHILD_TO_PARENT")
    process._wire.accept(events[1][0], direction="PARENT_TO_CHILD")
    process._wire.accept(events[2][0], direction="CHILD_TO_PARENT")
    process._channel.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
    calls = 0

    def readiness(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            assert peer.recv(4) == CREDENTIAL_LIMIT.to_bytes(4, "big")
        return calls < 3

    monkeypatch.setattr(transport, "_socket_ready", readiness)
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.send_credentials(b"x" * CREDENTIAL_LIMIT)
    attempt = process.credential_attempt
    assert 4 < attempt.transferred < attempt.framed_size


def test_foreign_thread_poison_the_same_process(adopted):
    process, _, custody, _, executor = adopted
    errors = []
    thread = threading.Thread(
        target=lambda: errors.append(pytest.raises(transport.PermissionGrantProcessUnknown, process.observe_eof))
    )
    thread.start()
    thread.join()
    assert errors and process.failed and custody._poisoned and len(executor.requests) == 1


def test_same_thread_containment_reentry_poison_the_active_operation(adopted, monkeypatch):
    process, _, custody, binding, executor = adopted
    _, events = transcript()
    process._wire.accept(events[0][0], direction="CHILD_TO_PARENT")
    request = strict_json_object(events[1][0])
    executor.exit = TdsChildExit(binding.startup.process, -9, True)
    custody.exit = executor.exit
    custody._closed = True
    executor.done.set()
    monkeypatch.setattr(transport, "_socket_ready", lambda *args, **kwargs: process.contain())
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.send_public(PermissionWireKind.REQUEST, 1, request["body"])
    assert process.failed and custody._poisoned and len(executor.requests) == 1


def test_containment_remains_available_after_failure(adopted, monkeypatch):
    process, _, custody, binding, executor = adopted
    monkeypatch.setattr(transport, "_socket_ready", lambda *args, **kwargs: False)
    with pytest.raises(transport.PermissionGrantProcessUnknown):
        process.receive_public(PermissionWireKind.STARTUP, 0)
    executor.exit = TdsChildExit(binding.startup.process, -9, True)
    custody.exit = executor.exit
    custody._closed = True
    executor.done.set()
    assert process.contain() is executor.exit


def test_settlement_uses_same_executor_then_close_is_idempotent(adopted):
    process, _, custody, _, executor = adopted
    process._eof = True
    result = process.settle(natural_deadline=time.monotonic() + 0.5)
    assert result is executor.exit and executor.settlements
    assert custody._closed
    process.close()
    process.close()


def test_close_before_exact_settlement_retains_process(adopted):
    process, _, _, _, _ = adopted
    with pytest.raises(transport.PermissionGrantProcessUnknown) as captured:
        process.close()
    assert captured.value.process is process


def test_unknown_remains_compatible_with_context_manager_traceback_assignment(adopted):
    process, *_ = adopted

    with pytest.raises(transport.PermissionGrantProcessUnknown) as captured:
        with pytest.raises(RuntimeError):
            raise transport.PermissionGrantProcessUnknown(process)

    assert captured.value.process is process


@pytest.mark.skipif(sys.platform != "linux", reason="requires actual Linux pidfd custody")
@pytest.mark.parametrize("natural", [True, False])
def test_actual_linux_settlement_uses_the_adopted_executor_and_reaps_once(natural):
    LinuxTdsProcess.admit()
    delay = 0.05 if natural else 30.0
    child = subprocess.Popen(
        [sys.executable, "-I", "-S", "-B", "-c", f"import time;time.sleep({delay})"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    identity = LinuxTdsProcess.identify(child.pid)
    frozen, *_ = fixture()
    operation_deadline = time.monotonic() + 5.0
    startup = replace(frozen.startup, process=identity)
    binding = PermissionWireBinding(
        frozen.request,
        frozen.operation,
        startup,
        frozen.execution_owner,
        deadline_nanoseconds(operation_deadline),
    )
    parent, peer = socket.socketpair()
    parent.setblocking(False)
    peer.setblocking(False)
    custody = TdsChildProcess(child, None, ())
    process = transport.SqlClientPermissionGrantProcess.adopt(
        custody,
        parent,
        binding,
        startup_deadline=time.monotonic() + 1.0,
        operation_deadline=operation_deadline,
        termination_timeout=1.0,
    )
    try:
        process.await_custody(deadline=process._startup_deadline)
        process._eof = True
        result = process.settle(natural_deadline=time.monotonic() + (1.0 if natural else 0.03))
        assert result.reaped and result.exit_code == (0 if natural else -9)
        assert process.exit is result and custody._closed
        with pytest.raises(ChildProcessError):
            os.waitpid(child.pid, os.WNOHANG)
        process.close()
    finally:
        peer.close()
