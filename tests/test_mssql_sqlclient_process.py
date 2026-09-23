"""Phased local transport unit checks; fake handles do not certify live SQL."""

import os
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_sqlclient_process import SqlClientChildProcess
from dpone.contracts.mssql_sqlclient_launch import (
    SqlClientDescriptors,
    SqlClientLaunch,
    SqlClientReady,
    encode_ready,
    launch_digest,
)
from dpone.contracts.mssql_tds_frames import encode_message
from dpone.contracts.mssql_tds_worker import TdsProcessIdentity


@pytest.fixture
def child(monkeypatch):
    pairs = [os.pipe() for _ in range(5)]
    for pair in pairs:
        for fd in pair:
            os.set_blocking(fd, False)
    parent = (pairs[0][0], pairs[1][1], pairs[2][0], pairs[3][1], pairs[4][0])
    identity = TdsProcessIdentity("a" * 64, "11111111-1111-4111-8111-111111111111", 234, 1)
    handle = SimpleNamespace(
        identity=identity,
        close=lambda: None,
        wait=lambda **kw: SimpleNamespace(reaped=True, exit_code=0),
        contain=lambda **kw: SimpleNamespace(reaped=True, exit_code=-9),
    )
    deadline = time.monotonic() + 5
    launch = SqlClientLaunch(
        1,
        "22222222-2222-4222-8222-222222222222",
        "a" * 64,
        "b" * 64,
        "c" * 64,
        identity,
        123,
        int(deadline * 10**9),
        int(deadline * 10**9),
        8 * 1024**3,
        SqlClientDescriptors(100, 101, 102, 103, 104, 105),
    )
    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_process.LinuxTdsProcess.identify", lambda pid: identity)
    worker = SqlClientChildProcess(SimpleNamespace(stdout=None, returncode=None), handle, parent, launch=launch)
    yield SimpleNamespace(worker=worker, pairs=pairs, deadline=deadline, launch=launch)
    for pair in pairs:
        for fd in pair:
            try:
                os.close(fd)
            except OSError:
                pass


def emit(child, index, data, close=True):
    fd = child.pairs[index][1]
    os.write(fd, encode_message(data, max_payload=262144))
    if close:
        os.close(fd)


def ready(child):
    return SqlClientReady(
        1, launch_digest(child.launch), child.launch.process, 8 * 1024**3, 9, 0, "8.0.31", 536870912, False, "Disable"
    )


def start(child):
    emit(child, 0, encode_ready(ready(child)))
    child.worker.startup(deadline=child.deadline)


def test_phases_raw_result_and_reap(child):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    assert os.read(child.pairs[1][0], 99) == encode_message(b"synthetic", max_payload=1024**2)
    assert os.read(child.pairs[1][0], 1) == b""
    emit(child, 2, b"session")
    assert child.worker.observe_session(deadline=child.deadline) == b"session"
    child.worker.send_grant(b"grant", deadline=child.deadline)
    emit(child, 4, b"result")
    assert child.worker.receive_result(deadline=child.deadline) == b"result"
    assert child.worker.received_result == b"result"
    assert child.worker.wait(deadline=child.deadline).reaped
    child.worker.close()


@pytest.mark.parametrize("method", ["receive_result", "receive_session_or_result"])
@pytest.mark.parametrize("boundary", ["eof_read", "finish"])
def test_complete_result_survives_expiry_without_success(child, monkeypatch, method, boundary):
    from dpone.adapters import mssql_tds_channels as channels

    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    emit(child, 4, b"exact-complete-result")
    now = 0.0
    original_read = os.read
    original_finish = channels.TdsMessageFrame.finish

    def read(fd, size):
        nonlocal now
        chunk = original_read(fd, size)
        if boundary == "eof_read" and fd == child.pairs[4][0] and not chunk:
            now = child.deadline
        return chunk

    def finish(frame):
        nonlocal now
        body = original_finish(frame)
        if boundary == "finish":
            now = child.deadline
        return body

    monkeypatch.setattr(channels, "time", SimpleNamespace(monotonic=lambda: now))
    monkeypatch.setattr(os, "read", read)
    monkeypatch.setattr(channels.TdsMessageFrame, "finish", finish)
    with pytest.raises(ValueError, match="deadline"):
        getattr(child.worker, method)(deadline=child.deadline)
    assert child.worker.received_result == b"exact-complete-result"
    with pytest.raises(ValueError, match="phase"):
        getattr(child.worker, method)(deadline=child.deadline + 60)


@pytest.mark.parametrize("method", ["receive_result", "receive_session_or_result"])
@pytest.mark.parametrize("wire", [b"", b"\x00\x00\x00\x05x", b"\x00\x00\x00\x01xy"])
def test_invalid_result_eof_never_retained(child, method, wire):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    os.write(child.pairs[4][1], wire)
    os.close(child.pairs[4][1])
    with pytest.raises(ValueError):
        getattr(child.worker, method)(deadline=child.deadline)
    assert child.worker.received_result is None


@pytest.mark.parametrize("method", ["receive_result", "receive_session_or_result"])
def test_complete_frame_without_eof_never_retained(child, method):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    emit(child, 4, b"incomplete-transport", close=False)
    with pytest.raises(ValueError, match="deadline"):
        getattr(child.worker, method)(deadline=time.monotonic() + 0.02)
    assert child.worker.received_result is None


@pytest.mark.parametrize("method", ["receive_result", "receive_session_or_result"])
def test_retention_callback_failure_poisoned_phase(child, monkeypatch, method):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    emit(child, 4, b"complete")

    def fail(body):
        raise RuntimeError("test.retention_failed")

    monkeypatch.setattr(child.worker, "_retain_result", fail)
    with pytest.raises(RuntimeError, match="test.retention_failed"):
        getattr(child.worker, method)(deadline=child.deadline)
    with pytest.raises(ValueError, match="phase"):
        getattr(child.worker, method)(deadline=child.deadline)
    assert child.worker.received_result is None


def test_session_never_invokes_result_retention(child, monkeypatch):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    emit(child, 2, b"session")

    def fail(body):
        raise AssertionError("session is not a result")

    monkeypatch.setattr(child.worker, "_retain_result", fail)
    assert child.worker.receive_session_or_result(deadline=child.deadline) == ("session", b"session")
    assert child.worker.received_result is None


def test_no_credential_before_ready(child):
    with pytest.raises(ValueError, match="phase"):
        child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    with pytest.raises(BlockingIOError):
        os.read(child.pairs[1][0], 1)


def test_wrong_launch_no_retry(child):
    emit(child, 0, encode_ready(replace(ready(child), launch_sha256="e" * 64)))
    with pytest.raises(ValueError):
        child.worker.startup(deadline=child.deadline)
    with pytest.raises(ValueError, match="phase"):
        child.worker.startup(deadline=child.deadline)
    with pytest.raises(ValueError, match="phase"):
        child.worker.send_credentials(b"synthetic", deadline=child.deadline)


def test_ready_requires_eof(child):
    emit(child, 0, encode_ready(ready(child)), close=False)
    with pytest.raises(ValueError, match="deadline"):
        child.worker.startup(deadline=time.monotonic() + 0.02)


def test_original_deadline_caps_later_call(child, monkeypatch):
    observed = []
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_process.read_worker_message",
        lambda fd, **kw: observed.append(kw["deadline"]) or encode_ready(ready(child)),
    )
    child.worker.startup(deadline=child.deadline + 60)
    assert observed[0] <= child.launch.startup_deadline_ns / 10**9


def test_invalid_delivery_is_latched(child):
    start(child)
    with pytest.raises(ValueError):
        child.worker.send_credentials(b"", deadline=child.deadline)
    with pytest.raises(ValueError, match="phase"):
        child.worker.send_credentials(b"synthetic", deadline=child.deadline)


def test_result_retained_when_close_fails(child, monkeypatch):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    emit(child, 2, b"session")
    child.worker.observe_session(deadline=child.deadline)
    child.worker.send_grant(b"grant", deadline=child.deadline)
    emit(child, 4, b"retained")
    monkeypatch.setattr(child.worker._resources, "close_descriptor", lambda fd: (_ for _ in ()).throw(OSError("close")))
    with pytest.raises(OSError):
        child.worker.receive_result(deadline=child.deadline)
    assert child.worker.received_result == b"retained"
    with pytest.raises(ValueError, match="phase"):
        child.worker.receive_result(deadline=child.deadline)


def test_termination_deadline_cannot_renew(child, monkeypatch):
    calls = []

    def settle(*, deadline):
        calls.append(deadline)
        raise ValueError("injected deadline")

    monkeypatch.setattr(child.worker._resources, "terminate", settle)
    with pytest.raises(ValueError):
        child.worker.terminate(deadline=0)
    with pytest.raises(ValueError):
        child.worker.terminate(deadline=child.deadline)
    assert calls == [0, 0]


def test_cross_thread_owner_cannot_start(child):
    import threading

    errors = []

    def attempt():
        try:
            child.worker.startup(deadline=child.deadline)
        except ValueError as error:
            errors.append(str(error))

    thread = threading.Thread(target=attempt)
    thread.start()
    thread.join()
    assert errors == ["mssql_native.tds_worker_owner_invalid"]


@pytest.mark.parametrize("session_eof", [False, True])
def test_pregrant_result_does_not_wait_for_session_and_forbids_grant(child, session_eof):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    if session_eof:
        os.close(child.pairs[2][1])
    emit(child, 4, b"early-error")
    kind, body = child.worker.receive_session_or_result(deadline=child.deadline)
    assert (kind, body) == ("result", b"early-error")
    assert child.worker.received_result == body
    with pytest.raises(ValueError, match="phase"):
        child.worker.send_grant(b"forbidden", deadline=child.deadline)


def test_empty_job_can_receive_result_without_session_or_grant(child):
    start(child)
    child.worker.send_credentials(b"null-secret-job", deadline=child.deadline)
    emit(child, 4, b"empty-result")
    assert child.worker.receive_result(deadline=child.deadline) == b"empty-result"
    with pytest.raises(BlockingIOError):
        os.read(child.pairs[3][0], 1)


def test_alternative_session_keeps_grant_phase(child):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    emit(child, 2, b"announcement")
    assert child.worker.receive_session_or_result(deadline=child.deadline) == ("session", b"announcement")
    child.worker.send_grant(b"grant", deadline=child.deadline)


def test_result_has_priority_when_both_complete_messages_are_ready(child):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    emit(child, 2, b"announcement")
    emit(child, 4, b"error")
    assert child.worker.receive_session_or_result(deadline=child.deadline) == ("result", b"error")


def test_partial_session_cannot_starve_early_result(child):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    os.write(child.pairs[2][1], b"\0\0\0\x10part")
    emit(child, 4, b"error")
    assert child.worker.receive_session_or_result(deadline=child.deadline) == ("result", b"error")


def test_alternative_reader_failure_poisoning_prevents_retry(child):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    with pytest.raises(ValueError, match="deadline"):
        child.worker.receive_session_or_result(deadline=time.monotonic() + 0.02)
    with pytest.raises(ValueError, match="phase"):
        child.worker.receive_session_or_result(deadline=child.deadline)


def test_partial_result_prevents_grant_even_if_session_complete(child):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    emit(child, 2, b"announcement")
    os.write(child.pairs[4][1], b"\0\0\0\x10part")
    with pytest.raises(ValueError, match="deadline"):
        child.worker.receive_session_or_result(deadline=time.monotonic() + 0.02)
    with pytest.raises(ValueError, match="phase"):
        child.worker.send_grant(b"forbidden", deadline=child.deadline)


def test_early_result_requires_actual_eof(child):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    emit(child, 4, b"error", close=False)
    with pytest.raises(ValueError, match="deadline"):
        child.worker.receive_session_or_result(deadline=time.monotonic() + 0.02)
    assert child.worker.received_result is None


def test_raw_early_result_survives_descriptor_close_failure(child, monkeypatch):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    emit(child, 4, b"error")

    def fail_close(*args):
        raise OSError("synthetic close failure")

    monkeypatch.setattr(child.worker._resources, "close_descriptor", fail_close)
    with pytest.raises(OSError):
        child.worker.receive_session_or_result(deadline=child.deadline)
    assert child.worker.received_result == b"error"
    with pytest.raises(ValueError, match="phase"):
        child.worker.send_grant(b"forbidden", deadline=child.deadline)


@pytest.mark.parametrize("raw", [b"\x7f\xff\xff\xff", b"\0\0\0\x10part", b"\0\0\0\x01xy"])
def test_invalid_early_result_cannot_become_a_grant(child, raw):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    emit(child, 2, b"announcement")
    os.write(child.pairs[4][1], raw)
    os.close(child.pairs[4][1])
    with pytest.raises(ValueError):
        child.worker.receive_session_or_result(deadline=child.deadline)
    assert child.worker.received_result is None
    with pytest.raises(ValueError, match="phase"):
        child.worker.send_grant(b"forbidden", deadline=child.deadline)


def test_result_arriving_while_session_is_drained_still_takes_priority(child, monkeypatch):
    start(child)
    child.worker.send_credentials(b"synthetic", deadline=child.deadline)
    emit(child, 2, b"announcement")
    read = os.read
    injected = False

    def arrival(fd, count):
        nonlocal injected
        chunk = read(fd, count)
        if fd == child.pairs[2][0] and chunk and not injected:
            injected = True
            emit(child, 4, b"early-error")
        return chunk

    monkeypatch.setattr("dpone.adapters.mssql_tds_channels.os.read", arrival)
    assert child.worker.receive_session_or_result(deadline=child.deadline) == ("result", b"early-error")


def test_legacy_declared_launch_and_missing_typed_input_are_readonly(child):
    assert child.worker.declared_launch is child.launch
    assert child.worker.bound_input is None
    with pytest.raises(AttributeError):
        child.worker.declared_launch = child.launch
    with pytest.raises(AttributeError):
        child.worker.bound_input = None


@pytest.mark.parametrize("mismatch", ["fd", "digest", "type"])
def test_child_constructor_rejects_typed_binding_before_resource_ownership(child, monkeypatch, mismatch):
    from dpone.contracts.mssql_sqlclient_input import input_descriptor_digest
    from tests.test_mssql_sqlclient_input_descriptor import descriptor

    source = replace(descriptor(), fd=child.launch.descriptors.input)
    launch = replace(child.launch, input_binding_sha256=input_descriptor_digest(source))
    if mismatch == "fd":
        source = replace(source, fd=launch.descriptors.input + 1)
    elif mismatch == "digest":
        launch = replace(launch, input_binding_sha256="f" * 64)
    else:
        source = object()
    allocated = []
    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_process.TdsChildProcess", lambda *a: allocated.append(a))
    with pytest.raises(ValueError, match="process_binding"):
        SqlClientChildProcess(
            SimpleNamespace(),
            SimpleNamespace(identity=launch.process),
            tuple(range(200, 205)),
            launch=launch,
            bound_input=source,
        )
    assert not allocated


def test_child_metadata_does_not_access_closed_parent_fd(child, monkeypatch):
    from dpone.contracts.mssql_sqlclient_input import input_descriptor_digest
    from tests.test_mssql_sqlclient_input_descriptor import descriptor

    source = replace(descriptor(), fd=child.launch.descriptors.input)
    launch = replace(child.launch, input_binding_sha256=input_descriptor_digest(source))
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_process.TdsChildProcess", lambda *a: SimpleNamespace(identity=launch.process)
    )

    def no_stat(*a):
        raise AssertionError("child FD is not a parent handle")

    monkeypatch.setattr(os, "fstat", no_stat)
    result = SqlClientChildProcess(
        SimpleNamespace(),
        SimpleNamespace(identity=launch.process),
        tuple(range(200, 205)),
        launch=launch,
        bound_input=source,
    )
    assert result.bound_input is source and result.declared_launch is launch
    with pytest.raises(AttributeError):
        result.bound_input = source
