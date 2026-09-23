"""Mechanical readiness extraction preserves the existing OBSERVE contract."""

import importlib
import socket

import pytest


def test_private_readiness_module_is_initially_required():
    importlib.import_module("dpone.adapters._mssql_tds_socket_readiness")


@pytest.mark.parametrize("writing", [False, True])
def test_socket_ready_uses_exact_select_direction_and_one_absolute_deadline(monkeypatch, writing):
    readiness = importlib.import_module("dpone.adapters._mssql_tds_socket_readiness")
    left, right = socket.socketpair()
    calls = []
    clocks = iter((3.0, 4.0))

    def selected(readers, writers, errors, timeout):
        calls.append((readers, writers, errors, timeout))
        return ([], [left], []) if writing else ([left], [], [])

    monkeypatch.setattr(readiness.time, "monotonic", lambda: next(clocks))
    monkeypatch.setattr(readiness.select, "select", selected)
    try:
        assert readiness._socket_ready(left, 5.0, writing=writing)
    finally:
        left.close()
        right.close()
    assert calls == [([], [left], [], 2.0) if writing else ([left], [], [], 2.0)]


def test_socket_ready_expired_and_empty_select_do_not_read_an_extra_clock(monkeypatch):
    readiness = importlib.import_module("dpone.adapters._mssql_tds_socket_readiness")
    left, right = socket.socketpair()
    selected = []
    try:
        monkeypatch.setattr(readiness.time, "monotonic", lambda: 5.0)
        monkeypatch.setattr(readiness.select, "select", lambda *args: selected.append(args))
        assert not readiness._socket_ready(left, 5.0)
        assert selected == []
        clocks = iter((3.0, AssertionError("second clock")))
        monkeypatch.setattr(readiness.time, "monotonic", lambda: next(clocks))
        monkeypatch.setattr(readiness.select, "select", lambda *args: ([], [], []))
        assert not readiness._socket_ready(left, 5.0)
    finally:
        left.close()
        right.close()


def test_socket_ready_rejects_readiness_at_deadline_and_preserves_select_error(monkeypatch):
    readiness = importlib.import_module("dpone.adapters._mssql_tds_socket_readiness")
    left, right = socket.socketpair()
    try:
        clocks = iter((3.0, 5.0))
        monkeypatch.setattr(readiness.time, "monotonic", lambda: next(clocks))
        monkeypatch.setattr(readiness.select, "select", lambda *args: ([left], [], []))
        assert not readiness._socket_ready(left, 5.0)

        failure = OSError("select-canary")
        monkeypatch.setattr(readiness.time, "monotonic", lambda: 3.0)

        def fail(*args):
            raise failure

        monkeypatch.setattr(readiness.select, "select", fail)
        with pytest.raises(OSError) as captured:
            readiness._socket_ready(left, 5.0)
        assert captured.value is failure
    finally:
        left.close()
        right.close()


def test_queued_input_treats_data_and_eof_as_readable(monkeypatch):
    readiness = importlib.import_module("dpone.adapters._mssql_tds_socket_readiness")
    left, right = socket.socketpair()
    calls = []

    def selected(readers, writers, errors, timeout):
        calls.append((readers, writers, errors, timeout))
        return readers, writers, errors

    monkeypatch.setattr(readiness.select, "select", selected)
    try:
        assert readiness._has_queued_input(left)
        right.close()
        assert readiness._has_queued_input(left)
    finally:
        left.close()
    assert calls == [([left], [], [], 0), ([left], [], [], 0)]


def test_real_queued_eof_and_select_exception_identity(monkeypatch):
    readiness = importlib.import_module("dpone.adapters._mssql_tds_socket_readiness")
    left, right = socket.socketpair()
    right.close()
    try:
        assert readiness._has_queued_input(left)
        marker = OSError("queued-select-canary")

        def fail(*args):
            raise marker

        monkeypatch.setattr(readiness.select, "select", fail)
        with pytest.raises(OSError) as captured:
            readiness._has_queued_input(left)
        assert captured.value is marker
    finally:
        left.close()


def test_real_socketpair_read_and_write_directions():
    readiness = importlib.import_module("dpone.adapters._mssql_tds_socket_readiness")
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    try:
        assert readiness._socket_ready(left, readiness.time.monotonic() + 1.0, writing=True)
        assert not readiness._has_queued_input(left)
        right.send(b"x")
        assert readiness._socket_ready(left, readiness.time.monotonic() + 1.0)
        assert readiness._has_queued_input(left)
    finally:
        left.close()
        right.close()


@pytest.mark.parametrize("deadline", [True, float("nan"), float("inf")])
def test_invalid_values_are_not_normalized(monkeypatch, deadline):
    readiness = importlib.import_module("dpone.adapters._mssql_tds_socket_readiness")
    marker = TypeError("native-canary")
    monkeypatch.setattr(readiness.time, "monotonic", lambda: 0.0)

    def fail(*args):
        raise marker

    monkeypatch.setattr(readiness.select, "select", fail)
    with pytest.raises((TypeError, ValueError, OSError)) as captured:
        readiness._socket_ready(object(), deadline)
    if captured.value is marker:
        assert captured.value is marker


@pytest.mark.parametrize("deadline", [None, "1", object()])
def test_wrong_deadline_types_preserve_native_exception(deadline):
    readiness = importlib.import_module("dpone.adapters._mssql_tds_socket_readiness")
    with pytest.raises(TypeError):
        readiness._socket_ready(object(), deadline)


@pytest.mark.parametrize("writing,expected", [(0, "read"), (1, "write"), ("write", "write")])
def test_writing_values_are_used_without_normalization(monkeypatch, writing, expected):
    readiness = importlib.import_module("dpone.adapters._mssql_tds_socket_readiness")
    left, right = socket.socketpair()
    seen = []

    def selected(readers, writers, errors, timeout):
        seen.append("write" if writers else "read")
        return readers, writers, errors

    monkeypatch.setattr(readiness.select, "select", selected)
    monkeypatch.setattr(readiness.time, "monotonic", iter((0.0, 0.5)).__next__)
    try:
        assert readiness._socket_ready(left, 1.0, writing=writing)
    finally:
        left.close()
        right.close()
    assert seen == [expected]


def test_helpers_remain_private_and_stdlib_only():
    readiness = importlib.import_module("dpone.adapters._mssql_tds_socket_readiness")
    source = readiness.__loader__.get_source(readiness.__name__)
    assert "import dpone" not in source
    assert not hasattr(importlib.import_module("dpone.adapters"), "_socket_ready")


def test_observe_wrappers_preserve_timeout_and_quiet_errors(monkeypatch):
    readiness = importlib.import_module("dpone.adapters._mssql_tds_socket_readiness")
    observe = importlib.import_module("dpone.adapters.mssql_sqlclient_observe_transport")
    left, right = socket.socketpair()
    try:
        monkeypatch.setattr(readiness, "_socket_ready", lambda *args, **kwargs: False)
        with pytest.raises(TimeoutError, match="mssql_native.sqlclient_observe_invalid") as captured:
            observe._wait(left, 1.0)
        assert captured.value.__cause__ is None
        monkeypatch.setattr(readiness, "_has_queued_input", lambda *args: True)
        with pytest.raises(ValueError, match="mssql_native.sqlclient_observe_invalid"):
            observe.require_quiet(left)

        marker = OSError("observe-select-canary")

        def fail(*args, **kwargs):
            raise marker

        monkeypatch.setattr(readiness, "_socket_ready", fail)
        with pytest.raises(OSError) as captured:
            observe._wait(left, 1.0)
        assert captured.value is marker
        monkeypatch.setattr(readiness, "_has_queued_input", fail)
        with pytest.raises(OSError) as captured:
            observe.require_quiet(left)
        assert captured.value is marker
    finally:
        left.close()
        right.close()
