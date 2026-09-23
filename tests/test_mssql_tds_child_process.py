"""Shared child ownership does not confuse descriptor closure with containment."""

import os
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_tds_child_process import TdsChildProcess
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from tests.test_mssql_tds_supervisor_process import PROCESS


def test_supplied_descriptors_detach_before_unknown_close(monkeypatch):
    read_fd, write_fd = os.pipe()
    child = TdsChildProcess(SimpleNamespace(stdout=None), None, (write_fd,))
    close = os.close
    calls = []

    def lose_ack(fd):
        calls.append(fd)
        close(fd)
        raise OSError("lost close acknowledgement")

    monkeypatch.setattr(os, "close", lose_ack)
    try:
        with pytest.raises(OSError, match="acknowledgement"):
            child.close_descriptor(write_fd)
        assert child.descriptors == ()
        with pytest.raises(ValueError, match="descriptor"):
            child.close_descriptor(write_fd)
        assert calls == [write_fd]
    finally:
        close(read_fd)


def test_supplied_descriptor_set_closed_after_exact_child_reaping():
    descriptors = os.pipe()
    calls = []
    handle = SimpleNamespace(
        identity=PROCESS,
        wait=lambda **kwargs: SimpleNamespace(reaped=True, exit_code=0),
        contain=lambda **kwargs: pytest.fail("already reaped"),
        close=lambda: calls.append("handle"),
    )
    process = SimpleNamespace(stdout=None, returncode=None)
    child = TdsChildProcess(process, handle, descriptors)
    exit_observation = child.wait(deadline=monotonic() + 1)
    assert child.terminate(deadline=monotonic() + 1) == exit_observation
    child.close()
    assert process.returncode == 0 and calls == ["handle"]
    for fd in descriptors:
        with pytest.raises(OSError):
            os.fstat(fd)


@pytest.mark.parametrize("descriptors_first", [False, True])
def test_close_error_precedence_preserves_all_resource_attempts(monkeypatch, descriptors_first):
    events = []

    def fail(label):
        events.append(label)
        raise OSError(label)

    handle = SimpleNamespace(
        identity=PROCESS,
        wait=lambda **kwargs: SimpleNamespace(reaped=True, exit_code=0),
        close=lambda: fail("handle"),
    )
    process = SimpleNamespace(stdout=SimpleNamespace(close=lambda: fail("stdout")), returncode=None)
    cache = SimpleNamespace(cleanup=lambda: fail("cache"))
    child = TdsChildProcess(process, handle, (123, 456), cache)
    child.wait(deadline=monotonic() + 1)
    monkeypatch.setattr(os, "close", lambda fd: fail(str(fd)))
    with pytest.raises(OSError, match="123" if descriptors_first else "handle"):
        child.close(descriptors_first=descriptors_first)
    assert events == (["123", "456", "handle"] if descriptors_first else ["handle", "123", "456"]) + ["stdout", "cache"]
    assert child.descriptors == ()
    with pytest.raises(WindowOutcomeUnknown, match="tds_close_unknown"):
        child.close()
    assert len(events) == 5


@pytest.mark.parametrize("method", ["wait", "terminate", "close_descriptor", "close"])
def test_foreign_process_cannot_touch_child_resources(monkeypatch, method):
    from dpone.adapters import mssql_tds_child_process as module

    child = TdsChildProcess(SimpleNamespace(stdout=None), None, (123,))
    monkeypatch.setattr(module.os, "getpid", lambda: -1)
    with pytest.raises(ValueError, match="owner_invalid"):
        if method in ("wait", "terminate"):
            getattr(child, method)(deadline=monotonic() + 1)
        elif method == "close_descriptor":
            child.close_descriptor(123)
        else:
            child.close()
    assert child.descriptors == (123,) and child.exit is None


@pytest.mark.parametrize(
    "proof", [SimpleNamespace(reaped=False, exit_code=0), SimpleNamespace(reaped=True, exit_code=None)]
)
def test_incomplete_reaping_keeps_every_resource(proof):
    from dpone.contracts.bounded_window import WindowOutcomeUnknown

    handle = SimpleNamespace(identity=PROCESS, wait=lambda **kwargs: proof)
    process = SimpleNamespace(stdout=None, returncode=None)
    child = TdsChildProcess(process, handle, (123,))
    with pytest.raises(WindowOutcomeUnknown, match="reaping_unknown"):
        child.wait(deadline=monotonic() + 1)
    with pytest.raises(WindowOutcomeUnknown, match="close_unsettled"):
        child.close()
    assert child.descriptors == (123,) and process.returncode is None


def test_unresolved_expired_deadline_retains_inspectable_capability():
    from dpone.adapters.mssql_tds_child_process import UnresolvedPythonTdsLaunch
    from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

    calls = []
    handle = SimpleNamespace(identity=PROCESS, contain=lambda **kwargs: calls.append("contain"))
    process = SimpleNamespace(stdout=None, returncode=None)
    launch = UnresolvedPythonTdsLaunch(process, (123,), handle)
    with pytest.raises(TdsLaunchUnknown) as caught:
        launch.contain(deadline=monotonic() - 1)
    assert caught.value.launch is launch
    assert launch.process is process and launch.handle is handle and launch.descriptors == (123,)
    assert process.returncode is None and calls == []
    with pytest.raises(TdsLaunchUnknown):
        launch.close()
    assert launch.descriptors == (123,)
