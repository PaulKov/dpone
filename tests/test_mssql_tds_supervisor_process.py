"""Fixed launch configuration and exclusive process/pipe authority."""

import os
import sys
import threading
import time
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_tds_supervisor_process import (
    PythonTdsWorker,
    PythonTdsWorkerLauncher,
    worker_installation_digest,
)
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy
from dpone.contracts.mssql_tds_frames import encode_message
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsProcessIdentity
from dpone.contracts.strict_json import canonical_json_bytes

H = "a" * 64
POLICY = NativeBulkTransportPolicy("mssql_python", "rows", 128 * 1024**2)
IDENTITY = TdsAttemptIdentity(
    "t", "r", 0, 0, H, sha256(canonical_json_bytes(POLICY.to_dict())).hexdigest(), H, H, "db", "stage", "owned", H
)
PROCESS = TdsProcessIdentity(H, "11111111-1111-1111-1111-111111111111", 234, 1)


def test_declared_venv_python_path_is_preserved(tmp_path):
    python = tmp_path / "bin/python"
    python.parent.mkdir()
    python.symlink_to(sys.executable)
    launcher = PythonTdsWorkerLauncher(
        policy=POLICY, identity=IDENTITY, python_executable=python, package_root=tmp_path
    )
    assert launcher.python == python


def test_source_fingerprint_detects_mutation_and_ignores_bytecode(tmp_path):
    source = tmp_path / "dpone/app/mssql_tds_worker_bootstrap.py"
    source.parent.mkdir(parents=True)
    source.write_text("# fixed worker\n")
    digest = worker_installation_digest(tmp_path)
    cache = source.parent / "__pycache__"
    cache.mkdir()
    (cache / "mssql_tds_worker_bootstrap.cpython-312.pyc").write_bytes(b"generated")
    assert worker_installation_digest(tmp_path) == digest
    source.write_text("# changed worker\n")
    assert worker_installation_digest(tmp_path) != digest


@pytest.fixture
def child():
    cr, cw = os.pipe()
    sr, sw = os.pipe()
    rr, rw = os.pipe()
    fds = (cr, cw, sr, sw, rr, rw)
    for fd in fds:
        os.set_blocking(fd, False)
    events = []
    handle = SimpleNamespace(
        identity=PROCESS,
        wait=lambda **kwargs: SimpleNamespace(reaped=True, exit_code=0),
        contain=lambda **kwargs: SimpleNamespace(reaped=True, exit_code=-9),
        close=lambda: events.append("handle_closed"),
    )
    process = SimpleNamespace(stdout=os.fdopen(rr, "rb", buffering=0), returncode=None)
    launcher = SimpleNamespace(
        identity=IDENTITY,
        policy=POLICY,
        package_root=Path("/admitted"),
        assert_installation=lambda: pytest.fail("active parent hashing"),
    )
    worker = PythonTdsWorker(launcher, process, handle, cw, sr)
    yield SimpleNamespace(worker=worker, sw=sw, cr=cr, handle=handle, process=process, events=events)
    process.stdout.close()
    for fd in fds:
        try:
            os.close(fd)
        except OSError:
            pass


def startup(h, identity=PROCESS):
    os.write(
        h.sw,
        encode_message(
            canonical_json_bytes(
                {
                    "schema_version": 1,
                    "process": asdict(identity),
                    "implementation_sha256": H,
                    "package_root": "/admitted",
                }
            ),
            max_payload=16384,
        ),
    )
    os.close(h.sw)
    h.worker.startup(deadline=time.monotonic() + 1)


def test_send_requires_startup_and_is_one_shot(child):
    body = canonical_json_bytes({"identity": asdict(IDENTITY), "policy": POLICY.to_dict()})
    with pytest.raises(ValueError, match="release_invalid"):
        child.worker.send(body, deadline=time.monotonic() + 1)
    startup(child)
    child.worker.send(body, deadline=time.monotonic() + 1)
    assert os.read(child.cr, 10000) == encode_message(body, max_payload=1024**2)
    assert os.read(child.cr, 1) == b""
    with pytest.raises(ValueError, match="release_invalid"):
        child.worker.send(body, deadline=time.monotonic() + 1)


def test_mismatched_request_cannot_be_retried_or_transmitted(child):
    startup(child)
    with pytest.raises(ValueError, match="binding_invalid"):
        child.worker.send(b"{}", deadline=time.monotonic() + 1)
    with pytest.raises(BlockingIOError):
        os.read(child.cr, 1)
    with pytest.raises(ValueError, match="release_invalid"):
        child.worker.send(b"{}", deadline=time.monotonic() + 1)


def test_cannot_close_unsettled_process(child):
    with pytest.raises(WindowOutcomeUnknown, match="close_unsettled"):
        child.worker.close()
    assert not child.events
    proof = child.worker.wait(deadline=time.monotonic() + 1)
    assert proof.reaped and proof.exit_code == 0
    child.worker.close()
    assert child.process.returncode == 0 and child.events == ["handle_closed"]


def test_cross_thread_close_cannot_reuse_descriptors(child):
    errors = []

    def close():
        try:
            child.worker.close()
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=close)
    thread.start()
    thread.join()
    assert len(errors) == 1 and "owner_invalid" in str(errors[0])
    assert not child.events


def test_settled_exit_is_reused_without_reaping_again(child):
    proof = child.worker.wait(deadline=time.monotonic() + 1)
    child.handle.contain = lambda **kwargs: pytest.fail("already reaped child must not be signalled")
    assert child.worker.terminate(deadline=time.monotonic() + 1) == proof


def test_second_pipe_failure_closes_first_pair(tmp_path, monkeypatch):
    from dpone.adapters import mssql_tds_supervisor_process as module

    launcher = PythonTdsWorkerLauncher(
        policy=POLICY, identity=IDENTITY, python_executable=Path(sys.executable), package_root=tmp_path
    )
    monkeypatch.setattr(module.LinuxTdsProcess, "admit", lambda: None)
    monkeypatch.setattr(launcher, "assert_installation", lambda: None)
    real_pipe = os.pipe
    opened = []

    def pipe():
        if opened:
            raise OSError("descriptor allocation failed")
        pair = real_pipe()
        opened.extend(pair)
        return pair

    monkeypatch.setattr(module.os, "pipe", pipe)
    try:
        with pytest.raises(OSError, match="allocation"):
            launcher.spawn(startup_deadline=time.monotonic() + 1, operation_deadline=time.monotonic() + 2)
        for fd in opened:
            with pytest.raises(OSError):
                os.fstat(fd)
    finally:
        for fd in opened:
            try:
                os.close(fd)
            except OSError:
                pass


def test_failed_startup_is_not_retried(child):
    os.write(child.sw, encode_message(b"{}", max_payload=16384))
    os.close(child.sw)
    with pytest.raises(ValueError, match="startup_invalid"):
        child.worker.startup(deadline=time.monotonic() + 1)
    with pytest.raises(ValueError, match="startup_already_observed"):
        child.worker.startup(deadline=time.monotonic() + 1)
    with pytest.raises(ValueError, match="release_invalid"):
        child.worker.send(b"{}", deadline=time.monotonic() + 1)


def test_fingerprint_rejects_nonregular_source_without_open(tmp_path, monkeypatch):
    source = tmp_path / "dpone/app/mssql_tds_worker_bootstrap.py"
    source.parent.mkdir(parents=True)
    source.write_text("# fixed worker\n")
    fifo = source.parent / "unexpected.py"
    os.mkfifo(fifo)
    real_open = Path.open

    def checked_open(path, *args, **kwargs):
        assert path != fifo, "fingerprint must reject FIFO before opening it"
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", checked_open)
    with pytest.raises(ValueError, match="installation_invalid"):
        worker_installation_digest(tmp_path)


def test_cleanup_attempts_all_resources_after_handle_close_error(child):
    child.worker.wait(deadline=time.monotonic() + 1)

    def failed_close():
        raise OSError("pidfd close failed")

    child.handle.close = failed_close
    control, startup_fd = child.worker._control, child.worker._startup
    with pytest.raises(OSError, match="pidfd close"):
        child.worker.close()
    assert child.process.stdout.closed
    for fd in (control, startup_fd):
        with pytest.raises(OSError):
            os.fstat(fd)
    assert child.worker._control == child.worker._startup == -1


def test_send_detaches_control_before_close_error(child, monkeypatch):
    startup(child)
    fd = child.worker._control
    real_close = os.close

    def close(value):
        real_close(value)
        if value == fd:
            raise OSError("close acknowledgement lost")

    monkeypatch.setattr(os, "close", close)
    body = canonical_json_bytes({"identity": asdict(IDENTITY), "policy": POLICY.to_dict()})
    with pytest.raises(OSError, match="acknowledgement"):
        child.worker.send(body, deadline=time.monotonic() + 1)
    assert child.worker._control == -1


def test_unknown_launch_without_pidfd_retains_resources(child):
    from dpone.adapters.mssql_tds_supervisor_process import UnresolvedPythonTdsLaunch
    from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

    launch = UnresolvedPythonTdsLaunch(child.process, (), None)
    with pytest.raises(TdsLaunchUnknown) as failed:
        launch.contain(deadline=time.monotonic() + 1)
    assert failed.value.launch is launch
    with pytest.raises(TdsLaunchUnknown):
        launch.close()
    assert not child.process.stdout.closed


def test_unknown_launch_with_pidfd_can_settle_before_close(child):
    from dpone.adapters.mssql_tds_supervisor_process import UnresolvedPythonTdsLaunch

    launch = UnresolvedPythonTdsLaunch(child.process, (), child.handle)
    launch.contain(deadline=time.monotonic() + 1)
    launch.close()
    assert child.process.returncode == -9
    assert child.process.stdout.closed
    assert child.events == ["handle_closed"]


def test_post_spawn_pipe_close_error_retains_launch_and_closes_other_child_end(tmp_path, monkeypatch):
    from dpone.adapters import mssql_tds_supervisor_process as module
    from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

    launcher = PythonTdsWorkerLauncher(
        policy=POLICY, identity=IDENTITY, python_executable=Path(sys.executable), package_root=tmp_path
    )
    real_pipe, real_close = os.pipe, os.close
    created = []
    rr, rw = real_pipe()
    process = SimpleNamespace(pid=PROCESS.pid, stdout=os.fdopen(rr, "rb", buffering=0), returncode=None)
    handle = SimpleNamespace(identity=PROCESS)

    def pipe():
        pair = real_pipe()
        created.extend(pair)
        return pair

    def close(fd):
        real_close(fd)
        if fd == created[0]:
            raise OSError("child channel close failed")

    monkeypatch.setattr(module.os, "pipe", pipe)
    monkeypatch.setattr(module.os, "close", close)
    monkeypatch.setattr(module.LinuxTdsProcess, "admit", lambda: None)
    monkeypatch.setattr(module.LinuxTdsProcess, "identify", lambda pid: PROCESS)
    monkeypatch.setattr(module.LinuxTdsProcess, "acquire", lambda identity: handle)
    monkeypatch.setattr(launcher, "assert_installation", lambda: None)
    monkeypatch.setattr(module.subprocess, "Popen", lambda *args, **kwargs: process)
    try:
        with pytest.raises(TdsLaunchUnknown) as caught:
            launcher.spawn(startup_deadline=time.monotonic() + 1, operation_deadline=time.monotonic() + 2)
        launch = caught.value.launch
        assert launch.process is process and launch.handle is handle
        assert launch.descriptors == (created[1], created[2])
        for fd in (created[0], created[3]):
            with pytest.raises(OSError):
                os.fstat(fd)
        for fd in launch.descriptors:
            os.fstat(fd)
    finally:
        process.stdout.close()
        real_close(rw)
        for fd in created:
            try:
                real_close(fd)
            except OSError:
                pass


@pytest.mark.parametrize("suffix", [".pyc", ".so", ".pyd"])
def test_fingerprint_rejects_alternative_framework_loaders(tmp_path, suffix):
    source = tmp_path / "dpone/app/mssql_tds_worker_bootstrap.py"
    source.parent.mkdir(parents=True)
    source.write_text("# fixed worker")
    source.with_name("alternative" + suffix).write_bytes(b"unbound implementation")
    with pytest.raises(ValueError, match="installation_invalid"):
        worker_installation_digest(tmp_path)


def test_result_validation_cannot_succeed_after_deadline(child, monkeypatch):
    from dpone.adapters import mssql_tds_supervisor_process as module
    from dpone.contracts.mssql_native_chunks import TdsInputReceipt
    from dpone.contracts.mssql_tds_result import TdsWorkerResult, attempt_identity_digest

    startup(child)
    body = canonical_json_bytes({"identity": asdict(IDENTITY), "policy": POLICY.to_dict()})
    child.worker.send(body, deadline=time.monotonic() + 1)
    deadline = time.monotonic() + 1
    now = [deadline - 0.1]
    receipt = TdsInputReceipt(0, 0, H)
    binding = attempt_identity_digest(IDENTITY)

    def delayed_validation(body, **kwargs):
        now[0] = deadline + 0.1
        return TdsWorkerResult(binding, receipt, None)

    monkeypatch.setattr(module, "read_worker_message", lambda *args, **kwargs: b"bounded-result")
    monkeypatch.setattr(module, "decode_result_payload", delayed_validation)
    monkeypatch.setattr(module.time, "monotonic", lambda: now[0])
    with pytest.raises(ValueError, match="tds_result_deadline"):
        child.worker.receive(expected_attempt_sha256=binding, expected_input=receipt, deadline=deadline)
