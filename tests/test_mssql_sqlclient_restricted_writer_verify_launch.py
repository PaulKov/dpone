import socket
import struct
import sys
import threading
from dataclasses import asdict, replace
from hashlib import sha256
from pathlib import Path
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.adapters import mssql_sqlclient_restricted_writer_verify_launch as launch_module
from dpone.adapters.mssql_sqlclient_restricted_writer_verify_launch import (
    LIMIT,
    RestrictedWriterVerifyProcess,
    RestrictedWriterVerifyProcessUnknown,
    _read,
    _write,
)
from dpone.adapters.mssql_tds_coordinator_connection import (
    TdsBinaryPin,
    TdsCoordinatorBuild,
    encode_connection_admission,
)
from dpone.app.mssql_sqlclient_restricted_writer_verify_request import RestrictedWriterVerifyWireContract
from dpone.contracts.mssql_sqlclient_restricted_writer_verify import verify_request_digest
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_codec import encode_verify_opening, encode_verify_request
from dpone.contracts.mssql_sqlclient_restricted_writer_verify_handshake import RestrictedWriterVerifyOpening
from dpone.contracts.mssql_tds_connection import TdsConnectionProfile
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown
from tests.test_mssql_sqlclient_restricted_writer_verify import verify_result
from tests.test_mssql_sqlclient_restricted_writer_verify_request import launch_request, registration
from tests.test_mssql_tds_coordinator import PROCESS


class Executor:
    def __init__(self, custody):
        self.identity = PROCESS
        self.done = threading.Event()
        self.done.set()
        self.exit = SimpleNamespace(identity=PROCESS, reaped=True, exit_code=-9)
        self.failed = False
        self.cleanup_deadline = monotonic() + 2
        self.cleanup_count = 0
        self._custody = custody

    def request(self):
        self.cleanup_count += 1
        self._custody._closed = True
        return self.cleanup_deadline


def process_pair():
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    custody = SimpleNamespace(process=SimpleNamespace(pid=PROCESS.pid), _closed=False)
    executor = Executor(custody)
    process = RestrictedWriterVerifyProcess(
        custody,
        executor,
        right,
        registration().reservation,
        monotonic() + 2,
        monotonic() + 4,
        RestrictedWriterVerifyWireContract(),
        custody_waiter=lambda custody, actual, identity, deadline: launch_module.await_permission_custody(
            custody, actual, identity, deadline
        ),
    )
    return process, executor, left, right


def test_frame_round_trip_is_exact_and_bounded():
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    payload = b"bounded-result"
    thread = threading.Thread(target=lambda: _write(left, payload, monotonic() + 2))
    thread.start()
    assert _read(right, monotonic() + 2) == payload
    thread.join()
    left.close()
    right.close()


def test_oversized_frame_is_rejected_before_body_read():
    left, right = socket.socketpair()
    left.sendall((LIMIT + 1).to_bytes(4, "big"))
    right.setblocking(False)
    with pytest.raises(ValueError):
        _read(right, monotonic() + 1)
    left.close()
    right.close()


def test_truncated_frame_and_trailing_bytes_are_observable():
    left, right = socket.socketpair()
    left.sendall((4).to_bytes(4, "big") + b"ab")
    left.close()
    right.setblocking(False)
    with pytest.raises(ValueError):
        _read(right, monotonic() + 1)
    right.close()


def test_registration_binds_original_process_and_reuse_forces_single_cleanup():
    process, executor, peer, owned = process_pair()
    payload = canonical_json_bytes(
        {"schema": "dpone.sqlclient.restricted-writer-verify-startup.v1", "process": asdict(PROCESS)}
    )
    peer.sendall(struct.pack("!I", len(payload)) + payload)
    assert process.registration(deadline=monotonic() + 3).process == PROCESS
    with pytest.raises(RestrictedWriterVerifyProcessUnknown):
        process.registration(deadline=monotonic() + 3)
    assert executor.cleanup_count == 1
    peer.close()
    owned.close()


def test_substituted_startup_and_trailing_eof_are_contained_once():
    process, executor, peer, owned = process_pair()
    payload = canonical_json_bytes(
        {
            "schema": "dpone.sqlclient.restricted-writer-verify-startup.v1",
            "process": asdict(replace(PROCESS, pid=PROCESS.pid + 1)),
        }
    )
    peer.sendall(struct.pack("!I", len(payload)) + payload)
    with pytest.raises(RestrictedWriterVerifyProcessUnknown):
        process.registration(deadline=monotonic() + 3)
    assert executor.cleanup_count == 1
    peer.close()
    owned.close()


def test_await_custody_uses_startup_ceiling(monkeypatch):
    process, executor, peer, owned = process_pair()
    observed = []
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_restricted_writer_verify_launch.await_permission_custody",
        lambda custody, actual, identity, deadline: observed.append((actual, identity, deadline)),
    )
    process.await_custody()
    assert observed[0][0] is executor and observed[0][1] is PROCESS
    assert observed[0][2] == process._startup_deadline
    peer.close()
    owned.close()


def test_probe_authorization_is_emitted_only_for_exact_opening_frame():
    process, _, peer, owned = process_pair()
    startup = canonical_json_bytes(
        {"schema": "dpone.sqlclient.restricted-writer-verify-startup.v1", "process": asdict(PROCESS)}
    )
    _write(peer, startup, monotonic() + 2)
    process.registration(deadline=monotonic() + 2)
    credentials = bytearray(b"credential")
    process.send_credentials(credentials, deadline=monotonic() + 2)
    assert _read(peer, monotonic() + 2) == b"credential"
    credentials[:] = b"\x00" * len(credentials)
    process.scrub_credentials(credentials)
    request = launch_request().request
    opening = RestrictedWriterVerifyOpening(
        verify_request_digest(encode_verify_request(request)), verify_result(request).opening
    )
    payload = encode_verify_opening(opening)
    _write(peer, payload, monotonic() + 2)
    observed = process.writer_session(deadline=monotonic() + 2)
    with pytest.raises(RestrictedWriterVerifyProcessUnknown):
        process.authorize_probe(replace(observed), deadline=monotonic() + 2)
    peer.close()
    owned.close()


class FakeSocket:
    def __init__(self, *, fail_blocking=False, fail_close=False):
        self.fail_blocking = fail_blocking
        self.fail_close = fail_close
        self.close_count = 0

    def setblocking(self, value):
        if self.fail_blocking:
            raise RuntimeError("setblocking")
        assert value is False

    def fileno(self):
        return 41

    def close(self):
        self.close_count += 1
        if self.fail_close:
            raise RuntimeError("ambiguous socket close")


class FakeCache:
    def __init__(self):
        self.name = "/tmp/p9a-cache"
        self.close_count = 0

    def cleanup(self):
        self.close_count += 1


@pytest.fixture
def concrete_launcher(tmp_path, monkeypatch):
    bootstrap = tmp_path / "dpone/app/mssql_sqlclient_restricted_writer_verify_bootstrap.py"
    bootstrap.parent.mkdir(parents=True)
    bootstrap.write_text("# fixed synthetic source\n")
    pin = TdsBinaryPin(Path(sys.executable), "b" * 64)
    admission = encode_connection_admission(
        TdsCoordinatorBuild(pin, pin, pin, pin), TdsConnectionProfile.SYNTHETIC_LOCAL
    )
    launch = replace(
        launch_request(),
        startup_deadline=100.0,
        operation_deadline=200.0,
        admission_sha256=sha256(admission).hexdigest(),
    )
    launcher = launch_module.PythonRestrictedWriterVerifyLauncher(
        python_executable=Path(sys.executable),
        package_root=tmp_path,
        contract=RestrictedWriterVerifyWireContract(),
        implementation_sha256=launch.request.implementation_sha256,
        admission=admission,
        max_address_space_bytes=1 << 30,
    )
    parent, child, cache = FakeSocket(), FakeSocket(), FakeCache()
    process = SimpleNamespace(pid=PROCESS.pid, stdout=None, returncode=None)
    monkeypatch.setattr(launch_module, "worker_installation_digest", lambda root: launch.request.implementation_sha256)
    monkeypatch.setattr(launch_module.LinuxTdsProcess, "admit", lambda: None)
    monkeypatch.setattr(launch_module.LinuxTdsProcess, "identify", lambda pid: PROCESS)
    monkeypatch.setattr(launch_module.socket, "socketpair", lambda *args: (parent, child))
    monkeypatch.setattr(launch_module, "TemporaryDirectory", lambda **kwargs: cache)
    monkeypatch.setattr(launch_module.subprocess, "Popen", lambda *args, **kwargs: process)
    monkeypatch.setattr(launch_module.time, "monotonic", lambda: 1.0)

    class ConcreteExecutor(Executor):
        def __init__(self, custody, identity, deadline, allowance):
            super().__init__(custody)
            self.identity = identity

    monkeypatch.setattr(launch_module, "TdsChildContainmentExecutor", ConcreteExecutor)
    monkeypatch.setattr(launch_module.RestrictedWriterVerifyProcess, "await_custody", lambda self: None)
    return SimpleNamespace(
        launcher=launcher,
        launch=launch,
        reservation=registration(launch).reservation,
        parent=parent,
        child=child,
        cache=cache,
        process=process,
    )


@pytest.mark.parametrize("boundary", ("setblocking-parent", "setblocking-child", "cache", "retain-cache", "popen"))
def test_preprocess_launcher_faults_close_every_allocated_resource(concrete_launcher, monkeypatch, boundary):
    value = concrete_launcher
    if boundary == "setblocking-parent":
        value.parent.fail_blocking = True
    elif boundary == "setblocking-child":
        value.child.fail_blocking = True
    elif boundary == "cache":
        monkeypatch.setattr(launch_module, "TemporaryDirectory", lambda **kwargs: (_ for _ in ()).throw(RuntimeError()))
    elif boundary == "retain-cache":
        monkeypatch.setattr(
            launch_module.TdsChildProcess,
            "retain_cache",
            lambda self, cache: (_ for _ in ()).throw(RuntimeError()),
        )
    else:
        monkeypatch.setattr(
            launch_module.subprocess, "Popen", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError())
        )
    with pytest.raises(RuntimeError):
        value.launcher.launch(value.launch, value.reservation, public_payload=value.launch.public_payload())
    assert value.parent.close_count == 1 and value.child.close_count == 1
    assert value.cache.close_count == (1 if boundary in ("retain-cache", "popen") else 0)


def test_socketpair_failure_precedes_popen(concrete_launcher, monkeypatch):
    value = concrete_launcher
    calls = []
    monkeypatch.setattr(launch_module.socket, "socketpair", lambda *args: (_ for _ in ()).throw(RuntimeError()))
    monkeypatch.setattr(launch_module.subprocess, "Popen", lambda *args, **kwargs: calls.append(True))
    with pytest.raises(RuntimeError):
        value.launcher.launch(value.launch, value.reservation, public_payload=value.launch.public_payload())
    assert calls == []


def test_custody_creation_failure_precedes_socketpair(concrete_launcher, monkeypatch):
    value = concrete_launcher
    calls = []
    monkeypatch.setattr(launch_module.TdsChildProcess, "launch", lambda: (_ for _ in ()).throw(RuntimeError("custody")))
    monkeypatch.setattr(launch_module.socket, "socketpair", lambda *args: calls.append(True))
    with pytest.raises(RuntimeError):
        value.launcher.launch(value.launch, value.reservation, public_payload=value.launch.public_payload())
    assert calls == []


def test_retain_socket_failure_closes_both_unadopted_channels(concrete_launcher, monkeypatch):
    value = concrete_launcher
    calls = 0
    original = launch_module.TdsChildProcess.retain_socket

    def fail_first(custody, channel):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("retain socket")
        return original(custody, channel)

    monkeypatch.setattr(launch_module.TdsChildProcess, "retain_socket", fail_first)
    with pytest.raises(RuntimeError):
        value.launcher.launch(value.launch, value.reservation, public_payload=value.launch.public_payload())
    assert value.parent.close_count == 1 and value.child.close_count == 1


def test_second_socket_adoption_failure_closes_each_channel_once(concrete_launcher, monkeypatch):
    value = concrete_launcher
    calls = 0
    original = launch_module.TdsChildProcess.retain_socket

    def fail_second(custody, channel):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("second socket adoption")
        return original(custody, channel)

    monkeypatch.setattr(launch_module.TdsChildProcess, "retain_socket", fail_second)
    with pytest.raises(RuntimeError):
        value.launcher.launch(value.launch, value.reservation, public_payload=value.launch.public_payload())
    assert value.parent.close_count == 1 and value.child.close_count == 1


def test_ambiguous_prespawn_close_returns_retained_capability_without_retry(concrete_launcher):
    value = concrete_launcher
    value.parent.fail_blocking = True
    value.parent.fail_close = True
    with pytest.raises(TdsLaunchUnknown) as caught:
        value.launcher.launch(value.launch, value.reservation, public_payload=value.launch.public_payload())
    assert caught.value.launch is not None
    assert value.parent.close_count == 1 and value.child.close_count == 1


@pytest.mark.parametrize("boundary", ("process-adoption", "child-close", "identify", "executor", "custody"))
def test_postspawn_launcher_faults_return_exact_retained_capability(concrete_launcher, monkeypatch, boundary):
    value = concrete_launcher
    if boundary == "process-adoption":
        monkeypatch.setattr(
            launch_module.TdsChildProcess,
            "retain_process",
            lambda self, process: (_ for _ in ()).throw(RuntimeError()),
        )
    elif boundary == "child-close":
        monkeypatch.setattr(
            launch_module.TdsChildProcess, "close_socket", lambda self, channel: (_ for _ in ()).throw(RuntimeError())
        )
    elif boundary == "identify":
        monkeypatch.setattr(
            launch_module.LinuxTdsProcess, "identify", lambda pid: (_ for _ in ()).throw(RuntimeError())
        )
    elif boundary == "executor":
        monkeypatch.setattr(
            launch_module, "TdsChildContainmentExecutor", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError())
        )
    else:
        monkeypatch.setattr(
            launch_module.RestrictedWriterVerifyProcess,
            "await_custody",
            lambda self: (_ for _ in ()).throw(RestrictedWriterVerifyProcessUnknown(self)),
        )
    with pytest.raises(TdsLaunchUnknown) as caught:
        value.launcher.launch(value.launch, value.reservation, public_payload=value.launch.public_payload())
    retained = caught.value.launch
    assert retained is not None
    if boundary == "custody":
        assert retained is caught.value.process
        assert callable(retained.contain) and callable(retained.close)
    else:
        assert retained.process is value.process


def test_expiry_after_argument_construction_prevents_popen(concrete_launcher, monkeypatch):
    value = concrete_launcher
    value.launch = replace(value.launch, startup_deadline=2.0, operation_deadline=3.0)
    clock = iter((1.0, 1.0, 2.0))
    monkeypatch.setattr(launch_module.time, "monotonic", lambda: next(clock))
    calls = []
    monkeypatch.setattr(launch_module.subprocess, "Popen", lambda *args, **kwargs: calls.append(True))
    with pytest.raises(TimeoutError):
        value.launcher.launch(value.launch, value.reservation, public_payload=value.launch.public_payload())
    assert calls == [] and value.parent.close_count == 1 and value.child.close_count == 1
