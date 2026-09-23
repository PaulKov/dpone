"""Five-pipe transport tests; synthetic handles do not certify Linux or SQL."""

import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_tds_coordinator_process import PythonTdsCoordinatorProcess
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, encode_startup
from dpone.contracts.mssql_tds_frames import encode_message
from dpone.contracts.mssql_tds_worker import TdsProcessIdentity

H = "a" * 64
PROCESS = TdsProcessIdentity(H, "11111111-1111-1111-1111-111111111111", 234, 1)
NONCE = b"n" * 32


@pytest.fixture
def child():
    pairs = [os.pipe() for _ in range(5)]
    for pair in pairs:
        for fd in pair:
            os.set_blocking(fd, False)
    parent = (pairs[0][0], pairs[1][1], pairs[2][0], pairs[3][1], pairs[4][0])
    handle = SimpleNamespace(
        identity=PROCESS,
        wait=lambda **kw: SimpleNamespace(reaped=True, exit_code=0),
        contain=lambda **kw: SimpleNamespace(reaped=True, exit_code=-9),
        close=lambda: None,
    )
    process = SimpleNamespace(stdout=None, returncode=None)
    deadline = time.monotonic() + 5
    worker = PythonTdsCoordinatorProcess(
        process,
        handle,
        parent,
        package_root=Path("/admitted"),
        implementation_sha256=H,
        launch_nonce=NONCE,
        startup_deadline=deadline,
        operation_deadline=deadline,
    )
    yield SimpleNamespace(worker=worker, pairs=pairs, handle=handle, deadline=deadline)
    for pair in pairs:
        for fd in pair:
            try:
                os.close(fd)
            except OSError:
                pass


def emit(child, phase, body):
    fd = child.pairs[phase][1]
    os.write(fd, encode_message(body, max_payload=262144))
    os.close(fd)


def start(child):
    receipt = TdsCoordinatorStartup(PROCESS, H, "/admitted", NONCE)
    emit(child, 0, encode_startup(receipt))
    assert child.worker.startup(deadline=child.deadline) == receipt
    assert child.worker.startup_receipt == receipt


def test_full_exchange_and_real_eof(child):
    start(child)
    child.worker.deliver_credentials(b"private", deadline=child.deadline)
    assert os.read(child.pairs[1][0], 100) == encode_message(b"private", max_payload=1024**2)
    assert os.read(child.pairs[1][0], 1) == b""
    emit(child, 2, b"authority")
    assert child.worker.observe_authority(deadline=child.deadline) == b"authority"
    child.worker.deliver_grant(b"grant", deadline=child.deadline)
    assert os.read(child.pairs[3][0], 100) == encode_message(b"grant", max_payload=16384)
    assert os.read(child.pairs[3][0], 1) == b""
    emit(child, 4, b"result")
    assert child.worker.receive_result(deadline=child.deadline) == b"result"
    child.worker.wait(deadline=child.deadline)
    child.worker.close()


def test_wrong_order_does_not_release_credentials(child):
    with pytest.raises(ValueError, match="phase"):
        child.worker.deliver_credentials(b"private", deadline=child.deadline)
    with pytest.raises(BlockingIOError):
        os.read(child.pairs[1][0], 1)


def test_failed_startup_poisoned(child):
    emit(child, 0, b"{}")
    with pytest.raises(ValueError, match="startup_invalid"):
        child.worker.startup(deadline=child.deadline)
    with pytest.raises(ValueError, match="phase"):
        child.worker.startup(deadline=child.deadline)
    with pytest.raises(ValueError, match="phase"):
        child.worker.deliver_credentials(b"private", deadline=child.deadline)


@pytest.mark.parametrize(
    "field,value",
    [
        ("package_root", "/other"),
        ("implementation_sha256", "b" * 64),
        ("launch_nonce", b"x" * 32),
        ("process", TdsProcessIdentity(H, PROCESS.boot_id, 235, 1)),
    ],
)
def test_all_startup_bindings_checked(child, field, value):
    from dataclasses import replace

    receipt = replace(TdsCoordinatorStartup(PROCESS, H, "/admitted", NONCE), **{field: value})
    emit(child, 0, encode_startup(receipt))
    with pytest.raises(ValueError, match="startup_binding"):
        child.worker.startup(deadline=child.deadline)
    assert child.worker.startup_receipt is None


def test_complete_frame_without_eof_times_out_and_cannot_retry(child):
    os.write(
        child.pairs[0][1],
        encode_message(encode_startup(TdsCoordinatorStartup(PROCESS, H, "/admitted", NONCE)), max_payload=16384),
    )
    with pytest.raises(ValueError, match="deadline"):
        child.worker.startup(deadline=time.monotonic() + 0.02)
    with pytest.raises(ValueError, match="phase"):
        child.worker.startup(deadline=child.deadline)


def test_second_frame_rejected(child):
    body = encode_message(encode_startup(TdsCoordinatorStartup(PROCESS, H, "/admitted", NONCE)), max_payload=16384)
    os.write(child.pairs[0][1], body + body)
    os.close(child.pairs[0][1])
    with pytest.raises(ValueError):
        child.worker.startup(deadline=child.deadline)
    assert child.worker.startup_receipt is None


def test_partial_send_poisoned(child, monkeypatch):
    from dpone.adapters import mssql_tds_coordinator_process as module

    start(child)

    def partial(fd, payload, **kw):
        os.write(fd, payload[:2])
        raise ValueError("partial_delivery")

    monkeypatch.setattr(module, "write_worker_control", partial)
    with pytest.raises(ValueError, match="partial_delivery"):
        child.worker.deliver_credentials(b"private", deadline=child.deadline)
    assert len(os.read(child.pairs[1][0], 100)) == 2
    with pytest.raises(ValueError, match="phase"):
        child.worker.deliver_credentials(b"private", deadline=child.deadline)
    with pytest.raises(ValueError, match="phase"):
        child.worker.observe_authority(deadline=child.deadline)


@pytest.mark.parametrize("limit,method,phase", [(1024**2, "deliver_credentials", 1), (16384, "deliver_grant", 3)])
def test_send_caps_consume_attempt(child, limit, method, phase):
    child.worker._phase = phase
    with pytest.raises(ValueError):
        getattr(child.worker, method)(b"x" * (limit + 1), deadline=child.deadline)
    with pytest.raises(ValueError, match="phase"):
        getattr(child.worker, method)(b"x", deadline=child.deadline)
    with pytest.raises(BlockingIOError):
        os.read(child.pairs[phase][0], 1)


def test_retains_result_on_close_ack_loss(child, monkeypatch):
    child.worker._phase = 4
    emit(child, 4, b"observed")
    real_close = os.close

    def close(fd):
        real_close(fd)
        if fd == child.pairs[4][0]:
            raise OSError("ack lost")

    monkeypatch.setattr(os, "close", close)
    with pytest.raises(OSError, match="ack lost"):
        child.worker.receive_result(deadline=child.deadline)
    assert child.worker.received_result == b"observed"
    assert child.pairs[4][0] not in child.worker._resources.descriptors
    with pytest.raises(ValueError, match="phase"):
        child.worker.receive_result(deadline=child.deadline)


def test_owner_and_expired_deadline_keep_containment_available(child):
    import threading

    errors = []

    def foreign():
        try:
            child.worker.startup(deadline=child.deadline)
        except ValueError as error:
            errors.append(str(error))

    thread = threading.Thread(target=foreign)
    thread.start()
    thread.join()
    assert errors == ["mssql_native.tds_worker_owner_invalid"]
    child.worker._operation_deadline = time.monotonic() - 1
    with pytest.raises(ValueError, match="deadline"):
        child.worker.wait(deadline=child.deadline)
    assert child.worker.terminate(deadline=child.deadline).reaped
    child.worker.close()


@pytest.fixture
def launcher(tmp_path, monkeypatch):
    import sys

    from dpone.adapters import mssql_tds_coordinator_process as module

    app = tmp_path / "dpone/app"
    app.mkdir(parents=True)
    (app / "mssql_tds_worker_bootstrap.py").write_text("# source inventory prerequisite\n")
    (app / "mssql_tds_coordinator_bootstrap.py").write_text("# synthetic fixture\n")
    from dpone.adapters.mssql_tds_coordinator_connection import (
        TdsBinaryPin,
        TdsConnectionProfile,
        TdsCoordinatorBuild,
        encode_connection_admission,
    )

    pin = TdsBinaryPin(Path(sys.executable), H)
    admission = encode_connection_admission(
        TdsCoordinatorBuild(pin, pin, pin, pin), TdsConnectionProfile.SYNTHETIC_LOCAL
    )
    instance = module.PythonTdsCoordinatorLauncher(
        python_executable=Path(sys.executable),
        admission=admission,
        package_root=tmp_path,
        implementation_sha256=module.worker_installation_digest(tmp_path),
        max_address_space_bytes=128 * 1024**2,
    )
    monkeypatch.setattr(module.LinuxTdsProcess, "admit", lambda: None)
    return instance


@pytest.mark.parametrize("fail_at", [1, 2, 3, 4, 5])
def test_partial_pipe_allocation_closes_every_descriptor(launcher, monkeypatch, fail_at):
    real_pipe = os.pipe
    opened = []

    def pipe():
        if len(opened) // 2 + 1 == fail_at:
            raise OSError("allocation")
        pair = real_pipe()
        opened.extend(pair)
        return pair

    monkeypatch.setattr(os, "pipe", pipe)
    with pytest.raises(OSError, match="allocation"):
        launcher.spawn(startup_deadline=time.monotonic() + 2, operation_deadline=time.monotonic() + 3)
    for fd in opened:
        with pytest.raises(OSError):
            os.fstat(fd)


def test_source_mutation_or_missing_bootstrap_fails_before_spawn(launcher):
    source = launcher.package_root / "dpone/app/mssql_tds_coordinator_bootstrap.py"
    source.write_text("# changed\n")
    with pytest.raises(ValueError, match="implementation_changed"):
        launcher.spawn(startup_deadline=time.monotonic() + 2, operation_deadline=time.monotonic() + 3)
    source.unlink()
    with pytest.raises(ValueError, match="bootstrap_missing"):
        launcher.assert_installation()


def test_post_spawn_identity_failure_retains_exact_resources(launcher, monkeypatch):
    from dpone.adapters import mssql_tds_coordinator_process as module
    from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

    created = []
    real_pipe = os.pipe
    process = SimpleNamespace(pid=234, stdout=None, returncode=None)

    def pipe():
        pair = real_pipe()
        created.extend(pair)
        return pair

    def fail(pid):
        raise OSError("identity unavailable")

    monkeypatch.setattr(os, "pipe", pipe)
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **kw: process)
    monkeypatch.setattr(module.LinuxTdsProcess, "identify", fail)
    with pytest.raises(TdsLaunchUnknown) as caught:
        launcher.spawn(startup_deadline=time.monotonic() + 2, operation_deadline=time.monotonic() + 3)
    retained = caught.value.launch
    assert retained.process is process and retained.handle is None
    assert len(retained.descriptors) == 5
    for fd in created:
        if fd in retained.descriptors:
            os.fstat(fd)
        else:
            with pytest.raises(OSError):
                os.fstat(fd)
    with pytest.raises(TdsLaunchUnknown):
        retained.contain(deadline=time.monotonic() + 1)
    # Test owns the fake process and can release its synthetic retained resources.
    for fd in retained.descriptors:
        os.close(fd)
    retained._resources._cache.cleanup()


def test_actual_fixed_popen_five_pipe_exchange_with_synthetic_guard_and_identity(launcher, monkeypatch):
    """Real Popen/EOF only: no Linux guard, pidfd, optional driver or SQL claim."""
    from dpone.adapters import mssql_tds_coordinator_process as module

    adapters = launcher.package_root / "dpone/adapters"
    adapters.mkdir()
    (adapters / "mssql_tds_worker_guard.py").write_text("def install_worker_guard(**kw): pass\n")
    script = """import json, os, select, struct, sys
args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
def send(name, body):
    fd = int(args[name]); os.write(fd, struct.pack('>I', len(body)) + body); os.close(fd)
def receive(name):
    fd = int(args[name]); data = b''
    while True:
        select.select([fd], [], [], 3)
        piece = os.read(fd, 4096)
        if not piece: break
        data += piece
    os.close(fd)
    assert int.from_bytes(data[:4], 'big') == len(data) - 4
    return data[4:]
receipt = dict(schema='dpone.tds.coordinator-startup.v1',
    process=dict(host_sha256='a'*64, boot_id='11111111-1111-1111-1111-111111111111', pid=os.getpid(), start_ticks=1),
    package_root=os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    implementation_sha256=args['--implementation-sha256'], launch_nonce=args['--launch-nonce'])
send('--startup-fd', json.dumps(receipt).encode())
assert receive('--credentials-fd') == b'private'
send('--authority-fd', b'authority')
assert receive('--grant-fd') == b'grant'
send('--result-fd', b'result')
"""
    (launcher.package_root / "dpone/app/mssql_tds_coordinator_bootstrap.py").write_text(script)
    launcher.implementation_sha256 = module.worker_installation_digest(launcher.package_root)
    actual_popen = module.subprocess.Popen
    spawned = []

    def popen(command, **kw):
        assert command[1:4] == ["-I", "-S", "-B"]
        assert module.COORDINATOR_SOURCE_SHIM in command
        assert command[command.index("--admission") + 1].encode() == launcher.admission
        assert kw["env"] == {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}
        assert len(kw["pass_fds"]) == 5 and len(set(kw["pass_fds"])) == 5
        assert kw["stdout"] == kw["stderr"] == module.subprocess.DEVNULL
        child_process = actual_popen(command, **kw)
        spawned.append(child_process)
        return child_process

    def acquire(identity):
        def settle(**kw):
            code = spawned[0].wait(timeout=max(0.01, kw["deadline"] - time.monotonic()))
            return SimpleNamespace(reaped=True, exit_code=code)

        return SimpleNamespace(identity=identity, wait=settle, close=lambda: None)

    monkeypatch.setattr(module.subprocess, "Popen", popen)
    monkeypatch.setattr(module.LinuxTdsProcess, "identify", lambda pid: TdsProcessIdentity(H, PROCESS.boot_id, pid, 1))
    monkeypatch.setattr(module.LinuxTdsProcess, "acquire", acquire)
    deadline = time.monotonic() + 5
    worker = launcher.spawn(startup_deadline=deadline, operation_deadline=deadline)
    try:
        worker.startup(deadline=deadline)
        worker.deliver_credentials(b"private", deadline=deadline)
        assert worker.observe_authority(deadline=deadline) == b"authority"
        worker.deliver_grant(b"grant", deadline=deadline)
        assert worker.receive_result(deadline=deadline) == b"result"
        assert worker.wait(deadline=deadline).exit_code == 0
        worker.close()
    finally:
        if spawned[0].poll() is None:
            spawned[0].kill()
            spawned[0].wait()


def test_phase_deadlines_never_extend_inherited_budget(child, monkeypatch):
    from dpone.adapters import mssql_tds_coordinator_process as module

    observed = []

    def read(fd, *, deadline, max_payload):
        observed.append((deadline, max_payload))
        return encode_startup(TdsCoordinatorStartup(PROCESS, H, "/admitted", NONCE))

    monkeypatch.setattr(module, "read_worker_message", read)
    child.worker.startup(deadline=child.deadline + 100)
    assert observed == [(child.deadline, 16384)]
    child.worker._phase = 4
    child.worker.receive_result(deadline=child.deadline + 100)
    assert observed[-1] == (child.deadline, 262144)


def test_popen_failure_closes_every_pipe_and_cache(launcher, monkeypatch):
    from dpone.adapters import mssql_tds_coordinator_process as module

    created, caches = [], []
    real_pipe, real_cache = os.pipe, module.TemporaryDirectory

    def pipe():
        pair = real_pipe()
        created.extend(pair)
        return pair

    def cache(**kw):
        value = real_cache(**kw)
        caches.append(value.name)
        return value

    def popen(*a, **kw):
        raise OSError("spawn failed")

    monkeypatch.setattr(os, "pipe", pipe)
    monkeypatch.setattr(module, "TemporaryDirectory", cache)
    monkeypatch.setattr(module.subprocess, "Popen", popen)
    with pytest.raises(OSError, match="spawn failed"):
        launcher.spawn(startup_deadline=time.monotonic() + 2, operation_deadline=time.monotonic() + 3)
    assert len(created) == 10 and len(caches) == 1 and not Path(caches[0]).exists()
    for fd in created:
        with pytest.raises(OSError):
            os.fstat(fd)


def test_child_end_close_ack_loss_retains_parent_fds_without_reclosing(launcher, monkeypatch):
    from dpone.adapters import mssql_tds_coordinator_process as module
    from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

    created, closed = [], []
    real_pipe, real_close = os.pipe, os.close

    def pipe():
        pair = real_pipe()
        created.extend(pair)
        return pair

    def close(fd):
        closed.append(fd)
        real_close(fd)
        if fd == created[1]:
            raise OSError("ack lost")

    monkeypatch.setattr(os, "pipe", pipe)
    monkeypatch.setattr(os, "close", close)
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **kw: SimpleNamespace(pid=234, stdout=None))
    with pytest.raises(TdsLaunchUnknown) as caught:
        launcher.spawn(startup_deadline=time.monotonic() + 2, operation_deadline=time.monotonic() + 3)
    launch = caught.value.launch
    assert len(closed) == len(set(closed)) == 5
    assert set(closed).isdisjoint(launch.descriptors)
    for fd in launch.descriptors:
        os.fstat(fd)
        real_close(fd)
    launch._resources._cache.cleanup()


def test_admission_descriptor_is_required_closed_and_bound(launcher):
    from hashlib import sha256

    from dpone.adapters.mssql_tds_coordinator_process import PythonTdsCoordinatorLauncher

    assert launcher.admission_sha256 == sha256(launcher.admission).hexdigest()
    with pytest.raises(ValueError, match="descriptor_invalid"):
        PythonTdsCoordinatorLauncher(
            python_executable=launcher.python,
            package_root=launcher.package_root,
            implementation_sha256=H,
            admission=b'{"connection_string":"forbidden"}',
            max_address_space_bytes=128 * 1024**2,
        )
