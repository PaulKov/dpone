"""Actual pipe resources with synthetic process handles; no live SQL certification."""

import os
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters import mssql_sqlclient_departure_launch as module
from dpone.adapters.mssql_tds_coordinator_connection import (
    TdsBinaryPin,
    TdsConnectionProfile,
    TdsCoordinatorBuild,
    encode_connection_admission,
)
from dpone.contracts.mssql_tds_worker import TdsProcessIdentity
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown

H = "a" * 64
PARENT = TdsProcessIdentity(H, "11111111-1111-4111-8111-111111111111", os.getpid(), 1)
CHILD = replace(PARENT, pid=123456, start_ticks=2)


@pytest.fixture
def setup(tmp_path, monkeypatch):
    app = tmp_path / "dpone/app"
    app.mkdir(parents=True)
    (app / "mssql_tds_worker_bootstrap.py").write_text("# inventory prerequisite\n")
    (app / "mssql_sqlclient_departure_bootstrap.py").write_text("# synthetic\n")
    pin = TdsBinaryPin(Path(sys.executable), H)
    admission = encode_connection_admission(
        TdsCoordinatorBuild(pin, pin, pin, pin), TdsConnectionProfile.SYNTHETIC_LOCAL
    )
    args = dict(
        python_executable=Path(sys.executable),
        package_root=tmp_path,
        implementation_sha256=module.worker_installation_digest(tmp_path),
        admission=admission,
        max_address_space_bytes=1 << 30,
    )
    launcher = module.PythonSqlClientDepartureLauncher(**args)
    monkeypatch.setattr(module.LinuxTdsProcess, "admit", lambda: None)
    monkeypatch.setattr(module.LinuxTdsProcess, "identify", lambda pid: PARENT if pid == os.getpid() else CHILD)
    handle = SimpleNamespace(
        identity=CHILD, close=lambda: None, contain=lambda **kw: SimpleNamespace(reaped=True, exit_code=-9)
    )
    monkeypatch.setattr(module.LinuxTdsProcess, "acquire", lambda identity: handle)
    monkeypatch.setattr(module.time, "monotonic", lambda: 1.0)
    monkeypatch.setattr(module.secrets, "token_bytes", lambda count: b"n" * count)
    process = SimpleNamespace(pid=CHILD.pid, returncode=None, stdout=None)
    calls = []

    def popen(command, **kwargs):
        calls.append((command, kwargs))
        return process

    monkeypatch.setattr(module.subprocess, "Popen", popen)
    pairs = []
    real_pipe = os.pipe

    def pipe():
        pair = real_pipe()
        pairs.append(pair)
        return pair

    monkeypatch.setattr(module.os, "pipe", pipe)
    value = SimpleNamespace(launcher=launcher, args=args, pairs=pairs, calls=calls, process=process, handle=handle)
    yield value
    for pair in pairs:
        for fd in pair:
            try:
                os.close(fd)
            except OSError:
                pass


def spawn(s):
    return s.launcher.spawn(startup_deadline=10.25, operation_deadline=20.5)


def closed(fd):
    with pytest.raises(OSError):
        os.fstat(fd)


def test_exact_three_pipes_arguments_and_ownership(setup):
    s = setup
    child = spawn(s)
    command, kw = s.calls[0]
    assert command[1:4] == ["-I", "-S", "-B"] and command[7] == module.DEPARTURE_SOURCE_SHIM
    assert command[command.index("--startup-deadline") + 1] == "10.25"
    assert command[command.index("--operation-deadline") + 1] == "20.5"
    assert command[command.index("--launch-nonce") + 1] == (b"n" * 32).hex()
    assert kw["env"] == {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}
    assert all(kw[name] == module.subprocess.DEVNULL for name in ("stdin", "stdout", "stderr"))
    assert kw["close_fds"] is True and len(kw["pass_fds"]) == 3
    assert not any(x in command for x in ("--credentials", "--password", "--grant-fd", "--authority-fd"))
    assert child.declared_startup.process == CHILD
    assert child.declared_startup.package_root == str(s.args["package_root"])
    assert child.declared_startup.implementation_sha256 == s.args["implementation_sha256"]
    parents = (s.pairs[0][0], s.pairs[1][1], s.pairs[2][0])
    for fd in parents:
        assert os.get_blocking(fd) is False
    for fd in kw["pass_fds"]:
        closed(fd)
    child.terminate(deadline=30.0)
    child.close()
    for fd in parents:
        closed(fd)


@pytest.mark.parametrize(
    "start,end",
    [
        (10, 20.0),
        (True, 20.0),
        (10.0, 20),
        (float("nan"), 20.0),
        (0.0, 20.0),
        (21.0, 20.0),
        (1e-12, 20.0),
        (10.0, float("inf")),
    ],
)
def test_invalid_original_deadlines_no_allocation(setup, start, end):
    with pytest.raises(ValueError):
        setup.launcher.spawn(startup_deadline=start, operation_deadline=end)
    assert not setup.pairs and not setup.calls


def test_invalid_implementation_hash_precedes_admission_effects(tmp_path, monkeypatch):
    effects = []
    monkeypatch.setattr(module, "decode_connection_admission", lambda admission: effects.append("decode"))
    monkeypatch.setattr(module, "AdmittedPythonInputs", lambda *args: effects.append("paths"))

    with pytest.raises(ValueError, match="mssql_native.tds_invalid_sha256"):
        module.PythonSqlClientDepartureLauncher(
            python_executable=Path(sys.executable),
            package_root=tmp_path,
            implementation_sha256="invalid",
            admission=b"invalid",
            max_address_space_bytes=1 << 30,
        )

    assert effects == []


@pytest.mark.parametrize(
    "mutation",
    [
        "admission_space",
        "admission_dict",
        "address_bool",
        "address_zero",
        "root_file",
        "dependency_file",
        "dependency_list",
    ],
)
def test_constructor_admission_and_scalar_checks(setup, mutation):
    args = dict(setup.args)
    if mutation == "admission_space":
        args["admission"] += b" "
    elif mutation == "admission_dict":
        args["admission"] = b"{}"
    elif mutation == "address_bool":
        args["max_address_space_bytes"] = True
    elif mutation == "address_zero":
        args["max_address_space_bytes"] = 0
    elif mutation == "root_file":
        args["package_root"] = Path(sys.executable)
    elif mutation == "dependency_file":
        args["dependency_paths"] = (Path(sys.executable),)
    else:
        args["dependency_paths"] = []
    with pytest.raises(ValueError):
        module.PythonSqlClientDepartureLauncher(**args)


@pytest.mark.parametrize("fail_at", [1, 2, 3])
def test_pipe_failure_closes_previous_pairs(setup, monkeypatch, fail_at):
    original = module.os.pipe
    calls = 0

    def pipe():
        nonlocal calls
        calls += 1
        if calls == fail_at:
            raise OSError("synthetic")
        return original()

    monkeypatch.setattr(module.os, "pipe", pipe)
    with pytest.raises(OSError):
        spawn(setup)
    assert not setup.calls
    for pair in setup.pairs:
        for fd in pair:
            closed(fd)


@pytest.mark.parametrize("fail_at", range(1, 7))
def test_nonblocking_failure_tracks_both_fds_before_operation(setup, monkeypatch, fail_at):
    original = os.set_blocking
    calls = 0

    def change(fd, blocking):
        nonlocal calls
        calls += 1
        if calls == fail_at:
            raise OSError("synthetic")
        original(fd, blocking)

    monkeypatch.setattr(module.os, "set_blocking", change)
    with pytest.raises(OSError):
        spawn(setup)
    assert not setup.calls
    for pair in setup.pairs:
        for fd in pair:
            closed(fd)


@pytest.mark.parametrize("phase", ["cache", "popen", "pre_popen_deadline"])
def test_pre_spawn_failure_closes_every_fd_and_cache(setup, monkeypatch, phase):
    caches = []
    original = module.TemporaryDirectory

    def cache(*args, **kw):
        if phase == "cache":
            raise OSError("synthetic")
        value = original(*args, **kw)
        caches.append(value.name)
        if phase == "pre_popen_deadline":
            monkeypatch.setattr(module.time, "monotonic", lambda: 11.0)
        return value

    monkeypatch.setattr(module, "TemporaryDirectory", cache)
    if phase == "popen":
        monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **k: (_ for _ in ()).throw(OSError("synthetic")))
    with pytest.raises((ValueError, OSError)):
        spawn(setup)
    for pair in setup.pairs:
        for fd in pair:
            closed(fd)
    assert all(not Path(path).exists() for path in caches)


@pytest.mark.parametrize(
    "phase", ["child_close", "identify", "acquire", "deadline", "transport", "pid", "host", "boot"]
)
def test_every_post_spawn_failure_retains_actual_ownership(setup, monkeypatch, phase):
    s = setup
    if phase == "child_close":
        original = module._close_fds

        def close(fds):
            original(fds)
            if fds:
                raise OSError("synthetic close ACK loss")

        monkeypatch.setattr(module, "_close_fds", close)
    elif phase == "identify":
        monkeypatch.setattr(
            module.LinuxTdsProcess,
            "identify",
            lambda pid: PARENT if pid == os.getpid() else (_ for _ in ()).throw(OSError("synthetic")),
        )
    elif phase == "acquire":
        monkeypatch.setattr(
            module.LinuxTdsProcess, "acquire", lambda identity: (_ for _ in ()).throw(OSError("synthetic"))
        )
    elif phase == "deadline":

        def acquire(identity):
            monkeypatch.setattr(module.time, "monotonic", lambda: 11.0)
            return s.handle

        monkeypatch.setattr(module.LinuxTdsProcess, "acquire", acquire)
    elif phase == "transport":
        monkeypatch.setattr(
            module, "SqlClientDepartureProcess", lambda *a, **k: (_ for _ in ()).throw(ValueError("synthetic"))
        )
    else:
        s.handle.identity = replace(
            CHILD,
            **{
                "pid": {"pid": 123457},
                "host": {"host_sha256": "b" * 64},
                "boot": {"boot_id": "22222222-2222-4222-8222-222222222222"},
            }[phase],
        )
    with pytest.raises(TdsLaunchUnknown) as caught:
        spawn(s)
    held = caught.value.launch
    assert held.process is s.process and len(held.descriptors) == 3
    assert held.handle is (None if phase in ("child_close", "identify", "acquire") else s.handle)
    for fd in held.descriptors:
        assert os.fstat(fd)
    cache = held._resources._cache
    assert cache is not None and Path(cache.name).is_dir()
    # Test cleanup is synthetic, not a claim of actual Linux child reaping.
    held._resources.handle = s.handle
    monkeypatch.setattr(module.time, "monotonic", lambda: 1.0)
    held.contain(deadline=30.0)
    if phase == "child_close":
        from dpone.contracts.bounded_window import WindowOutcomeUnknown

        with pytest.raises(WindowOutcomeUnknown, match="tds_close_unknown"):
            held.close()
        resources = held._resources._resources
        closed_before = tuple(resource.state for resource in resources)
        with pytest.raises(WindowOutcomeUnknown, match="tds_close_unknown"):
            held.close()
        assert tuple(resource.state for resource in resources) == closed_before
        assert all(resource.state == "CLOSED" for resource in resources if resource.kind == "cache")
    else:
        held.close()
    assert not Path(cache.name).exists()


@pytest.mark.parametrize(
    "phase", ["initial", "identity", "source_missing", "source_changed", "source_after_allocation"]
)
def test_admission_source_and_deadline_before_effects(setup, monkeypatch, phase):
    s = setup
    bootstrap = s.args["package_root"] / "dpone/app/mssql_sqlclient_departure_bootstrap.py"
    if phase == "initial":
        monkeypatch.setattr(module.time, "monotonic", lambda: 11.0)
    elif phase == "identity":

        def identify(pid):
            monkeypatch.setattr(module.time, "monotonic", lambda: 11.0)
            return PARENT

        monkeypatch.setattr(module.LinuxTdsProcess, "identify", identify)
    elif phase == "source_missing":
        bootstrap.unlink()
    elif phase == "source_changed":
        bootstrap.write_text("# changed")
    else:
        original = module.TemporaryDirectory

        def cache(*args, **kw):
            value = original(*args, **kw)
            bootstrap.write_text("# changed")
            return value

        monkeypatch.setattr(module, "TemporaryDirectory", cache)
    with pytest.raises(ValueError):
        spawn(s)
    assert not s.calls
    if phase != "source_after_allocation":
        assert not s.pairs
    for pair in s.pairs:
        for fd in pair:
            closed(fd)


def test_venv_symlink_and_explicit_dependency_paths(setup, tmp_path):
    link = tmp_path / "python-link"
    link.symlink_to(sys.executable)
    dependency = tmp_path / "deps"
    dependency.mkdir()
    args = dict(setup.args, python_executable=link, dependency_paths=(dependency,))
    launcher = module.PythonSqlClientDepartureLauncher(**args)
    assert launcher.python == link.absolute()
    assert launcher.dependency_paths == (dependency.resolve(),)
    assert launcher.admission == args["admission"]
    from hashlib import sha256

    assert launcher.admission_sha256 == sha256(args["admission"]).hexdigest()
