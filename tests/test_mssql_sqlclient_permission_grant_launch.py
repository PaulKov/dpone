"""Launcher performs one spawn and returns only after custody readiness."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters import mssql_sqlclient_permission_grant_launch as module
from dpone.adapters.mssql_tds_coordinator_connection import (
    TdsBinaryPin,
    TdsCoordinatorBuild,
    encode_connection_admission,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionProfile
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown
from tests.test_mssql_sqlclient_permission_grant_request import launch_request


@pytest.fixture
def setup(tmp_path, monkeypatch):
    bootstrap = tmp_path / "dpone/app/mssql_sqlclient_permission_grant_bootstrap.py"
    bootstrap.parent.mkdir(parents=True)
    bootstrap.write_text("# fixed synthetic source\n")
    pin = TdsBinaryPin(Path(sys.executable), "b" * 64)
    admission = encode_connection_admission(
        TdsCoordinatorBuild(pin, pin, pin, pin), TdsConnectionProfile.SYNTHETIC_LOCAL
    )
    value, binding = launch_request()
    value = __import__("dataclasses").replace(
        value, admission_sha256=__import__("hashlib").sha256(admission).hexdigest()
    )
    launcher = module.PythonSqlClientPermissionGrantLauncher(
        python_executable=Path(sys.executable),
        package_root=tmp_path,
        implementation_sha256=binding.operation.implementation_sha256,
        admission=admission,
        max_address_space_bytes=1 << 30,
    )
    monkeypatch.setattr(module, "worker_installation_digest", lambda root: binding.operation.implementation_sha256)
    monkeypatch.setattr(module.launch_shims, "PERMISSION_GRANT_SOURCE_SHIM", "FIXED_SHIM", raising=False)
    monkeypatch.setattr(module.LinuxTdsProcess, "admit", lambda: None)
    identities = iter((binding.startup.process, binding.startup.process, binding.startup.process))
    monkeypatch.setattr(module.LinuxTdsProcess, "identify", lambda pid: next(identities))
    monkeypatch.setattr(module.time, "monotonic", lambda: 1.0)
    monkeypatch.setattr(module.secrets, "token_bytes", lambda count: b"n" * count)
    calls = []
    process = SimpleNamespace(pid=binding.startup.process.pid, stdout=None, returncode=None)

    def popen(argv, **kwargs):
        calls.append((argv, kwargs))
        return process

    monkeypatch.setattr(module.subprocess, "Popen", popen)
    events = []
    adopted = SimpleNamespace(await_custody=lambda *, deadline: events.append(("custody", deadline)))
    monkeypatch.setattr(module.SqlClientPermissionGrantProcess, "adopt", lambda *args, **kwargs: adopted)
    return SimpleNamespace(launcher=launcher, value=value, calls=calls, events=events, adopted=adopted, process=process)


def test_one_spawn_fixed_metadata_and_custody_before_return(setup):
    result = setup.launcher.launch(setup.value)
    assert result is setup.adopted
    assert setup.events == [("custody", setup.value.startup_deadline)]
    assert len(setup.calls) == 1
    argv, options = setup.calls[0]
    assert "FIXED_SHIM" in argv and "--public-request" in argv
    assert "PRIVATE_CANARY" not in repr(argv)
    assert options["env"] == {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}
    assert options["cwd"] == str(setup.launcher.package_root)
    assert options["close_fds"] is True and len(options["pass_fds"]) == 1
    assert all(name not in argv for name in ("--password", "--credentials"))


def test_custody_failure_after_spawn_retains_unknown(setup, monkeypatch):
    setup.adopted.await_custody = lambda **kwargs: (_ for _ in ()).throw(ValueError("private detail"))
    with pytest.raises(TdsLaunchUnknown) as caught:
        setup.launcher.launch(setup.value)
    assert caught.value.launch.process is setup.process
    assert "private detail" not in str(caught.value)


def test_adopted_custody_unknown_preserves_exact_process_capability(setup):
    failure = module.permission_process.PermissionGrantProcessUnknown(setup.adopted)
    setup.adopted.await_custody = lambda **kwargs: (_ for _ in ()).throw(failure)
    with pytest.raises(module.permission_process.PermissionGrantProcessUnknown) as caught:
        setup.launcher.launch(setup.value)
    assert caught.value is failure and caught.value.process is setup.adopted


def test_invalid_request_precedes_spawn(setup):
    setup.value = __import__("dataclasses").replace(setup.value, admission_sha256="c" * 64)
    with pytest.raises(ValueError):
        setup.launcher.launch(setup.value)
    assert setup.calls == []


def test_expired_deadline_precedes_spawn(setup):
    setup.value = __import__("dataclasses").replace(setup.value, startup_deadline=0.5, operation_deadline=0.5)
    with pytest.raises(ValueError):
        setup.launcher.launch(setup.value)
    assert setup.calls == []
