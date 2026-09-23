"""Spawn/ownership unit checks; synthetic handles are not a live launch pass."""

import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_sqlclient_installation import AdmittedSqlClientInstallation
from dpone.adapters.mssql_sqlclient_launch import SqlClientLauncher, _deadline_ns
from dpone.contracts.mssql_tds_worker import TdsProcessIdentity
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown


@pytest.fixture
def setup(tmp_path, monkeypatch):
    path = tmp_path / "input.bin"
    path.write_bytes(b"synthetic")
    fd = os.open(path, os.O_RDONLY)
    installation = AdmittedSqlClientInstallation(
        Path("/python"),
        "a" * 64,
        Path("/framework"),
        "b" * 64,
        Path("/runtime/dotnet"),
        Path("/runtime"),
        Path("/companion"),
        Path("/companion/Worker.dll"),
        Path("/manifest.json"),
        "c" * 64,
        (),
    )
    identity = TdsProcessIdentity("a" * 64, "11111111-1111-4111-8111-111111111111", 456, 2)
    handle = SimpleNamespace(
        identity=identity,
        close=lambda: None,
        wait=lambda **kw: SimpleNamespace(reaped=True, exit_code=0),
        contain=lambda **kw: SimpleNamespace(reaped=True, exit_code=-9),
    )
    calls = []
    monkeypatch.setattr(AdmittedSqlClientInstallation, "assert_admitted", lambda *a, **kw: None)
    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.LinuxTdsProcess.admit", lambda: None)
    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.LinuxTdsProcess.identify", lambda pid: identity)
    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.LinuxTdsProcess.acquire", lambda value: handle)
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_launch.subprocess.Popen",
        lambda command, **kw: calls.append((command, kw)) or SimpleNamespace(pid=456, stdout=None, returncode=None),
    )
    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.write_worker_control", lambda *a, **kw: None)
    launcher = SqlClientLauncher(
        installation=installation,
        attempt_sha256="d" * 64,
        input_binding_sha256="e" * 64,
        input_fd=fd,
        address_space_bytes=8 * 1024**3,
    )
    yield SimpleNamespace(launcher=launcher, fd=fd, calls=calls, handle=handle, deadline=time.monotonic() + 5)
    os.close(fd)


def test_isolated_launch_and_no_inherited_environment(setup, monkeypatch):
    monkeypatch.setenv("DOTNET_STARTUP_HOOKS", "poison")
    worker = setup.launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    command, options = setup.calls[0]
    assert command[:4] == ["/python", "-I", "-S", "-B"]
    assert options["close_fds"] is True and len(options["pass_fds"]) == 7
    assert setup.fd not in options["pass_fds"]
    assert options["env"] == {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}
    assert options["cwd"] == "/"
    assert worker.identity == setup.handle.identity
    worker.terminate(deadline=setup.deadline)
    worker.close()
    assert os.fstat(setup.fd).st_size == 9
    with pytest.raises(ValueError, match="launch_used"):
        setup.launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)


def test_failed_bootstrap_retains_capability(setup, monkeypatch):
    def fail(*a, **kw):
        raise OSError("injected partial delivery")

    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.write_worker_control", fail)
    with pytest.raises(TdsLaunchUnknown) as failure:
        setup.launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    capability = failure.value.launch
    assert capability.handle is setup.handle and capability.descriptors
    capability.contain(deadline=setup.deadline)
    capability.close()


def test_admission_failure_never_spawns(setup, monkeypatch):
    monkeypatch.setattr(
        AdmittedSqlClientInstallation,
        "assert_admitted",
        lambda *a, **kw: (_ for _ in ()).throw(ValueError("admission")),
    )
    with pytest.raises(ValueError, match="admission"):
        setup.launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    assert not setup.calls


@pytest.mark.parametrize("value", [float("inf"), float("nan"), -1, True, 2**63])
def test_deadline_rejects_unbounded_values(value):
    with pytest.raises(ValueError):
        _deadline_ns(value)


def typed_descriptor(fd):
    from hashlib import sha256

    from dpone.contracts.mssql_native_chunks import TdsInputReceipt
    from dpone.contracts.mssql_sqlclient_input import SqlClientFileIdentity, SqlClientInputDescriptor
    from dpone.contracts.native_wire_layout import NativeWireColumnLayout

    info = os.fstat(fd)
    return SqlClientInputDescriptor(
        1,
        fd,
        (NativeWireColumnLayout("value", "bigint", "Int64", False, "bigint", 0, 8, None, None, None),),
        TdsInputReceipt(1, info.st_size, sha256(b"synthetic").hexdigest()),
        1024,
        SqlClientFileIdentity(info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns),
    )


def test_typed_handoff_binds_actual_child_duplicate(setup):
    from dataclasses import replace

    from dpone.contracts.mssql_sqlclient_input import input_descriptor_digest

    original = typed_descriptor(setup.fd)
    launcher = SqlClientLauncher.for_input_descriptor(
        installation=setup.launcher._installation,
        attempt_sha256="d" * 64,
        input_descriptor=original,
        address_space_bytes=8 * 1024**3,
    )
    assert os.lseek(setup.fd, 0, os.SEEK_CUR) == 0
    worker = launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    try:
        child_fd = worker.declared_launch.descriptors.input
        assert child_fd != original.fd
        assert worker.bound_input == replace(original, fd=child_fd)
        assert worker.declared_launch.input_binding_sha256 == input_descriptor_digest(worker.bound_input)
        assert worker.declared_launch.input_binding_sha256 != input_descriptor_digest(original)
        assert child_fd in setup.calls[0][1]["pass_fds"]
        assert os.lseek(setup.fd, 0, os.SEEK_CUR) == 0
    finally:
        worker.terminate(deadline=setup.deadline)
        worker.close()


def typed_launcher(setup, descriptor=None):
    return SqlClientLauncher.for_input_descriptor(
        installation=setup.launcher._installation,
        attempt_sha256="d" * 64,
        input_descriptor=typed_descriptor(setup.fd) if descriptor is None else descriptor,
        address_space_bytes=8 * 1024**3,
    )


def track_fds(monkeypatch):
    created = []
    pipe, dup = os.pipe, os.dup

    def tracked_pipe():
        pair = pipe()
        created.extend(pair)
        return pair

    def tracked_dup(fd):
        result = dup(fd)
        created.append(result)
        return result

    monkeypatch.setattr(os, "pipe", tracked_pipe)
    monkeypatch.setattr(os, "dup", tracked_dup)
    return created


def assert_closed(fds):
    for fd in fds:
        with pytest.raises(OSError):
            os.fstat(fd)


@pytest.mark.parametrize("field", ["st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_mode"])
@pytest.mark.parametrize("check", [1, 2])
def test_actual_duplicate_identity_rechecked_before_spawn(setup, monkeypatch, field, check):
    import stat

    launcher = typed_launcher(setup)
    created = track_fds(monkeypatch)
    real = os.fstat
    checks = 0

    def changed(fd):
        nonlocal checks
        info = real(fd)
        if fd == setup.fd:
            return info
        checks += 1
        if checks != check:
            return info
        values = {
            key: getattr(info, key) for key in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_mode")
        }
        values[field] = stat.S_IFIFO if field == "st_mode" else values[field] + 1
        return SimpleNamespace(**values)

    monkeypatch.setattr(os, "fstat", changed)
    with pytest.raises(ValueError, match="input_descriptor"):
        launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    assert not setup.calls and checks >= check
    monkeypatch.setattr(os, "fstat", real)
    assert_closed(created)
    assert real(setup.fd).st_size == 9
    assert os.lseek(setup.fd, 0, os.SEEK_CUR) == 0


@pytest.mark.parametrize("flag", [os.O_WRONLY, os.O_RDWR, getattr(os, "O_PATH", 0x200000)])
def test_duplicate_access_flags_fail_closed(setup, monkeypatch, flag):
    import fcntl

    launcher = typed_launcher(setup)
    created = track_fds(monkeypatch)
    real = fcntl.fcntl
    monkeypatch.setattr(fcntl, "fcntl", lambda fd, op: real(fd, op) if fd == setup.fd else flag)
    with pytest.raises(ValueError, match="input_descriptor"):
        launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    assert not setup.calls
    assert_closed(created)
    assert os.fstat(setup.fd).st_size == 9


def test_nonzero_shared_offset_rejected_without_reset(setup, monkeypatch):
    launcher = typed_launcher(setup)
    created = track_fds(monkeypatch)
    os.lseek(setup.fd, 1, os.SEEK_SET)
    with pytest.raises(ValueError, match="input_descriptor"):
        launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    assert os.lseek(setup.fd, 0, os.SEEK_CUR) == 1 and not setup.calls
    assert_closed(created)


def test_typed_factory_performs_no_fd_io(setup, monkeypatch):
    descriptor = typed_descriptor(setup.fd)

    def forbidden(*a, **kw):
        raise AssertionError("constructor allocated or observed FD")

    for operation in ("dup", "fstat", "lseek", "pipe", "read"):
        monkeypatch.setattr(os, operation, forbidden)
    launcher = typed_launcher(setup, descriptor)
    assert launcher._input_descriptor is descriptor


@pytest.mark.parametrize("at_check", [1, 2])
def test_expired_during_duplicate_observation_never_spawns(setup, monkeypatch, at_check):
    launcher = typed_launcher(setup)
    created = track_fds(monkeypatch)
    real = os.fstat
    checks = 0
    expired = False

    def observe(fd):
        nonlocal checks, expired
        value = real(fd)
        if fd != setup.fd:
            checks += 1
            expired = checks >= at_check
        return value

    monkeypatch.setattr(os, "fstat", observe)
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_launch.time.monotonic_ns", lambda: int(setup.deadline * 1e9) if expired else 1
    )
    with pytest.raises(ValueError, match="deadline"):
        launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    assert not setup.calls
    monkeypatch.setattr(os, "fstat", real)
    assert_closed(created)
    assert os.fstat(setup.fd).st_size == 9


def test_typed_postspawn_failure_retains_capability(setup, monkeypatch):
    def fail(*a, **kw):
        raise OSError("synthetic bootstrap failure")

    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.write_worker_control", fail)
    with pytest.raises(TdsLaunchUnknown) as caught:
        typed_launcher(setup).spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    capability = caught.value.launch
    assert capability.handle is setup.handle and capability.descriptors
    capability.contain(deadline=setup.deadline)
    capability.close()
    assert os.fstat(setup.fd).st_size == 9


def test_typed_pre_spawn_failure_closes_duplicate_and_cache(setup, monkeypatch):
    import tempfile

    launcher = typed_launcher(setup)
    created = track_fds(monkeypatch)
    cache_paths = []

    def cache(**kwargs):
        result = tempfile.TemporaryDirectory(**kwargs)
        cache_paths.append(Path(result.name))
        return result

    def fail(*a, **kw):
        raise OSError("synthetic spawn failure")

    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.TemporaryDirectory", cache)
    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.subprocess.Popen", fail)
    with pytest.raises(OSError, match="spawn failure"):
        launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    assert_closed(created)
    assert cache_paths and all(not path.exists() for path in cache_paths)
    assert os.fstat(setup.fd).st_size == 9


def test_bound_handoff_builds_matching_job_and_rejects_original_fd(setup):
    from dataclasses import replace

    from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy
    from dpone.contracts.mssql_sqlclient_job import decode_job, encode_job, validate_job
    from dpone.contracts.mssql_sqlclient_launch import launch_digest
    from dpone.contracts.mssql_tds_result import attempt_identity_digest

    template = decode_job((Path(__file__).parent / "fixtures/mssql_sqlclient/jobs/rows.job.json").read_bytes())
    original = typed_descriptor(setup.fd)
    attempt = replace(template.identity, file_sha256=original.expected.file_sha256)
    launcher = SqlClientLauncher.for_input_descriptor(
        installation=setup.launcher._installation,
        attempt_sha256=attempt_identity_digest(attempt),
        input_descriptor=original,
        address_space_bytes=8 * 1024**3,
    )
    worker = launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    try:
        value = replace(
            template, launch_sha256=launch_digest(worker.declared_launch), identity=attempt, input=worker.bound_input
        )
        decoded = decode_job(encode_job(value))

        def validate(value):
            validate_job(
                value,
                launch=worker.declared_launch,
                ownership=template.ownership,
                object_identity=template.object_identity,
                policy=NativeBulkTransportPolicy(
                    "mssql_sqlclient",
                    template.input_mode,
                    8 * 1024**3,
                    batch_rows=template.batch_rows,
                    max_input_batch_bytes=template.max_input_batch_bytes,
                ),
                session_nonce=template.session_nonce,
                tls_profile=template.credentials.tls_profile,
                allow_disposable_test=True,
                now_ns=1,
            )

        validate(decoded)
        assert decoded.input.fd == worker.declared_launch.descriptors.input
        with pytest.raises(ValueError, match="job_invalid"):
            validate(replace(value, input=original))
    finally:
        worker.terminate(deadline=setup.deadline)
        worker.close()


def test_input_digest_computed_on_actual_duplicate_before_spawn(setup, monkeypatch):
    from dpone.contracts.mssql_sqlclient_input import input_descriptor_digest

    launcher = typed_launcher(setup)
    hashes = []
    real_spawn = __import__("subprocess").Popen

    def digest(record):
        assert record.fd != setup.fd and os.fstat(record.fd).st_ino == os.fstat(setup.fd).st_ino
        hashes.append(record)
        return input_descriptor_digest(record)

    def spawn(*a, **kw):
        assert len(hashes) == 1
        assert hashes[0].fd in kw["pass_fds"]
        return real_spawn(*a, **kw)

    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.input_descriptor_digest", digest)
    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.subprocess.Popen", spawn)
    worker = launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    worker.terminate(deadline=setup.deadline)
    worker.close()


def test_digest_failure_closes_original_duplicate_without_spawning(setup, monkeypatch):
    launcher = typed_launcher(setup)
    created = track_fds(monkeypatch)

    def fail(record):
        raise ValueError("synthetic digest failure")

    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.input_descriptor_digest", fail)
    with pytest.raises(ValueError, match="digest failure"):
        launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    assert not setup.calls
    assert_closed(created)
    assert os.fstat(setup.fd).st_size == 9


@pytest.mark.parametrize("site", ["children", "gate"])
def test_unknown_launch_close_retains_original_token_and_never_retries(setup, monkeypatch, site):
    from dpone.contracts.bounded_window import WindowOutcomeUnknown

    original_close = os.close
    closed = []
    injected = []

    def ambiguous_close(fd):
        original_close(fd)
        closed.append(fd)
        if not injected and setup.calls:
            inherited = setup.calls[0][1]["pass_fds"]
            if (site == "children" and fd in inherited) or (site == "gate" and fd not in inherited):
                injected.append(fd)
                raise OSError("close acknowledgment lost")

    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.os.close", ambiguous_close)
    with pytest.raises(TdsLaunchUnknown) as failure:
        setup.launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    capability = failure.value.launch
    assert len(injected) == 1
    capability.contain(deadline=setup.deadline)
    # Cleanup may close independent resources, but must retain UNKNOWN and never
    # retry a descriptor whose number may already have been reused by the OS.
    with pytest.raises(WindowOutcomeUnknown):
        capability.close()
    assert closed.count(injected[0]) == 1
    assert any(r.value == injected[0] and r.state == "CLOSE_UNKNOWN" for r in capability._resources._resources)


def test_failed_pidfd_acquisition_retains_original_unknown_close_record(setup, monkeypatch):
    from dpone.adapters.mssql_tds_process import TdsProcessError, _AcquisitionFailure

    error = TdsProcessError("synthetic acquisition failure")
    error.acquisition = _AcquisitionFailure(987654, True)

    def fail(identity):
        raise error

    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.LinuxTdsProcess.acquire", fail)
    with pytest.raises(TdsLaunchUnknown) as failure:
        setup.launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    capability = failure.value.launch
    try:
        with pytest.raises(TdsLaunchUnknown):
            capability.contain(deadline=setup.deadline)
        assert any(
            r.kind == "acquisition" and r.value == 987654 and r.state == "CLOSE_UNKNOWN"
            for r in capability._resources._resources
        )
    finally:
        # Fixture Popen is synthetic. Release only this test's real pipe copies;
        # this teardown is neither authenticated containment nor a recovery path.
        for fd in capability.descriptors:
            os.close(fd)
        capability._resources._cache.cleanup()


def test_pre_spawn_ambiguous_cleanup_retains_resource_without_inventing_child(setup, monkeypatch):
    original_close = os.close
    original_set_blocking = os.set_blocking
    injected = []

    def fail_setup(fd, blocking):
        original_set_blocking(fd, blocking)
        raise ValueError("synthetic pre-spawn setup failure")

    def ambiguous_close(fd):
        original_close(fd)
        if not injected:
            injected.append(fd)
            raise OSError("close acknowledgment lost")

    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.os.set_blocking", fail_setup)
    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_launch.os.close", ambiguous_close)
    with pytest.raises(TdsLaunchUnknown) as failure:
        setup.launcher.spawn(startup_deadline=setup.deadline, operation_deadline=setup.deadline)
    assert not setup.calls
    custody = failure.value.launch._resources
    assert custody.process is None and custody.handle is None
    assert any(r.value == injected[0] and r.state == "CLOSE_UNKNOWN" for r in custody._resources)
