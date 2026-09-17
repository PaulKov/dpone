from __future__ import annotations

import io
import os
import subprocess
import sys
from pathlib import Path
from threading import Event

import pytest

from dpone.adapters.dbt_physical_transport_subprocess import PhysicalTransportLaunchLease
from dpone.adapters.dbt_process_supervisor import DbtProcessSupervisor
from dpone.adapters.dbt_subprocess import SubprocessDbtCommandRunner
from dpone.contracts.dbt_physical_transport_delivery import PhysicalTransportDelivery
from dpone.contracts.dbt_publishing import DbtPublishingError
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from dpone.ports.dbt_physical_transport import PhysicalTransportLaunchContext
from tests.test_dbt_physical_transport_delivery import packet_payload


def _args(root: Path) -> tuple[str, ...]:
    return "dbt", "build", "--target-path", str((root / "attempt" / "target").absolute())


class _Launch:
    def __init__(self, context: PhysicalTransportLaunchContext, *, fail_exit: bool = False) -> None:
        self.context = context
        self.fail_exit = fail_exit
        self.entered = False
        self.exited = False

    def __enter__(self) -> PhysicalTransportLaunchContext:
        self.entered = True
        return self.context

    def __exit__(self, *exc: object) -> None:
        self.exited = True
        if self.fail_exit:
            raise RuntimeError("synthetic lease close failure")


class _Process:
    pid = 123

    def __init__(
        self,
        *,
        launch: _Launch,
        cancellation: Event | None = None,
        cancel_on_success: bool = False,
    ) -> None:
        self.stdout = io.BytesIO()
        self.stderr = io.BytesIO()
        self.launch = launch
        self.cancellation = cancellation
        self.cancel_on_success = cancel_on_success
        self.wait_calls = 0
        self.terminate_calls = 0

    def wait(self, timeout: float | None = None) -> int:
        assert self.launch.exited, "the parent descriptor lease must close immediately after spawn"
        self.wait_calls += 1
        if self.cancellation is None:
            return 0
        if self.cancel_on_success:
            self.cancellation.set()
            return 0
        if self.wait_calls == 1:
            self.cancellation.set()
            assert timeout is not None
            raise subprocess.TimeoutExpired(("dbt",), timeout)
        return 0

    def terminate(self) -> None:
        self.terminate_calls += 1

    def kill(self) -> None:
        self.terminate_calls += 1


def test_runner_adds_only_reserved_descriptor_context_around_spawn(tmp_path: Path) -> None:
    cancellation = Event()
    context = PhysicalTransportLaunchContext((9,), 20_000_000_000, cancellation)
    launch = _Launch(context)
    observed: dict[str, object] = {}
    process = _Process(launch=launch)

    def popen(_args: tuple[str, ...], **kwargs: object) -> _Process:
        observed.update(kwargs)
        assert launch.entered and not launch.exited
        return process

    result = SubprocessDbtCommandRunner(
        popen_factory=popen,
        dbt_executable="/runtime/bin/dbt",
        physical_transport_launch=launch,
        monotonic_ns_clock=lambda: 1_000_000_000,
    ).run(_args(tmp_path), cwd=tmp_path, timeout_seconds=10, redactions=())

    assert result.exit_code == 0
    assert launch.exited
    assert observed["close_fds"] is True
    assert observed["pass_fds"] == (9,)
    environment = observed["env"]
    assert isinstance(environment, dict)
    assert environment["DPONE_PHYSICAL_TRANSPORT_FD"] == "9"


def test_owner_cancellation_uses_existing_supervised_cleanup_without_retry(tmp_path: Path) -> None:
    cancellation = Event()
    context = PhysicalTransportLaunchContext((9,), 20_000_000_000, cancellation)
    launch = _Launch(context)
    process = _Process(launch=launch, cancellation=cancellation)
    runner = SubprocessDbtCommandRunner(
        popen_factory=lambda *_args, **_kwargs: process,
        dbt_executable="/runtime/bin/dbt",
        physical_transport_launch=launch,
        monotonic_ns_clock=lambda: 1_000_000_000,
        process_supervisor=DbtProcessSupervisor(posix=False),
    )

    with pytest.raises(DbtPublishingError, match="timed out"):
        runner.run(_args(tmp_path), cwd=tmp_path, timeout_seconds=10, redactions=())

    assert process.wait_calls == 2
    assert process.terminate_calls == 1


def test_success_return_after_owner_cancellation_is_not_accepted(tmp_path: Path) -> None:
    cancellation = Event()
    context = PhysicalTransportLaunchContext((9,), 20_000_000_000, cancellation)
    launch = _Launch(context)
    process = _Process(launch=launch, cancellation=cancellation, cancel_on_success=True)
    runner = SubprocessDbtCommandRunner(
        popen_factory=lambda *_args, **_kwargs: process,
        dbt_executable="/runtime/bin/dbt",
        physical_transport_launch=launch,
        monotonic_ns_clock=lambda: 1_000_000_000,
        process_supervisor=DbtProcessSupervisor(posix=False),
    )

    with pytest.raises(DbtPublishingError, match="timed out"):
        runner.run(_args(tmp_path), cwd=tmp_path, timeout_seconds=10, redactions=())

    assert process.wait_calls == 2
    assert process.terminate_calls == 1


def test_success_return_after_absolute_delivery_deadline_is_not_accepted(tmp_path: Path) -> None:
    context = PhysicalTransportLaunchContext((9,), 2_000_000_000, Event())
    launch = _Launch(context)
    process = _Process(launch=launch)
    observed_times = iter((1_000_000_000, 1_000_000_000, 3_000_000_000))
    runner = SubprocessDbtCommandRunner(
        popen_factory=lambda *_args, **_kwargs: process,
        dbt_executable="/runtime/bin/dbt",
        physical_transport_launch=launch,
        monotonic_ns_clock=lambda: next(observed_times),
        process_supervisor=DbtProcessSupervisor(posix=False),
    )

    with pytest.raises(DbtPublishingError, match="timed out"):
        runner.run(_args(tmp_path), cwd=tmp_path, timeout_seconds=10, redactions=())

    assert process.wait_calls == 2
    assert process.terminate_calls == 1


def test_default_runner_does_not_add_descriptor_or_reserved_environment(tmp_path: Path) -> None:
    observed: dict[str, object] = {}

    class _Completed:
        stdout = io.BytesIO()
        stderr = io.BytesIO()

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            return 0

    def popen(_args: tuple[str, ...], **kwargs: object) -> _Completed:
        observed.update(kwargs)
        return _Completed()

    SubprocessDbtCommandRunner(popen_factory=popen).run(
        _args(tmp_path), cwd=tmp_path, timeout_seconds=10, redactions=()
    )
    assert "pass_fds" not in observed
    environment = observed["env"]
    assert isinstance(environment, dict)
    assert "DPONE_PHYSICAL_TRANSPORT_FD" not in environment


def test_actual_runner_spawn_inherits_one_anonymous_packet_and_closes_parent(tmp_path: Path) -> None:
    executable = tmp_path / "packet-reader"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import os\n"
        "fd = int(os.environ.pop('DPONE_PHYSICAL_TRANSPORT_FD'))\n"
        "payload = os.read(fd, 1 << 20)\n"
        "os.close(fd)\n"
        "print(f'packet-bytes={len(payload)}')\n",
        encoding="utf-8",
    )
    executable.chmod(0o700)
    delivery = PhysicalTransportDelivery(encode_native_delivery_json(packet_payload()))
    lease = PhysicalTransportLaunchLease(
        delivery=delivery,
        profile_directory=tmp_path,
        cancellation=Event(),
    )

    result = SubprocessDbtCommandRunner(
        dbt_executable=os.fspath(executable),
        physical_transport_launch=lease,
    ).run(_args(tmp_path), cwd=tmp_path, timeout_seconds=10, redactions=())

    assert result.exit_code == 0
    assert result.stdout.strip() == f"packet-bytes={len(delivery.payload)}"
    assert not tuple(tmp_path.glob(".dpone-transport-*"))
    with pytest.raises(ValueError, match="already consumed"):
        lease.__enter__()


def test_spawn_failure_consumes_and_closes_actual_launch_lease(tmp_path: Path) -> None:
    lease = PhysicalTransportLaunchLease(
        delivery=PhysicalTransportDelivery(encode_native_delivery_json(packet_payload())),
        profile_directory=tmp_path,
        cancellation=Event(),
    )

    def fail_spawn(*_args: object, **_kwargs: object) -> _Process:
        raise OSError("synthetic spawn failure")

    runner = SubprocessDbtCommandRunner(
        popen_factory=fail_spawn,
        physical_transport_launch=lease,
    )
    with pytest.raises(DbtPublishingError, match="unavailable or unsafe"):
        runner.run(_args(tmp_path), cwd=tmp_path, timeout_seconds=10, redactions=())

    assert not tuple(tmp_path.glob(".dpone-transport-*"))
    with pytest.raises(ValueError, match="already consumed"):
        lease.__enter__()


def test_lease_exit_failure_cleans_up_spawned_process(tmp_path: Path) -> None:
    context = PhysicalTransportLaunchContext((9,), 20_000_000_000, Event())
    launch = _Launch(context, fail_exit=True)
    process = _Process(launch=launch)
    runner = SubprocessDbtCommandRunner(
        popen_factory=lambda *_args, **_kwargs: process,
        physical_transport_launch=launch,
        process_supervisor=DbtProcessSupervisor(posix=False),
    )

    with pytest.raises(DbtPublishingError, match="unavailable or unsafe"):
        runner.run(_args(tmp_path), cwd=tmp_path, timeout_seconds=10, redactions=())

    assert launch.exited
    assert process.terminate_calls == 1
