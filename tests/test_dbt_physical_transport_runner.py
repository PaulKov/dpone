from __future__ import annotations

import io
import subprocess
from pathlib import Path
from threading import Event

import pytest

from dpone.adapters.dbt_process_supervisor import DbtProcessSupervisor
from dpone.adapters.dbt_subprocess import SubprocessDbtCommandRunner
from dpone.contracts.dbt_publishing import DbtPublishingError
from dpone.ports.dbt_physical_transport import PhysicalTransportLaunchContext


def _args(root: Path) -> tuple[str, ...]:
    return "dbt", "build", "--target-path", str((root / "attempt" / "target").absolute())


class _Launch:
    def __init__(self, context: PhysicalTransportLaunchContext) -> None:
        self.context = context
        self.entered = False
        self.exited = False

    def __enter__(self) -> PhysicalTransportLaunchContext:
        self.entered = True
        return self.context

    def __exit__(self, *exc: object) -> None:
        self.exited = True


class _Process:
    pid = 123

    def __init__(self, *, launch: _Launch, cancellation: Event | None = None) -> None:
        self.stdout = io.BytesIO()
        self.stderr = io.BytesIO()
        self.launch = launch
        self.cancellation = cancellation
        self.wait_calls = 0
        self.terminate_calls = 0

    def wait(self, timeout: float | None = None) -> int:
        assert self.launch.exited, "the parent descriptor lease must close immediately after spawn"
        self.wait_calls += 1
        if self.cancellation is None:
            return 0
        if self.wait_calls == 1:
            self.cancellation.set()
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
