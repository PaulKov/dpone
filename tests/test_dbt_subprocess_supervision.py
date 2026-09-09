from __future__ import annotations

import io
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, cast

import pytest

import dpone.adapters.dbt_subprocess as dbt_subprocess_module
from dpone.adapters.dbt_process_supervisor import DbtProcessSupervisor
from dpone.adapters.dbt_subprocess import SubprocessDbtCommandRunner
from dpone.contracts.dbt_publishing import DbtPublishingError


class _TimeoutProcess:
    def __init__(self) -> None:
        self.stdout: io.BytesIO | None = io.BytesIO(b"stdout sentinel-runtime-secret")
        self.stderr: io.BytesIO | None = io.BytesIO(b"stderr sentinel-runtime-secret")
        self.returncode: int | None = None
        self.terminate_calls = 0
        self.kill_calls = 0

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        if self.returncode is None:
            raise subprocess.TimeoutExpired(("dbt", "build"), 0)
        return self.returncode

    def terminate(self) -> None:
        self.terminate_calls += 1

    def kill(self) -> None:
        self.kill_calls += 1
        self.returncode = -signal.SIGKILL


class _InterruptProcess(_TimeoutProcess):
    def __init__(self) -> None:
        super().__init__()
        self._interrupted = False

    def wait(self, timeout: float | None = None) -> int:
        if not self._interrupted:
            self._interrupted = True
            raise KeyboardInterrupt
        return super().wait(timeout)


class _UnsafeBlockingStream:
    def __init__(self) -> None:
        self.closed = False
        self.read_called = False

    def fileno(self) -> int:
        raise OSError("no bounded descriptor")

    def read(self, _size: int = -1) -> bytes:
        self.read_called = True
        raise AssertionError("unsafe stream must not be read")

    def close(self) -> None:
        self.closed = True


def _dbt_args(root: Path) -> tuple[str, ...]:
    return (
        "dbt",
        "build",
        "--target-path",
        str((root / "attempt" / "target").absolute()),
    )


@pytest.mark.parametrize("timeout_seconds", (True, 0, -1, float("nan"), float("inf")))
def test_cleanup_deadlines_must_be_finite_positive_numbers(timeout_seconds: float) -> None:
    with pytest.raises(ValueError):
        SubprocessDbtCommandRunner(collector_join_timeout_seconds=timeout_seconds)
    with pytest.raises(ValueError):
        DbtProcessSupervisor(terminate_grace_seconds=timeout_seconds)


def test_posix_runner_starts_dbt_in_a_new_session(tmp_path: Path) -> None:
    observed: dict[str, object] = {}

    class _CompletedProcess:
        stdout = io.BytesIO(b"ok")
        stderr = io.BytesIO()

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            return 0

    def popen(args: tuple[str, ...], **kwargs: object) -> _CompletedProcess:
        observed["args"] = args
        observed.update(kwargs)
        return _CompletedProcess()

    result = SubprocessDbtCommandRunner(
        popen_factory=popen,
        process_supervisor=DbtProcessSupervisor(posix=True),
        dbt_executable="/locked/python-environment/bin/dbt",
    ).run(
        _dbt_args(tmp_path),
        cwd=tmp_path,
        timeout_seconds=10,
        redactions=(),
    )

    assert result.exit_code == 0
    assert observed["args"] == (
        "/locked/python-environment/bin/dbt",
        *_dbt_args(tmp_path)[1:],
    )
    assert observed["start_new_session"] is True


def test_non_posix_timeout_uses_bounded_parent_fallback_and_redacts_error(
    tmp_path: Path,
) -> None:
    process = _TimeoutProcess()
    observed: dict[str, object] = {}

    def popen(args: tuple[str, ...], **kwargs: object) -> _TimeoutProcess:
        observed["args"] = args
        observed.update(kwargs)
        return process

    runner = SubprocessDbtCommandRunner(
        popen_factory=popen,
        process_supervisor=DbtProcessSupervisor(
            posix=False,
            terminate_grace_seconds=0.01,
            kill_grace_seconds=0.01,
        ),
        collector_join_timeout_seconds=0.05,
    )

    started = time.monotonic()
    with pytest.raises(DbtPublishingError) as exc_info:
        runner.run(
            _dbt_args(tmp_path),
            cwd=tmp_path,
            timeout_seconds=1,
            redactions=("sentinel-runtime-secret",),
        )
    elapsed = time.monotonic() - started

    assert exc_info.value.code == "DPONE_DBT_EXECUTION_FAILED"
    assert "timed out" in str(exc_info.value)
    assert "sentinel-runtime-secret" not in str(exc_info.value)
    assert process.terminate_calls == 1
    assert process.kill_calls == 1
    assert observed["start_new_session"] is False
    assert elapsed < 1


def test_keyboard_interrupt_cleans_up_spawned_process_before_propagating(tmp_path: Path) -> None:
    process = _InterruptProcess()
    runner = SubprocessDbtCommandRunner(
        popen_factory=lambda *_args, **_kwargs: process,
        process_supervisor=DbtProcessSupervisor(
            posix=False,
            terminate_grace_seconds=0.01,
            kill_grace_seconds=0.01,
        ),
        collector_join_timeout_seconds=0.05,
    )

    with pytest.raises(KeyboardInterrupt):
        runner.run(
            _dbt_args(tmp_path),
            cwd=tmp_path,
            timeout_seconds=1,
            redactions=(),
        )

    assert process.terminate_calls == 1
    assert process.kill_calls == 1


def test_missing_output_pipe_still_terminates_spawned_process(tmp_path: Path) -> None:
    process = _TimeoutProcess()
    process.stderr = None

    runner = SubprocessDbtCommandRunner(
        popen_factory=lambda *_args, **_kwargs: process,
        process_supervisor=DbtProcessSupervisor(
            posix=False,
            terminate_grace_seconds=0.01,
            kill_grace_seconds=0.01,
        ),
        collector_join_timeout_seconds=0.05,
    )

    with pytest.raises(DbtPublishingError) as exc_info:
        runner.run(
            _dbt_args(tmp_path),
            cwd=tmp_path,
            timeout_seconds=1,
            redactions=(),
        )

    assert exc_info.value.code == "DPONE_DBT_EXECUTION_FAILED"
    assert "output pipe is unavailable" in str(exc_info.value)
    assert process.terminate_calls == 1
    assert process.kill_calls == 1
    assert process.stdout is not None
    assert process.stdout.closed


def test_unsupported_output_stream_fails_closed_without_starting_collector(
    tmp_path: Path,
) -> None:
    process = _TimeoutProcess()
    stdout = _UnsafeBlockingStream()
    stderr = _UnsafeBlockingStream()
    process.stdout = cast(Any, stdout)
    process.stderr = cast(Any, stderr)
    runner = SubprocessDbtCommandRunner(
        popen_factory=lambda *_args, **_kwargs: process,
        process_supervisor=DbtProcessSupervisor(
            posix=False,
            terminate_grace_seconds=0.01,
            kill_grace_seconds=0.01,
        ),
        collector_join_timeout_seconds=0.05,
    )

    started = time.monotonic()
    with pytest.raises(DbtPublishingError) as exc_info:
        runner.run(
            _dbt_args(tmp_path),
            cwd=tmp_path,
            timeout_seconds=1,
            redactions=(),
        )

    assert exc_info.value.code == "DPONE_DBT_EXECUTION_FAILED"
    assert "output pipe is unavailable or unsafe" in str(exc_info.value)
    assert process.terminate_calls == 1
    assert process.kill_calls == 1
    assert stdout.closed
    assert stderr.closed
    assert not stdout.read_called
    assert not stderr.read_called
    assert time.monotonic() - started < 1


def test_collector_start_failure_still_terminates_spawned_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _TimeoutProcess()

    def fail_to_start(_thread: object) -> None:
        raise RuntimeError("thread unavailable")

    monkeypatch.setattr(dbt_subprocess_module.Thread, "start", fail_to_start)
    runner = SubprocessDbtCommandRunner(
        popen_factory=lambda *_args, **_kwargs: process,
        process_supervisor=DbtProcessSupervisor(
            posix=False,
            terminate_grace_seconds=0.01,
            kill_grace_seconds=0.01,
        ),
        collector_join_timeout_seconds=0.05,
    )

    with pytest.raises(DbtPublishingError) as exc_info:
        runner.run(
            _dbt_args(tmp_path),
            cwd=tmp_path,
            timeout_seconds=1,
            redactions=(),
        )

    assert exc_info.value.code == "DPONE_DBT_EXECUTION_FAILED"
    assert process.terminate_calls == 1
    assert process.kill_calls == 1


@pytest.mark.skipif(os.name != "posix", reason="POSIX non-blocking pipe contract")
def test_signal_failure_still_closes_parent_pipes_and_collectors(
    tmp_path: Path,
) -> None:
    stdout_read, stdout_write = os.pipe()
    stderr_read, stderr_write = os.pipe()
    process = _TimeoutProcess()
    process.pid = 424242
    process.stdout = os.fdopen(stdout_read, "rb", buffering=0)
    process.stderr = os.fdopen(stderr_read, "rb", buffering=0)

    def deny_signal(_process_group_id: int, _signal_number: int) -> None:
        raise PermissionError

    runner = SubprocessDbtCommandRunner(
        popen_factory=lambda *_args, **_kwargs: process,
        process_supervisor=DbtProcessSupervisor(
            posix=True,
            terminate_grace_seconds=0.01,
            kill_grace_seconds=0.01,
            kill_process_group=deny_signal,
        ),
        collector_join_timeout_seconds=0.2,
    )

    started = time.monotonic()
    try:
        with pytest.raises(DbtPublishingError) as exc_info:
            runner.run(
                _dbt_args(tmp_path),
                cwd=tmp_path,
                timeout_seconds=1,
                redactions=(),
            )
    finally:
        os.close(stdout_write)
        os.close(stderr_write)

    assert exc_info.value.code == "DPONE_DBT_EXECUTION_FAILED"
    assert "cleanup did not complete safely" in str(exc_info.value)
    assert process.stdout.closed
    assert process.stderr.closed
    assert time.monotonic() - started < 1


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group contract")
def test_timeout_terminates_the_complete_process_group(tmp_path: Path) -> None:
    child_pid_path = tmp_path / "child.pid"
    parent_code = _parent_with_child_code(child_pid_path, parent_exits=False)
    runner = SubprocessDbtCommandRunner(
        popen_factory=_python_process(parent_code),
        process_supervisor=DbtProcessSupervisor(
            terminate_grace_seconds=0.05,
            kill_grace_seconds=0.5,
        ),
        collector_join_timeout_seconds=0.2,
    )

    with pytest.raises(DbtPublishingError) as exc_info:
        runner.run(
            _dbt_args(tmp_path),
            cwd=tmp_path,
            timeout_seconds=1,
            redactions=(),
        )

    child_pid = int(child_pid_path.read_text(encoding="utf-8"))
    assert exc_info.value.code == "DPONE_DBT_EXECUTION_FAILED"
    assert "timed out" in str(exc_info.value)
    assert _wait_until_process_is_gone(child_pid)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group contract")
def test_descendant_retaining_output_pipe_fails_closed_without_hanging(
    tmp_path: Path,
) -> None:
    child_pid_path = tmp_path / "child.pid"
    parent_code = _parent_with_child_code(child_pid_path, parent_exits=True)
    runner = SubprocessDbtCommandRunner(
        popen_factory=_python_process(parent_code),
        process_supervisor=DbtProcessSupervisor(
            terminate_grace_seconds=0.05,
            kill_grace_seconds=0.5,
        ),
        collector_join_timeout_seconds=0.05,
    )

    started = time.monotonic()
    with pytest.raises(DbtPublishingError) as exc_info:
        runner.run(
            _dbt_args(tmp_path),
            cwd=tmp_path,
            timeout_seconds=5,
            redactions=(),
        )
    elapsed = time.monotonic() - started

    child_pid = int(child_pid_path.read_text(encoding="utf-8"))
    assert exc_info.value.code == "DPONE_DBT_EXECUTION_FAILED"
    assert "output could not be captured safely" in str(exc_info.value)
    assert _wait_until_process_is_gone(child_pid)
    assert elapsed < 2


def _python_process(code: str) -> Any:
    def popen(_args: tuple[str, ...], **kwargs: object) -> subprocess.Popen[bytes]:
        return subprocess.Popen((sys.executable, "-c", code), **cast(Any, kwargs))

    return popen


def _parent_with_child_code(child_pid_path: Path, *, parent_exits: bool) -> str:
    child_code = "import signal,time;signal.signal(signal.SIGTERM, signal.SIG_IGN);time.sleep(30)"
    parent_tail = "" if parent_exits else "time.sleep(30)"
    return (
        "import pathlib,subprocess,sys,time;"
        f"child=subprocess.Popen([sys.executable,'-c',{child_code!r}]);"
        f"pathlib.Path({str(child_pid_path)!r}).write_text(str(child.pid));"
        "print('child-started', flush=True);"
        f"{parent_tail}"
    )


def _wait_until_process_is_gone(pid: int) -> bool:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.01)
    return False
