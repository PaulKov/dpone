from __future__ import annotations

import errno
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import dpone.readiness.python_import_probe_runner as probe_runner
from dpone.readiness.python_import_health import (
    PythonImportHealth,
    probe_python_import,
)
from dpone.readiness.python_import_probe_runner import (
    ContainedProcessResult,
    ProbeProcessCleanupError,
    run_contained_import_process,
)

pytestmark = pytest.mark.usefixtures("replayable_doctor_import_surface")


@pytest.mark.parametrize("outcome", ("success", "timeout", "cancelled"))
@pytest.mark.skipif(os.name != "posix", reason="requires POSIX process groups")
def test_contained_runner_starts_a_session_and_quiesces_the_process_group(
    outcome: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    popen_kwargs: dict[str, object] = {}
    killed_groups: list[tuple[int, int]] = []

    class FakeProcess:
        pid = 4242
        returncode = 0

        def communicate(self, *, input: bytes, timeout: float) -> None:
            assert input == b"payload"
            if outcome == "timeout":
                raise subprocess.TimeoutExpired(("python",), timeout)
            if outcome == "cancelled":
                raise KeyboardInterrupt

        def wait(self, *, timeout: float) -> int:
            assert 0 < timeout <= 2.0
            return self.returncode

        def kill(self) -> None:
            self.returncode = -9

    def fake_popen(*_args: object, **kwargs: object) -> FakeProcess:
        popen_kwargs.update(kwargs)
        return FakeProcess()

    def fake_killpg(pid: int, sig: int) -> None:
        if sig == 0:
            raise ProcessLookupError
        killed_groups.append((pid, sig))

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.setattr(os, "killpg", fake_killpg)

    if outcome == "timeout":
        with pytest.raises(subprocess.TimeoutExpired):
            run_contained_import_process(
                ("python",),
                check=False,
                input=b"payload",
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=0.1,
                env={},
                cwd="/tmp",
            )
    elif outcome == "cancelled":
        with pytest.raises(KeyboardInterrupt):
            run_contained_import_process(
                ("python",),
                check=False,
                input=b"payload",
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=0.1,
                env={},
                cwd="/tmp",
            )
    else:
        result = run_contained_import_process(
            ("python",),
            check=False,
            input=b"payload",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=0.1,
            env={},
            cwd="/tmp",
        )
        assert result.returncode == 0

    assert popen_kwargs["start_new_session"] is True
    assert killed_groups == [(4242, signal.SIGKILL)]


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX process groups")
def test_posix_runner_accounts_only_process_communication_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = iter((10.0, 10.75, 20.0, 20.25))

    class FakeProcess:
        pid = 4245
        returncode = 0

        def communicate(self, *, input: bytes, timeout: float) -> None:
            assert input == b"payload"
            assert timeout == 5.0

        def wait(self, *, timeout: float) -> int:
            assert timeout == 1.75
            return self.returncode

    def kill_group(_pid: int, sig: int) -> None:
        if sig == 0:
            raise ProcessLookupError

    monkeypatch.setattr(probe_runner.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: FakeProcess())
    monkeypatch.setattr(os, "killpg", kill_group)

    result = run_contained_import_process(
        ("python",),
        check=False,
        input=b"payload",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=5.0,
        env={},
        cwd="/tmp",
    )

    assert type(result) is ContainedProcessResult
    assert result.communication_elapsed_seconds == 0.75


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX process groups")
def test_contained_runner_bounds_unconfirmed_process_group_quiescence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StuckProcess:
        pid = 4243
        returncode = 0

        def communicate(self, *, input: bytes, timeout: float) -> None:
            assert input == b"payload"
            assert timeout == 0.1

        def wait(self, *, timeout: float) -> int:
            raise subprocess.TimeoutExpired(("python",), timeout)

        def kill(self) -> None:
            raise AssertionError("group signal succeeded; fallback kill is not expected")

    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: StuckProcess())
    monkeypatch.setattr(os, "killpg", lambda _pid, _sig: None)

    with pytest.raises(ProbeProcessCleanupError) as failure:
        run_contained_import_process(
            ("python",),
            check=False,
            input=b"payload",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=0.1,
            env={},
            cwd="/tmp",
        )

    assert isinstance(failure.value.__cause__, subprocess.TimeoutExpired)
    assert 0 < failure.value.__cause__.timeout <= 2.0


@pytest.mark.parametrize("cleanup_failure", ("kill", "wait"))
@pytest.mark.skipif(os.name != "posix", reason="requires POSIX process groups")
def test_posix_cleanup_failure_wins_over_communication_timeout(
    cleanup_failure: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StuckProcess:
        pid = 4244
        returncode: int | None = None

        def communicate(self, *, input: bytes, timeout: float) -> None:
            raise subprocess.TimeoutExpired(("python",), timeout)

        def wait(self, *, timeout: float) -> int:
            if cleanup_failure == "wait":
                raise subprocess.TimeoutExpired(("python",), timeout)
            self.returncode = -9
            return self.returncode

        def kill(self) -> None:
            if cleanup_failure == "kill":
                raise OSError
            self.returncode = -9

    def kill_group(_pid: int, sig: int) -> None:
        if cleanup_failure == "kill" and sig == signal.SIGKILL:
            raise OSError
        if sig == 0:
            raise ProcessLookupError

    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: StuckProcess())
    monkeypatch.setattr(os, "killpg", kill_group)

    with pytest.raises(ProbeProcessCleanupError):
        run_contained_import_process(
            ("python",),
            check=False,
            input=b"payload",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=0.1,
            env={},
            cwd="/tmp",
        )


def test_import_probe_reports_unconfirmed_process_cleanup_as_unavailable() -> None:
    def unconfirmed_cleanup(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise ProbeProcessCleanupError

    result = probe_python_import("json", process_runner=unconfirmed_cleanup)

    assert result == PythonImportHealth(
        False,
        "python_import_probe_unavailable",
        "module import probe cleanup could not be confirmed",
    )


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="requires Linux child reaping semantics")
def test_import_probe_quiesces_real_inherited_helper_processes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "_dpone_inherited_helper_probe"
    pid_file = tmp_path / "helper-pids.txt"
    (tmp_path / f"{module_name}.py").write_text(
        "import subprocess, sys\n"
        "process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        f"with open({str(pid_file)!r}, 'a', encoding='ascii') as stream:\n"
        "    stream.write(str(process.pid) + '\\n')\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    result = probe_python_import(module_name)

    assert result.passed is True
    helper_pids = [int(value) for value in pid_file.read_text(encoding="ascii").splitlines()]
    assert len(helper_pids) == 2
    for pid in helper_pids:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


@pytest.mark.parametrize("outcome", ("success", "timeout", "cancelled"))
def test_windows_runner_bounds_the_direct_child(
    outcome: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    popen_kwargs: dict[str, object] = {}
    kills = 0
    waits: list[float] = []

    class FakeStdin:
        payload = b""

        def write(self, payload: bytes) -> int:
            self.payload += payload
            return len(payload)

        def close(self) -> None:
            return None

    class FakeProcess:
        returncode: int | None = None
        stdin = FakeStdin()

        def wait(self, *, timeout: float) -> int:
            waits.append(timeout)
            if len(waits) == 1 and outcome == "timeout":
                raise subprocess.TimeoutExpired(("python",), timeout)
            if len(waits) == 1 and outcome == "cancelled":
                raise KeyboardInterrupt
            if self.returncode is None:
                self.returncode = 0
            assert self.returncode is not None
            return self.returncode

        def kill(self) -> None:
            nonlocal kills
            kills += 1
            self.returncode = -9

    def fake_popen(*_args: object, **kwargs: object) -> FakeProcess:
        popen_kwargs.update(kwargs)
        return FakeProcess()

    monkeypatch.setattr(os, "name", "nt")
    monkeypatch.setattr(subprocess, "Popen", fake_popen)

    if outcome == "timeout":
        with pytest.raises(subprocess.TimeoutExpired):
            run_contained_import_process(
                ("python",),
                check=False,
                input=b"payload",
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=0.1,
                env={},
                cwd="C:\\probe",
            )
    elif outcome == "cancelled":
        with pytest.raises(KeyboardInterrupt):
            run_contained_import_process(
                ("python",),
                check=False,
                input=b"payload",
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=0.1,
                env={},
                cwd="C:\\probe",
            )
    else:
        result = run_contained_import_process(
            ("python",),
            check=False,
            input=b"payload",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=0.1,
            env={},
            cwd="C:\\probe",
        )
        assert result.returncode == 0

    assert "start_new_session" not in popen_kwargs
    assert popen_kwargs["stdin"] == subprocess.PIPE
    assert FakeProcess.stdin.payload == b"payload"
    assert kills == (0 if outcome == "success" else 1)
    assert len(waits) == (1 if outcome == "success" else 2)
    assert 0 < waits[0] <= 0.1
    if outcome != "success":
        assert 0 < waits[1] <= 2.0


def test_windows_runner_bounds_input_backpressure_and_joins_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    released = threading.Event()
    writer_finished = threading.Event()

    class BlockingStdin:
        def write(self, payload: bytes) -> int:
            assert payload == b"payload"
            released.wait(timeout=5.0)
            writer_finished.set()
            return len(payload)

        def close(self) -> None:
            return None

    class BlockedProcess:
        returncode: int | None = None
        stdin = BlockingStdin()
        waits = 0

        def wait(self, *, timeout: float) -> int:
            self.waits += 1
            if self.waits == 1:
                raise subprocess.TimeoutExpired(("python",), timeout)
            assert self.returncode is not None
            return self.returncode

        def kill(self) -> None:
            self.returncode = -9
            released.set()

    monkeypatch.setattr(os, "name", "nt")
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: BlockedProcess())

    with pytest.raises(subprocess.TimeoutExpired):
        run_contained_import_process(
            ("python",),
            check=False,
            input=b"payload",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=0.1,
            env={},
            cwd="C:\\probe",
        )

    assert writer_finished.is_set()
    assert not any(
        thread.name == "dpone-python-import-probe-stdin" and thread.is_alive() for thread in threading.enumerate()
    )


@pytest.mark.parametrize("error", (BrokenPipeError(), OSError(errno.EINVAL, "closed")))
def test_windows_runner_suppresses_only_benign_input_pipe_errors(
    error: OSError,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ClosedStdin:
        def write(self, _payload: bytes) -> int:
            raise error

        def close(self) -> None:
            return None

    class CompletedProcess:
        returncode: int | None = None
        stdin = ClosedStdin()

        def wait(self, *, timeout: float) -> int:
            assert 0 < timeout <= 0.1
            self.returncode = 0
            return self.returncode

    monkeypatch.setattr(os, "name", "nt")
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: CompletedProcess())

    result = run_contained_import_process(
        ("python",),
        check=False,
        input=b"payload",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=0.1,
        env={},
        cwd="C:\\probe",
    )

    assert result.returncode == 0


def test_windows_runner_fails_closed_on_other_input_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenStdin:
        def write(self, _payload: bytes) -> int:
            raise OSError(errno.EIO, "write failed")

        def close(self) -> None:
            return None

    class CompletedProcess:
        returncode: int | None = None
        stdin = BrokenStdin()

        def wait(self, *, timeout: float) -> int:
            assert 0 < timeout <= 0.1
            self.returncode = 0
            return self.returncode

    monkeypatch.setattr(os, "name", "nt")
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: CompletedProcess())

    with pytest.raises(OSError, match="write failed"):
        run_contained_import_process(
            ("python",),
            check=False,
            input=b"payload",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=0.1,
            env={},
            cwd="C:\\probe",
        )


@pytest.mark.parametrize("cleanup_failure", ("kill", "wait"))
def test_windows_cleanup_failure_wins_over_communication_timeout(
    cleanup_failure: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeStdin:
        def write(self, payload: bytes) -> int:
            assert payload == b"payload"
            return len(payload)

        def close(self) -> None:
            return None

    class StuckProcess:
        returncode: int | None = None
        stdin = FakeStdin()
        waits = 0

        def wait(self, *, timeout: float) -> int:
            self.waits += 1
            if self.waits == 1 or cleanup_failure == "wait":
                raise subprocess.TimeoutExpired(("python",), timeout)
            self.returncode = -9
            return self.returncode

        def kill(self) -> None:
            if cleanup_failure == "kill":
                raise OSError
            self.returncode = -9

    monkeypatch.setattr(os, "name", "nt")
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: StuckProcess())

    with pytest.raises(ProbeProcessCleanupError):
        run_contained_import_process(
            ("python",),
            check=False,
            input=b"payload",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=0.1,
            env={},
            cwd="C:\\probe",
        )


@pytest.mark.skipif(os.name != "nt", reason="requires Windows")
def test_windows_default_probe_executes_a_real_import() -> None:
    result = probe_python_import("json")

    assert result == PythonImportHealth(True, None, "module import succeeded")


@pytest.mark.skipif(os.name != "nt", reason="requires Windows")
def test_windows_default_probe_bounds_a_hanging_direct_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module_name = "_dpone_windows_hanging_import_probe"
    (tmp_path / f"{module_name}.py").write_text(
        "import time\ntime.sleep(60)\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    result = probe_python_import(module_name)

    assert result == PythonImportHealth(
        False,
        "python_import_timed_out",
        "module import exceeded the health-check deadline",
    )


@pytest.mark.skipif(os.name != "nt", reason="requires Windows anonymous-pipe backpressure")
def test_windows_runner_bounds_a_child_that_never_reads_stdin() -> None:
    started = time.monotonic()

    with pytest.raises(subprocess.TimeoutExpired):
        run_contained_import_process(
            (sys.executable, "-c", "import time; time.sleep(10)"),
            check=False,
            input=b"x" * (132 * 1024 + 128),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=0.2,
            env=dict(os.environ),
            cwd=os.getcwd(),
        )

    assert time.monotonic() - started < 5.0
    assert not any(
        thread.name == "dpone-python-import-probe-stdin" and thread.is_alive() for thread in threading.enumerate()
    )
