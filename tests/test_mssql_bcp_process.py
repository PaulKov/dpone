from __future__ import annotations

import io
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import dpone.runtime.connectors.mssql_bulk as mssql_bulk_module
from dpone.runtime.connectors.mssql_bulk import (
    BcpCredentials,
    BcpDsnOptions,
    BcpOptions,
    BcpProcess,
    BcpRunner,
    BcpTimeoutError,
)
from dpone.runtime.process_io import (
    ProcessAbortError,
    ProcessOutputDrainer,
    ProcessOutputDrainTimeout,
    add_exception_note,
    iter_fifo_bytes,
)


def test_add_exception_note_uses_runtime_capability() -> None:
    error = RuntimeError("transfer failed")

    add_exception_note(error, "cleanup failed")

    assert error.__notes__ == ["cleanup failed"]


def test_bcp_runner_process_writes_stdin_password_before_wait(monkeypatch) -> None:
    class FakeStdin:
        def __init__(self) -> None:
            self.writes: list[str] = []
            self.closed = False

        def write(self, value: str) -> None:
            self.writes.append(value)

        def flush(self) -> None:
            return None

        def close(self) -> None:
            self.closed = True

    class FakeProcess:
        def __init__(self) -> None:
            self.stdin_obj = FakeStdin()
            self.stdin = self.stdin_obj
            self.stdout = io.StringIO("12 rows copied.\n")
            self.stderr = io.StringIO("")
            self.returncode = 0
            self.communicate_input = None
            self.communicate_timeout = None

        def communicate(self, input=None, timeout=None):
            self.communicate_input = input
            self.communicate_timeout = timeout
            return "12 rows copied.\n", ""

        def wait(self, timeout=None):
            self.communicate_timeout = timeout
            return self.returncode

        def poll(self):
            return self.returncode

    fake_process = FakeProcess()

    def fake_popen(command, **kwargs):
        del command, kwargs
        return fake_process

    monkeypatch.setattr(mssql_bulk_module.subprocess, "Popen", fake_popen)
    runner = BcpRunner(
        BcpCredentials(host="sql.example.com", port=1433, database="dwh", user="etl", password="secret"),
        BcpOptions(password_transport="stdin", timeout_seconds=30),
    )

    process = runner.queryout_process("SELECT 1", "/tmp/out.bcp")
    result = process.wait()

    assert fake_process.stdin_obj.writes == ["secret\n"]
    assert fake_process.stdin_obj.closed is True
    assert fake_process.stdin is None
    assert fake_process.communicate_input is None
    assert fake_process.communicate_timeout == 30
    assert result.rows_copied == 12


def test_bcp_runner_queryout_process_drains_output_without_communicate(monkeypatch) -> None:
    progress: list[str] = []

    class FakeProcess:
        def __init__(self) -> None:
            self.stdin = None
            self.stdout = io.StringIO("1000 rows copied.\n2000 rows copied.\n")
            self.stderr = io.StringIO("")
            self.returncode = 0
            self.wait_timeout = object()

        def wait(self, timeout=None):
            self.wait_timeout = timeout
            return self.returncode

        def communicate(self, input=None, timeout=None):
            del input, timeout
            raise AssertionError("queryout_process output must be drained asynchronously")

        def poll(self):
            return self.returncode

    fake_process = FakeProcess()

    def fake_popen(command, **kwargs):
        del command
        assert kwargs["stdout"] == mssql_bulk_module.subprocess.PIPE
        assert kwargs["stderr"] == mssql_bulk_module.subprocess.PIPE
        assert kwargs["text"] is True
        return fake_process

    monkeypatch.setattr(mssql_bulk_module.subprocess, "Popen", fake_popen)
    runner = BcpRunner(
        BcpCredentials(host="sql.example.com", port=1433, database="dwh", trusted_connection=True),
        BcpOptions(timeout_seconds=7),
    )

    process = runner.queryout_process("SELECT 1", "/tmp/out.bcp", progress_callback=progress.append)
    result = process.wait()

    assert fake_process.wait_timeout == 7
    assert result.stdout == "1000 rows copied.\n2000 rows copied.\n"
    assert result.stderr == ""
    assert result.rows_copied == 2000
    assert progress == ["1000 rows copied.", "2000 rows copied."]


def test_bcp_runner_import_uses_bounded_communicate_without_reader_threads(monkeypatch) -> None:
    class FakeProcess:
        def __init__(self) -> None:
            self.stdin = None
            self.stdout = io.StringIO("4 rows copied.\n")
            self.stderr = io.StringIO("")
            self.returncode = 0
            self.communicate_input = object()
            self.communicate_timeout = None

        def communicate(self, input=None, timeout=None):
            self.communicate_input = input
            self.communicate_timeout = timeout
            return "4 rows copied.\n", ""

        def poll(self):
            return self.returncode

    process = FakeProcess()

    class ForbiddenDrainer:
        def __init__(self, *_args, **_kwargs) -> None:
            raise AssertionError("synchronous BCP imports must not create reader threads")

    def fake_popen(command, **kwargs):
        assert command[2:4] == ["in", "/tmp/payload.bin"]
        assert kwargs["stdout"] == mssql_bulk_module.subprocess.PIPE
        assert kwargs["stderr"] == mssql_bulk_module.subprocess.PIPE
        return process

    monkeypatch.setattr(mssql_bulk_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        "dpone.runtime.connectors.mssql_bcp_process.ProcessOutputDrainer",
        ForbiddenDrainer,
    )
    runner = BcpRunner(
        BcpCredentials(host="sql.example.com", port=1433, database="dwh", trusted_connection=True),
        BcpOptions(timeout_seconds=17),
    )

    result = runner.import_format_file("[dwh].[staging].[delta]", "/tmp/payload.bin", "/tmp/payload.fmt")

    assert process.communicate_input is None
    assert process.communicate_timeout == 17
    assert result.rows_copied == 4


@pytest.mark.skipif(os.name == "nt", reason="POSIX passes a held descriptor to BCP")
def test_synchronous_bcp_import_inherits_and_redacts_pinned_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "payload.bin"
    source.write_bytes(b"payload")
    descriptor = os.open(source, os.O_RDONLY)
    private_path = f"/proc/self/fd/{descriptor}"
    observed: dict[str, object] = {}

    class Authority:
        @staticmethod
        def prepare_process_input(source_path: str) -> tuple[str, tuple[int, ...]]:
            assert source_path == str(source)
            return private_path, (descriptor,)

    class FakeProcess:
        stdin = None
        returncode = 0

        @staticmethod
        def communicate(input=None, timeout=None):
            del input, timeout
            return f"read {private_path}\n4 rows copied.\n", ""

        @staticmethod
        def poll():
            return 0

    def fake_popen(command, **kwargs):  # noqa: ANN001
        observed.update(command=tuple(command), pass_fds=kwargs.get("pass_fds"))
        return FakeProcess()

    monkeypatch.setattr(mssql_bulk_module.subprocess, "Popen", fake_popen)
    runner = BcpRunner(
        BcpCredentials(host="sql.example.com", port=1433, database="dwh", trusted_connection=True),
        BcpOptions(timeout_seconds=17, input_file_authority=Authority()),
    )

    try:
        result = runner.import_file("[dwh].[staging].[delta]", str(source))
    finally:
        os.close(descriptor)

    assert observed == {
        "command": result.command,
        "pass_fds": (descriptor,),
    }
    assert result.redacted_command[3] == "<pinned-input>"
    assert private_path not in result.stdout
    assert "<pinned-input>" in result.stdout
    assert result.rows_copied == 4


def test_bcp_runner_synchronous_password_is_owned_by_communicate(monkeypatch) -> None:
    secret = "never-log-this-password"

    class FakeProcess:
        def __init__(self) -> None:
            self.stdin = SimpleNamespace()
            self.stdout = SimpleNamespace()
            self.stderr = SimpleNamespace()
            self.returncode = 0
            self.communicate_input = None
            self.communicate_timeout = None

        def communicate(self, input=None, timeout=None):
            self.communicate_input = input
            self.communicate_timeout = timeout
            return "1 row copied.\n", ""

        def poll(self):
            return self.returncode

    process = FakeProcess()
    monkeypatch.setattr(mssql_bulk_module.subprocess, "Popen", lambda *_args, **_kwargs: process)
    runner = BcpRunner(
        BcpCredentials(host="sql.example.com", port=1433, database="dwh", user="etl", password=secret),
        BcpOptions(password_transport="stdin", timeout_seconds=19),
    )

    result = runner.import_file("[dwh].[staging].[delta]", "/tmp/payload.bin")

    assert process.communicate_input == f"{secret}\n"
    assert process.communicate_timeout == 19
    assert result.rows_copied == 1


def test_bcp_runner_synchronous_timeout_reaps_drains_and_releases_once(monkeypatch) -> None:
    events: list[object] = []

    class TimedOutProcess:
        def __init__(self) -> None:
            self.stdin = SimpleNamespace()
            self.stdout = SimpleNamespace()
            self.stderr = SimpleNamespace()
            self.returncode = None
            self.communication_started = False

        def communicate(self, input=None, timeout=None):
            events.append(("communicate", input, timeout))
            if not self.communication_started:
                self.communication_started = True
                raise subprocess.TimeoutExpired(
                    ("bcp", "-P", "raw-secret"),
                    timeout,
                    output="raw-secret",
                    stderr="raw-secret",
                )
            assert self.returncode is not None
            return "partial output", ""

        def poll(self):
            return self.returncode

        def terminate(self) -> None:
            events.append("terminate")

        def wait(self, timeout=None):
            events.append(("wait", timeout))
            self.returncode = -15
            return self.returncode

        def kill(self) -> None:
            events.append("kill")
            self.returncode = -9

    process = TimedOutProcess()
    lease = SimpleNamespace(environment={}, close=lambda: events.append("lease_close"))
    monkeypatch.setattr(mssql_bulk_module.subprocess, "Popen", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(BcpRunner, "_issue_connection_dsn", lambda _self: lease)
    runner = BcpRunner(
        BcpCredentials(host="sql.example.com", port=1433, database="dwh", trusted_connection=True),
        BcpOptions(timeout_seconds=23),
    )

    with pytest.raises(BcpTimeoutError) as caught:
        runner.import_file("[dwh].[staging].[delta]", "/tmp/payload.bin")

    assert caught.value.cmd[0:4] == (
        "bcp",
        "[dwh].[staging].[delta]",
        "in",
        "/tmp/payload.bin",
    )
    assert caught.value.cmd[caught.value.cmd.index("-l") + 1] == "23"
    assert "raw-secret" not in repr(caught.value)
    assert caught.value.output is None
    assert caught.value.stderr is None
    assert caught.value.__context__ is None
    assert events == [
        ("communicate", None, 23),
        "terminate",
        ("wait", 5.0),
        ("communicate", None, 5.0),
        "lease_close",
    ]


def test_bcp_runner_normalizes_explicit_none_to_finite_deadline() -> None:
    runner = BcpRunner(
        BcpCredentials(host="sql.example.com", port=1433, database="dwh", trusted_connection=True),
        BcpOptions(timeout_seconds=None),
        run=lambda *_args, **_kwargs: pytest.fail("construction must not execute BCP"),
    )

    assert runner.options.timeout_seconds == 3600


@pytest.mark.parametrize("value", [0, -1, True, False])
def test_bcp_runner_rejects_invalid_explicit_deadline(value: object) -> None:
    with pytest.raises(ValueError, match="MSSQL BCP timeout_seconds must be a positive integer"):
        BcpRunner(
            BcpCredentials(host="sql.example.com", port=1433, database="dwh", trusted_connection=True),
            BcpOptions(timeout_seconds=value),  # type: ignore[arg-type]
        )


def test_injected_bcp_timeout_exposes_only_redacted_command() -> None:
    secret = "never-log-this-password"

    def timeout(command, **_kwargs):
        raise subprocess.TimeoutExpired(command, 11, output=secret, stderr=secret)

    runner = BcpRunner(
        BcpCredentials(host="sql.example.com", port=1433, database="dwh", user="etl", password=secret),
        BcpOptions(password_transport="argument", timeout_seconds=11),
        run=timeout,
    )

    with pytest.raises(BcpTimeoutError) as caught:
        runner.import_file("[dwh].[staging].[delta]", "/tmp/payload.bin")

    error = caught.value
    rendered = " ".join((str(error), repr(error), repr(error.cmd), repr(getattr(error, "__notes__", ()))))
    assert secret not in rendered
    assert error.cmd[error.cmd.index("-P") + 1] == "***"
    assert error.output is None
    assert error.stderr is None
    assert error.__context__ is None


def test_supervised_bcp_timeout_redacts_raw_error_and_cleanup_failure() -> None:
    secret = "never-log-this-password"

    def fail_resource_cleanup() -> None:
        raise OSError("reviewed resource cleanup failure")

    class TimedOutProcess:
        returncode = None

        def wait(self, timeout=None):
            raise subprocess.TimeoutExpired(("bcp", "-P", secret), timeout)

        @staticmethod
        def poll():
            return None

        @staticmethod
        def terminate() -> None:
            raise OSError("reviewed terminate failure")

    bcp = BcpProcess(
        process=TimedOutProcess(),  # type: ignore[arg-type]
        command=("bcp", "-P", secret),
        redacted_command=("bcp", "-P", "***"),
        timeout_seconds=0.25,
        output_drainer=SimpleNamespace(join=lambda timeout=None: None),  # type: ignore[arg-type]
        cleanup_callback=fail_resource_cleanup,
    )

    with pytest.raises(BcpTimeoutError) as caught:
        bcp.wait()

    error = caught.value
    rendered = " ".join((str(error), repr(error), repr(error.cmd), repr(error.__notes__)))
    assert secret not in rendered
    assert error.cmd == ("bcp", "-P", "***")
    assert error.__notes__ == [
        "bcp timeout cleanup failed: OSError",
        "bcp resource cleanup failed: OSError",
    ]


def test_bcp_process_timeout_terminates_and_reaps_before_raising() -> None:
    class TimedOutProcess:
        def __init__(self) -> None:
            self.returncode = None
            self.events: list[object] = []

        def wait(self, timeout=None):
            self.events.append(("wait", timeout))
            if self.returncode is None and "terminate" not in self.events:
                raise subprocess.TimeoutExpired("bcp", timeout)
            return self.returncode

        def poll(self):
            return self.returncode

        def terminate(self) -> None:
            self.events.append("terminate")
            self.returncode = -15

        def kill(self) -> None:
            self.events.append("kill")
            self.returncode = -9

    class FakeDrainer:
        def __init__(self) -> None:
            self.timeouts: list[float | None] = []

        def join(self, timeout=None):
            self.timeouts.append(timeout)
            return SimpleNamespace(stdout="", stderr="")

    process = TimedOutProcess()
    drainer = FakeDrainer()
    cleaned: list[bool] = []
    bcp = BcpProcess(
        process=process,  # type: ignore[arg-type]
        command=("bcp",),
        redacted_command=("bcp",),
        timeout_seconds=0.25,
        output_drainer=drainer,  # type: ignore[arg-type]
        cleanup_callback=lambda: cleaned.append(True),
    )

    with pytest.raises(subprocess.TimeoutExpired):
        bcp.wait()

    assert process.events == [("wait", 0.25), "terminate", ("wait", 5.0)]
    assert drainer.timeouts == [5.0]
    assert cleaned == [True]


def test_bcp_process_terminal_output_drain_is_bounded_and_retried_once() -> None:
    class TerminalProcess:
        returncode = 0

        @staticmethod
        def wait(timeout=None):
            return 0

        @staticmethod
        def poll():
            return 0

    class StuckDrainer:
        def __init__(self) -> None:
            self.timeouts: list[float | None] = []

        def join(self, timeout=None):
            self.timeouts.append(timeout)
            raise ProcessOutputDrainTimeout(timeout_seconds=timeout, active_readers=1)

    drainer = StuckDrainer()
    cleaned: list[bool] = []
    bcp = BcpProcess(
        process=TerminalProcess(),  # type: ignore[arg-type]
        command=("bcp",),
        redacted_command=("bcp",),
        output_drainer=drainer,  # type: ignore[arg-type]
        cleanup_callback=lambda: cleaned.append(True),
    )

    with pytest.raises(ProcessOutputDrainTimeout) as caught:
        bcp.wait()

    assert caught.value.timeout_seconds == 5.0
    assert caught.value.__notes__ == ["bcp wait cleanup failed: ProcessOutputDrainTimeout"]
    assert drainer.timeouts == [5.0, 5.0]
    assert cleaned == [True]


def test_bcp_runner_process_retains_dsn_until_wait_then_cleans_it(monkeypatch) -> None:
    observed: dict[str, object] = {}

    class FakeProcess:
        stdin = None
        stdout = io.StringIO("1 row copied.\n")
        stderr = io.StringIO("")
        returncode = 0

        def wait(self, timeout=None):
            del timeout
            return self.returncode

        def poll(self):
            return self.returncode

    def fake_popen(command, **kwargs):
        dsn_path = Path(kwargs["env"]["ODBCINI"])
        observed.update(command=list(command), dsn_path=dsn_path)
        assert dsn_path.is_file()
        return FakeProcess()

    monkeypatch.setattr(mssql_bulk_module.subprocess, "Popen", fake_popen)
    runner = BcpRunner(
        BcpCredentials(
            host="bi-listener.example.com",
            port=1433,
            database="dwh",
            trusted_connection=True,
        ),
        BcpOptions(connection_dsn=BcpDsnOptions()),
    )

    process = runner.queryout_process("SELECT 1", "/tmp/out.bcp")
    dsn_path = Path(observed["dsn_path"])
    assert dsn_path.is_file()

    result = process.wait()

    assert result.rows_copied == 1
    assert "-D" in observed["command"]
    assert not dsn_path.exists()


def test_bcp_runner_process_cleans_dsn_when_process_creation_fails(monkeypatch) -> None:
    observed: dict[str, Path] = {}

    def failing_popen(_command, **kwargs):
        dsn_path = Path(kwargs["env"]["ODBCINI"])
        observed["dsn_path"] = dsn_path
        assert dsn_path.is_file()
        raise OSError("reviewed process creation failure")

    monkeypatch.setattr(mssql_bulk_module.subprocess, "Popen", failing_popen)
    runner = BcpRunner(
        BcpCredentials(
            host="bi-listener.example.com",
            port=1433,
            database="dwh",
            trusted_connection=True,
        ),
        BcpOptions(connection_dsn=BcpDsnOptions()),
    )

    with pytest.raises(OSError, match="reviewed process creation failure"):
        runner.queryout_process("SELECT 1", "/tmp/out.bcp")

    assert not observed["dsn_path"].exists()


def test_bcp_process_abort_reaps_and_kills_after_bounded_terminate_timeout() -> None:
    class FakeProcess:
        def __init__(self) -> None:
            self.returncode = None
            self.events: list[object] = []

        def poll(self):
            return self.returncode

        def terminate(self) -> None:
            self.events.append("terminate")

        def wait(self, timeout=None):
            self.events.append(("wait", timeout))
            if self.returncode is None and "kill" not in self.events:
                raise subprocess.TimeoutExpired("bcp", timeout)
            self.returncode = -9
            return self.returncode

        def kill(self) -> None:
            self.events.append("kill")
            self.returncode = -9

    class FakeDrainer:
        def __init__(self) -> None:
            self.timeouts: list[float | None] = []

        def join(self, timeout=None):
            self.timeouts.append(timeout)
            return SimpleNamespace(stdout="", stderr="")

    process = FakeProcess()
    drainer = FakeDrainer()
    bcp = BcpProcess(
        process=process,  # type: ignore[arg-type]
        command=("bcp",),
        redacted_command=("bcp",),
        output_drainer=drainer,  # type: ignore[arg-type]
    )

    bcp.abort(timeout_seconds=0.25)

    assert process.events == ["terminate", ("wait", 0.25), "kill", ("wait", 0.25)]
    assert drainer.timeouts == [0.25]


def test_bcp_process_abort_joins_drainer_and_reports_unreaped_process() -> None:
    class UnreapedProcess:
        def __init__(self) -> None:
            self.events: list[object] = []

        @staticmethod
        def poll():
            return None

        def terminate(self) -> None:
            self.events.append("terminate")

        def wait(self, timeout=None):
            self.events.append(("wait", timeout))
            raise subprocess.TimeoutExpired("bcp", timeout)

        def kill(self) -> None:
            self.events.append("kill")

    class FakeDrainer:
        def __init__(self) -> None:
            self.timeouts: list[float | None] = []

        def join(self, timeout=None):
            self.timeouts.append(timeout)
            return SimpleNamespace(stdout="", stderr="")

    process = UnreapedProcess()
    drainer = FakeDrainer()
    bcp = BcpProcess(
        process=process,  # type: ignore[arg-type]
        command=("bcp",),
        redacted_command=("bcp",),
        output_drainer=drainer,  # type: ignore[arg-type]
    )

    with pytest.raises(ProcessAbortError, match="process_abort_unreaped"):
        bcp.abort(timeout_seconds=0.25)

    assert process.events == ["terminate", ("wait", 0.25), "kill", ("wait", 0.25)]
    assert drainer.timeouts == [0.25]


def test_process_output_drainer_rejects_reader_threads_alive_after_timeout() -> None:
    class StuckReader:
        def __init__(self) -> None:
            self.timeouts: list[float | None] = []

        def join(self, timeout=None) -> None:
            self.timeouts.append(timeout)

        @staticmethod
        def is_alive() -> bool:
            return True

    reader = StuckReader()
    drainer = ProcessOutputDrainer.__new__(ProcessOutputDrainer)
    drainer._stdout = []
    drainer._stderr = []
    drainer._threads = [reader]  # type: ignore[list-item]

    with pytest.raises(ProcessOutputDrainTimeout, match="process_output_drain_timeout") as caught:
        drainer.join(timeout=0.25)

    assert caught.value.active_readers == 1
    assert caught.value.timeout_seconds == 0.25
    assert reader.timeouts == [0.25]


def test_fifo_reader_finishes_when_bcp_exits_before_opening_writer(tmp_path: Path) -> None:
    class ExitedProcess:
        @staticmethod
        def poll():
            return 1

    fifo_path = tmp_path / "early-exit.fifo"
    os.mkfifo(fifo_path)

    assert list(iter_fifo_bytes(fifo_path, ExitedProcess(), read_buffer_bytes=64 * 1024)) == []


def test_fifo_reader_drains_bytes_written_between_empty_read_and_process_exit(tmp_path: Path) -> None:
    fifo_path = tmp_path / "exit-race.fifo"
    os.mkfifo(fifo_path)

    class ExitAfterWrite:
        written = False

        def poll(self):
            if not self.written:
                descriptor = os.open(fifo_path, os.O_WRONLY | os.O_NONBLOCK)
                try:
                    os.write(descriptor, b"final-bytes")
                finally:
                    os.close(descriptor)
                self.written = True
            return 0

    assert b"".join(iter_fifo_bytes(fifo_path, ExitAfterWrite(), read_buffer_bytes=64 * 1024)) == b"final-bytes"
