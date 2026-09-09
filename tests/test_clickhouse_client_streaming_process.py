from __future__ import annotations

import pytest

from dpone.runtime.connectors.clickhouse_bulk import (
    ClickHouseClientCredentials,
    ClickHouseClientOptions,
    ClickHouseClientRunner,
)


def test_clickhouse_client_runner_reports_consumer_stderr_on_stream_broken_pipe() -> None:
    runner = ClickHouseClientRunner(
        credentials=ClickHouseClientCredentials(
            host="localhost",
            port=9000,
            database="default",
            user="default",
            password="super-secret",
        ),
        options=ClickHouseClientOptions(input_format="CustomSeparated", timeout_seconds=30),
        popen=_EarlyExitProcess,
    )

    with pytest.raises(RuntimeError) as exc_info:
        runner.insert_stream("landing.orders", ["id"], [b"row-1\n", b"row-2\n"])

    message = str(exc_info.value)
    assert "stream write failed" in message
    assert "Broken pipe" in message
    assert "Cannot parse input stream" in message
    assert "super-secret" not in message
    assert "--password ***" in message


class _BrokenPipeStdin:
    def __init__(self) -> None:
        self.writes = 0

    def write(self, chunk: bytes) -> None:
        del chunk
        self.writes += 1
        if self.writes > 1:
            raise BrokenPipeError(32, "Broken pipe")

    def close(self) -> None:
        pass


class _EarlyExitProcess:
    def __init__(self, command, **kwargs):
        del command
        assert kwargs["stdin"] == -1
        assert kwargs["stdout"] == -1
        assert kwargs["stderr"] == -1
        self.stdin = _BrokenPipeStdin()
        self.stdout = iter(())
        self.stderr = iter(("Code: 27. Cannot parse input stream\n",))
        self.returncode = 27

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        assert timeout == 30
        return self.returncode

    def terminate(self) -> None:
        self.returncode = -15
