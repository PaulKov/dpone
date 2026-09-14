"""Real local child-process tests for controlled client I/O, without ClickHouse."""

import hashlib
import shlex
import subprocess
import sys
from time import monotonic

import pytest

from dpone.runtime.clickhouse_file_stage_contract import ClickHouseFilePlan, IdentifiedStageQuery, QueryIdentity
from dpone.runtime.connectors.clickhouse_bulk import ClickHouseClientCredentials, ClickHouseClientOptions
from dpone.runtime.connectors.clickhouse_file_stage_client import ClickHouseFileClientRunner


@pytest.fixture
def runner_factory(tmp_path):
    def build(behavior="success", *, clock=monotonic, popen=subprocess.Popen):
        script = tmp_path / f"client_{behavior}.py"
        capture = tmp_path / f"{behavior}.bin"
        script.write_text(
            "import sys,time,json\n"
            + f"behavior={behavior!r}\ncapture={str(capture)!r}\n"
            + """
query=sys.argv[sys.argv.index('--query')+1]
if 'system.databases' in query:
    print(json.dumps(['11111111-1111-4111-8111-111111111111','sample','22222222-2222-4222-8222-222222222222','Atomic']))
elif query.startswith('INSERT'):
    data=sys.stdin.buffer.read()
    open(capture,'wb').write(data)
    if behavior=='oversized': sys.stdout.write('x'*65537)
    if behavior=='failure': sys.exit(3)
    if behavior=='hang': time.sleep(30)
"""
        )
        options = ClickHouseClientOptions(
            client_command=f"{shlex.quote(sys.executable)} {shlex.quote(str(script))}",
            input_format="RowBinary",
            timeout_seconds=2,
        )
        runner = ClickHouseFileClientRunner(
            ClickHouseClientCredentials("127.0.0.1", 9000, "sample", "synthetic"), options, clock=clock, popen=popen
        )
        plan = ClickHouseFilePlan((("v", "int"),), (("v", "Int32"),), "client", "none", 2, "a" * 64, "sample", "rows")
        endpoint = runner.preflight(plan)
        request = IdentifiedStageQuery(
            QueryIdentity("dpone-b02-" + "a" * 32 + "-insert", "insert", endpoint),
            "INSERT INTO sample.stage FORMAT RowBinary",
        )
        return runner, request, capture

    return build


def test_actual_child_receives_exact_bytes_and_terminal_ack(runner_factory):
    runner, request, capture = runner_factory()
    data = bytes(range(256)) * 1000
    result = runner.execute(request, chunks=[data[:1024], data[1024:]], deadline_monotonic=monotonic() + 3)
    assert capture.read_bytes() == data
    assert result.emitted_bytes == len(data)
    assert result.emitted_sha256 == hashlib.sha256(data).hexdigest()
    assert runner.local_stopped


@pytest.mark.parametrize("behavior", ["oversized", "failure"])
def test_complete_response_and_exit_are_checked(runner_factory, behavior):
    runner, request, _capture = runner_factory(behavior)
    with pytest.raises(RuntimeError):
        runner.execute(request, chunks=[b"data"], deadline_monotonic=monotonic() + 3)
    assert runner.local_stopped
    assert runner.cancel_and_observe(request.identity, deadline_monotonic=monotonic() + 3).remote_state == "unknown"


def test_timeout_joins_child_and_does_not_claim_remote_stop(runner_factory):
    runner, request, _capture = runner_factory("hang")
    with pytest.raises(TimeoutError):
        runner.execute(request, chunks=[b"data"], deadline_monotonic=monotonic() + 0.2)
    assert runner.local_stopped
    assert runner.cancel_and_observe(request.identity, deadline_monotonic=monotonic() + 3).remote_state == "unknown"


def test_base_exception_from_source_joins_child(runner_factory):
    runner, request, _capture = runner_factory()

    def failing_chunks():
        yield b"partial"
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        runner.execute(request, chunks=failing_chunks(), deadline_monotonic=monotonic() + 3)
    assert runner.local_stopped


@pytest.mark.parametrize("boundary", ["before_wait", "after_wait"])
def test_expired_final_wait_cannot_record_completed_query(runner_factory, boundary):
    """A real exited child does not grant success beyond the absolute deadline."""
    armed, expired = False, False
    process = None
    deadline = 0.0

    def clock():
        drained = process is not None and all(
            stream is not None and stream.closed for stream in (process.stdin, process.stdout, process.stderr)
        )
        if armed and (expired or (boundary == "before_wait" and drained)):
            return deadline
        return monotonic()

    class ObservedChild(subprocess.Popen):
        def wait(self, timeout=None):
            nonlocal expired
            result = super().wait(timeout=timeout)
            if armed and boundary == "after_wait":
                expired = True
            return result

    def popen(*args, **kwargs):
        nonlocal process
        process = ObservedChild(*args, **kwargs)
        return process

    runner, request, capture = runner_factory(clock=clock, popen=popen)
    process = None  # The completed preflight child is outside this query's deadline.
    deadline = monotonic() + 3
    armed = True
    with pytest.raises(TimeoutError, match="clickhouse file query deadline exceeded"):
        runner.execute(request, chunks=[b"complete"], deadline_monotonic=deadline)
    assert capture.read_bytes() == b"complete"
    assert runner.local_stopped
    assert process is not None and process.poll() == 0
    armed = False
    observation = runner.cancel_and_observe(request.identity, deadline_monotonic=monotonic() + 3)
    assert observation.local_state == "stopped"
    assert observation.remote_state == "unknown"
