from __future__ import annotations

import traceback
from pathlib import Path
from threading import Thread
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.byte_stream_artifacts import ByteStreamArtifact
from dpone.runtime.connectors.mssql_bulk import BcpResult
from dpone.runtime.decision_audit import RuntimeDecisionContext
from dpone.runtime.process_io import ProcessCleanupError
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory


def test_mssql_queryout_required_streaming_returns_lazy_fifo_byte_stream(tmp_path: Path) -> None:
    connector = _FakeMssqlConnector(payload=b"1\x1fA\x1e\n2\x1fB\x1e\n")
    artifact = _build_artifact(connector, tmp_path, read_buffer_bytes="4MiB")

    assert isinstance(artifact, ByteStreamArtifact)
    assert artifact.format == "mssql-delimited"
    assert getattr(artifact, "source_export_provider") == "mssql_bcp_pipe"
    assert getattr(artifact, "bulk_wire_contract").selected_route == "typed_raw_direct"

    assert b"".join(artifact.iter_bytes()) == b"1\x1fA\x1e\n2\x1fB\x1e\n"
    assert artifact.rows_exported == 2
    assert connector.calls == [("SELECT [id], [name] FROM [reporting].[sales]", "fifo")]
    assert not list(tmp_path.glob("*.fifo"))


def test_mssql_queryout_streaming_uses_configured_fifo_read_buffer(tmp_path: Path) -> None:
    payload = b"x" * ((64 * 1024 * 3) + 17)
    connector = _FakeMssqlConnector(payload=payload)
    artifact = _build_artifact(connector, tmp_path, read_buffer_bytes="64KiB")

    frames = list(artifact.iter_bytes())

    assert b"".join(frames) == payload
    assert len(frames) >= 4
    assert max(map(len, frames)) <= 64 * 1024
    assert not list(tmp_path.glob("*.fifo"))


def test_mssql_queryout_streaming_default_frames_do_not_exceed_four_mib(tmp_path: Path) -> None:
    payload = b"x" * ((4 * 1024 * 1024) + 17)
    connector = _FakeMssqlConnector(payload=payload)
    artifact = _build_artifact(connector, tmp_path, read_buffer_bytes="4MiB")

    frames = list(artifact.iter_bytes())

    assert b"".join(frames) == payload
    assert len(frames) >= 2
    assert max(map(len, frames)) <= 4 * 1024 * 1024


def test_mssql_queryout_streaming_publishes_deprecated_alias_warning_once(tmp_path: Path) -> None:
    class Collector:
        def __init__(self) -> None:
            self.decisions = []

        def publish(self, decision) -> None:
            self.decisions.append(decision)

    collector = Collector()
    connector = _FakeMssqlConnector(payload=b"1\x1fA\x1e\n")
    with RuntimeDecisionContext.activate(collector):
        artifact = _build_artifact(
            connector,
            tmp_path,
            read_buffer_bytes=None,
            legacy_target_chunk_bytes="512MiB",
        )
        assert b"".join(artifact.iter_bytes()) == b"1\x1fA\x1e\n"

    route_decisions = [decision for decision in collector.decisions if decision.decision_id == "mssql.bcp_pipe.route"]
    assert len(route_decisions) == 1
    assert route_decisions[0].warnings == ("streaming_target_chunk_bytes_deprecated_use_read_buffer_bytes",)


def test_mssql_pipe_stream_cancellation_reports_redacted_abort_failure(tmp_path: Path) -> None:
    connector = _FakeMssqlConnector(payload=b"first\nsecond\n", abort_error=RuntimeError("SECRET-ABORT"))
    artifact = _build_artifact(connector, tmp_path, read_buffer_bytes="64KiB")
    stream = artifact.iter_bytes()

    assert next(stream)
    with pytest.raises(ProcessCleanupError, match="process_cleanup_failed") as caught:
        stream.close()

    assert caught.value.operation == "bcp_abort"
    assert caught.value.cause_type == "RuntimeError"
    assert "SECRET-ABORT" not in str(caught.value)
    rendered = "".join(traceback.format_exception(caught.type, caught.value, caught.tb))
    assert "SECRET-ABORT" not in rendered
    assert caught.value.__cause__ is None
    assert not isinstance(caught.value.__context__, RuntimeError)
    assert not list(tmp_path.glob("*.fifo"))


def test_byte_stream_cleanup_runs_when_completed_iterator_close_fails() -> None:
    cleanup_calls: list[str] = []

    class FailingCloseIterator:
        def __init__(self) -> None:
            self._remaining = [b"payload"]

        def __iter__(self):
            return self

        def __next__(self) -> bytes:
            if self._remaining:
                return self._remaining.pop()
            raise StopIteration

        @staticmethod
        def close() -> None:
            raise ProcessCleanupError(operation="iterator_close", cause_type="RuntimeError")

    artifact = ByteStreamArtifact(
        FailingCloseIterator,
        columns=("value",),
        format="mssql-delimited",
        cleanup_callback=lambda: cleanup_calls.append("cleanup"),
    )

    with pytest.raises(ProcessCleanupError, match="process_cleanup_failed"):
        list(artifact.iter_bytes())

    assert cleanup_calls == ["cleanup"]


def _build_artifact(
    connector: _FakeMssqlConnector,
    tmp_path: Path,
    *,
    read_buffer_bytes: str | None,
    legacy_target_chunk_bytes: str | None = None,
) -> ByteStreamArtifact:
    streaming = {
        "mode": "required",
        "provider": "bcp_pipe",
        "pipe_mode": "fifo",
        "delimiter_safety": "advisory",
    }
    if read_buffer_bytes is not None:
        streaming["read_buffer_bytes"] = read_buffer_bytes
    if legacy_target_chunk_bytes is not None:
        streaming["target_chunk_bytes"] = legacy_target_chunk_bytes
    config = LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema="reporting",
        source_table="sales",
        target_schema="DWH_Raw",
        target_table="sales",
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=100_000,
        options={
            "runtime_storage": {"work_dir": str(tmp_path)},
            "native_transfer": {
                "wire": {"mode": "typed_raw", "delimiter_profile": "ascii_control"},
                "snapshot": {"streaming": streaming},
            },
            "clickhouse_bulk": {
                "mode": "client",
                "ingest_contract": "typed_raw_streaming_staging",
                "streaming": {
                    "format": "CustomSeparated",
                    "async_insert": True,
                    "wait_for_async_insert": True,
                },
            },
        },
    )

    return MSSQLQueryoutArtifactFactory(
        connector,
        SimpleNamespace(log_etl_progress=lambda *_args, **_kwargs: None),
        sink_connector=ClickHouseConnector(),
    ).artifact_for_query(
        config,
        "SELECT [id], [name] FROM [reporting].[sales]",
        [("id", "int"), ("name", "varchar(16)")],
    )


class _FakeMssqlConnector:
    bcp_path = "bcp"
    trust_server_certificate = "yes"

    def __init__(self, payload: bytes, abort_error: Exception | None = None) -> None:
        self.payload = payload
        self.abort_error = abort_error
        self.calls: list[tuple[str, str]] = []

    def _bcp_runner(self, options=None):
        del options
        return _FakeBcpRunner(self)


class _FakeBcpRunner:
    def __init__(self, connector: _FakeMssqlConnector) -> None:
        self.connector = connector

    def queryout_process(self, query: str, output_path: str, progress_callback=None):
        self.connector.calls.append((query, "fifo"))

        def write_payload() -> None:
            with open(output_path, "wb", buffering=0) as handle:
                handle.write(self.connector.payload)
            if progress_callback is not None:
                progress_callback("2 rows copied.")

        thread = Thread(target=write_payload, daemon=True)
        thread.start()
        return _FakeBcpProcess(thread, abort_error=self.connector.abort_error)


class _FakeBcpProcess:
    def __init__(self, thread: Thread, *, abort_error: Exception | None = None) -> None:
        self.thread = thread
        self.terminated = False
        self.abort_error = abort_error

    def wait(self) -> BcpResult:
        self.thread.join(timeout=5)
        return BcpResult(
            command=("bcp",),
            redacted_command=("bcp",),
            returncode=0,
            stdout="2 rows copied.",
            stderr="",
            rows_copied=2,
        )

    def poll(self):
        return None if self.thread.is_alive() else 0

    def abort(self) -> None:
        self.terminated = True
        self.thread.join(timeout=5)
        if self.abort_error is not None:
            raise self.abort_error


class ClickHouseConnector:
    pass
