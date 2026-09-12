"""COPY wire and primary-error regression matrix for the functional exporter."""

import asyncio
import gzip
import hashlib
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from psycopg import sql

from dpone.runtime.connectors import postgres_copy_stream as module


class FalseFailure(RuntimeError):
    def __bool__(self):
        return False


class CopyConnection:
    def __init__(self, chunks=(), failure=None):
        self.chunks = iter(chunks)
        self.failure = failure
        self.calls = []

    @contextmanager
    def cursor(self):
        yield self

    @contextmanager
    def copy(self, query, params):
        self.calls.append((query, params))
        yield self

    def read(self):
        if self.failure is not None:
            raise self.failure
        return next(self.chunks, b"")


@pytest.mark.parametrize("format_name", ["CSV", "BINARY", "MSSQL_DELIMITED", "mssql-delimited"])
@pytest.mark.parametrize("composable", [False, True])
@pytest.mark.parametrize("compress", [False, True])
@pytest.mark.parametrize("chunks", [(), (b"1\n", memoryview(b"2\n"))])
def test_wire_payload_and_stats(format_name, composable, compress, chunks, tmp_path):
    query = sql.SQL("SELECT 1") if composable else "SELECT 1"
    path = tmp_path / "export.bin"
    connection = CopyConnection(chunks)
    result = module.PostgresCopyFileExporter(connection).export(
        query, str(path), format=format_name, compress=compress, params=(7,)
    )
    assert (gzip.decompress(path.read_bytes()) if compress else path.read_bytes()) == b"".join(chunks)
    normalized = format_name.upper().replace("-", "_")
    expected = {
        "CSV": "COPY (SELECT 1) TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE *)",
        "BINARY": "COPY (SELECT 1) TO STDOUT WITH (FORMAT BINARY)",
        "MSSQL_DELIMITED": r"COPY (SELECT 1) TO STDOUT WITH (FORMAT CSV, DELIMITER E'\t', QUOTE E'\x1f', ESCAPE E'\x1f', NULL '')",
    }[normalized]
    actual, params = connection.calls[0]
    assert params == (7,)
    assert (actual.as_string(None) if composable else actual) == expected
    assert result["total_bytes"] == sum(map(len, chunks))
    assert result["chunk_count"] == result["copy_read_count"] == len(chunks)
    is_delimited = format_name.upper().replace("-", "_") == "MSSQL_DELIMITED"
    assert result["rows_exported"] == (len(chunks) if is_delimited else None)
    assert result["sha256"] == (hashlib.sha256(b"".join(chunks)).hexdigest() if is_delimited and not compress else None)
    assert result["elapsed"] >= 0 and result["throughput"] >= 0


@pytest.mark.parametrize("compress", [False, True])
@pytest.mark.parametrize(
    "primary_type",
    [FalseFailure, RuntimeError, asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit, None],
)
def test_close_never_masks_primary(primary_type, compress, monkeypatch):
    primary = primary_type("copy failure") if primary_type else None
    secondary = RuntimeError("close failure")
    events = []

    def close():
        events.append("close")
        raise secondary

    def open_output(*args, **kwargs):
        events.append("open")
        return SimpleNamespace(write=lambda payload: None, close=close)

    monkeypatch.setattr(module, "open", open_output, raising=False)
    monkeypatch.setattr(module.gzip, "open", open_output)
    with pytest.raises(BaseException) as caught:
        module.PostgresCopyFileExporter(CopyConnection(failure=primary)).export("SELECT 1", "unused", compress=compress)
    assert caught.value is (primary if primary is not None else secondary)
    assert caught.value.__cause__ is caught.value.__context__ is None
    assert events == ["open", "close"]


@pytest.mark.parametrize("compress", [False, True])
@pytest.mark.parametrize("progress", [1, 2, 0, None, -1, True, 100])
def test_short_writes_complete_or_fail_closed(progress, compress, monkeypatch):
    stored = bytearray()
    closed = []

    def write(payload):
        if type(progress) is int and 0 < progress <= len(payload):
            stored.extend(payload[:progress])
            return progress
        if progress == 2 and len(payload) == 1:
            stored.extend(payload)
            return 1
        return progress

    output = SimpleNamespace(write=write, close=lambda: closed.append(True))
    monkeypatch.setattr(module, "open", lambda *args, **kwargs: output, raising=False)
    monkeypatch.setattr(module.gzip, "open", lambda *args, **kwargs: output)
    export = module.PostgresCopyFileExporter(CopyConnection([b"12\n"]))
    if type(progress) is int and progress in {1, 2}:
        result = export.export("SELECT 1", "unused", format="MSSQL_DELIMITED", compress=compress, buffer_size=0)
        assert stored == b"12\n"
        assert result["total_bytes"] == 3 and result["rows_exported"] == 1
    else:
        with pytest.raises(OSError, match="write_progress_invalid"):
            export.export("SELECT 1", "unused", compress=compress, buffer_size=0)
    assert closed == [True]


@pytest.mark.parametrize("stage", ["write_after_partial", "logger"])
def test_write_and_logger_primary_survives_close_failure(stage, monkeypatch):
    primary = FalseFailure("primary failure")
    events = []

    def write(payload):
        events.append("write")
        if stage == "write_after_partial":
            if events.count("write") == 1:
                return 1
            raise primary
        return len(payload)

    def log(*args):
        events.append("log")
        raise primary

    def close():
        events.append("close")
        raise RuntimeError("secondary close failure")

    output = SimpleNamespace(write=write, close=close)
    monkeypatch.setattr(module, "open", lambda *args, **kwargs: output, raising=False)
    payload = bytes(100 * 1024 * 1024) if stage == "logger" else b"12\n"
    with pytest.raises(BaseException) as caught:
        module.PostgresCopyFileExporter(CopyConnection([payload])).export(
            "SELECT 1", "unused", compress=False, logger=SimpleNamespace(log_etl_progress=log)
        )
    assert caught.value is primary
    assert primary.__cause__ is primary.__context__ is None
    assert events.count("close") == 1
    assert events.count("write") == (2 if stage == "write_after_partial" else 1)
    assert events.count("log") == (1 if stage == "logger" else 0)
