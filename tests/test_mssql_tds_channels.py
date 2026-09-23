"""Real nonblocking pipes cannot replace EOF with a complete-looking frame."""

import os
import subprocess
import sys
import time
from contextlib import contextmanager
from threading import Thread

import pytest

from dpone.adapters import mssql_tds_channels as channels
from dpone.adapters.mssql_tds_channels import read_worker_message, write_worker_control
from dpone.contracts.mssql_tds_frames import encode_message

PAYLOAD = b"arbitrary framed bytes: \xff\x00"
MAX_PAYLOAD = 16 * 1024


@contextmanager
def pipe():
    read, write = os.pipe()
    os.set_blocking(read, False)
    os.set_blocking(write, False)
    try:
        yield read, write
    finally:
        for fd in (read, write):
            try:
                os.close(fd)
            except OSError:
                pass


def read_message(fd, timeout=0.1):
    return read_worker_message(fd, deadline=time.monotonic() + timeout, max_payload=MAX_PAYLOAD)


def test_complete_frame_without_eof_times_out():
    with pipe() as (r, w):
        os.write(w, encode_message(PAYLOAD, max_payload=MAX_PAYLOAD))
        with pytest.raises(ValueError, match="tds_channel_deadline"):
            read_message(r, timeout=0.02)


def test_complete_frame_with_actual_eof():
    with pipe() as (r, w):
        body = PAYLOAD
        os.write(w, encode_message(body, max_payload=MAX_PAYLOAD))
        os.close(w)
        assert read_message(r) == body


@pytest.mark.parametrize(
    "payload", [b"", b"\0", b"\0\0\x40\x01", encode_message(PAYLOAD, max_payload=MAX_PAYLOAD) + b"x"]
)
def test_bad_or_truncated_frame_fails(payload):
    with pipe() as (r, w):
        if payload:
            os.write(w, payload)
        os.close(w)
        with pytest.raises(ValueError, match="tds_result_protocol"):
            read_message(r)


def test_control_bytes_exact_and_bounded():
    with pipe() as (r, w):
        write_worker_control(w, b"private-control", deadline=time.monotonic() + 0.1, max_bytes=32)
        assert os.read(r, 32) == b"private-control"
        with pytest.raises(ValueError, match="tds_channel_payload"):
            write_worker_control(w, b"too-big", deadline=time.monotonic() + 0.1, max_bytes=2)
        with pytest.raises(BlockingIOError):
            os.read(r, 32)


def test_partial_writes_and_interrupted_syscall_preserve_exact_bytes(monkeypatch):
    original = os.write
    calls = 0

    def partial(fd, data):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise InterruptedError
        if calls == 2:
            raise BlockingIOError
        return original(fd, data[:2])

    with pipe() as (r, w):
        monkeypatch.setattr(os, "write", partial)
        write_worker_control(w, b"abcdef", deadline=time.monotonic() + 0.1, max_bytes=32)
        assert os.read(r, 32) == b"abcdef"
        assert calls == 5


def test_fragmented_reads_with_transient_unavailability(monkeypatch):
    original = os.read
    calls = 0

    def partial(fd, size):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise BlockingIOError
        if calls == 2:
            raise InterruptedError
        return original(fd, min(size, 3))

    with pipe() as (r, w):
        body = PAYLOAD
        os.write(w, encode_message(body, max_payload=MAX_PAYLOAD))
        os.close(w)
        monkeypatch.setattr(os, "read", partial)
        assert read_message(r) == body


def test_full_control_pipe_cannot_block_deadline():
    with pipe() as (_r, w):
        while True:
            try:
                os.write(w, b"x" * 4096)
            except BlockingIOError:
                break
        with pytest.raises(ValueError, match="tds_channel_deadline"):
            write_worker_control(w, b"secret", deadline=time.monotonic() + 0.02, max_bytes=32)


def test_blocking_descriptor_rejected_before_io():
    with pipe() as (r, w):
        os.set_blocking(r, True)
        with pytest.raises(ValueError, match="tds_channel_requires_nonblocking"):
            read_message(r)
        os.set_blocking(w, True)
        with pytest.raises(ValueError, match="tds_channel_requires_nonblocking"):
            write_worker_control(w, b"x", deadline=time.monotonic() + 0.1, max_bytes=32)


def test_closed_reader_does_not_expose_control_content():
    with pipe() as (r, w):
        os.close(r)
        with pytest.raises(ValueError, match="tds_channel_io") as caught:
            write_worker_control(w, b"secret", deadline=time.monotonic() + 0.1, max_bytes=32)
        assert "secret" not in str(caught.value)


@pytest.mark.parametrize("deadline", [True, float("nan"), float("inf"), -1])
def test_invalid_or_expired_deadline_has_no_effect(deadline):
    with pipe() as (r, w):
        with pytest.raises(ValueError, match="tds_channel_deadline"):
            write_worker_control(w, b"x", deadline=deadline, max_bytes=32)
        with pytest.raises(BlockingIOError):
            os.read(r, 32)


def test_nonblocking_regular_file_rejected_without_io(tmp_path):
    path = tmp_path / "not-a-pipe"
    path.write_bytes(b"unchanged")
    fd = os.open(path, os.O_RDWR | os.O_NONBLOCK)
    try:
        with pytest.raises(ValueError, match="tds_channel_requires_pipe"):
            write_worker_control(fd, b"changed", deadline=time.monotonic() + 0.1, max_bytes=32)
        with pytest.raises(ValueError, match="tds_channel_requires_pipe"):
            read_message(fd)
        assert os.lseek(fd, 0, os.SEEK_CUR) == 0
        assert path.read_bytes() == b"unchanged"
    finally:
        os.close(fd)


def test_frame_finish_cannot_report_success_after_deadline(monkeypatch):
    now = 0.0
    original = channels.TdsMessageFrame.finish

    def late_finish(frame):
        nonlocal now
        body = original(frame)
        now = 2.0
        return body

    with pipe() as (r, w):
        os.write(w, encode_message(PAYLOAD, max_payload=MAX_PAYLOAD))
        os.close(w)
        monkeypatch.setattr(channels.time, "monotonic", lambda: now)
        monkeypatch.setattr(channels.TdsMessageFrame, "finish", late_finish)
        with pytest.raises(ValueError, match="tds_channel_deadline"):
            read_worker_message(r, deadline=1.0, max_payload=MAX_PAYLOAD)


def test_full_private_payload_cap_includes_four_byte_header():
    body = b"x" * (1024 * 1024)
    framed = encode_message(body, max_payload=len(body))
    errors = []
    with pipe() as (r, w):
        deadline = time.monotonic() + 5

        def write():
            try:
                write_worker_control(w, framed, deadline=deadline, max_bytes=len(body) + 4)
            except Exception as error:
                errors.append(error)
            finally:
                os.close(w)

        worker = Thread(target=write)
        worker.start()
        try:
            assert channels.read_worker_message(r, deadline=deadline, max_payload=len(body)) == body
        finally:
            worker.join(timeout=5)
        assert not worker.is_alive() and not errors


def test_transport_import_does_not_require_result_domain():
    script = """
import importlib.abc
import sys
class RejectDomain(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {"dpone.contracts.mssql_native_chunks", "dpone.contracts.mssql_tds_result"}:
            raise AssertionError("transport imported result domain")
sys.meta_path.insert(0, RejectDomain())
from dpone.adapters.mssql_tds_channels import read_worker_message, write_worker_control
assert callable(read_worker_message) and callable(write_worker_control)
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "body, limit", [(b"", 1), (bytearray(b"x"), 1), (b"xx", 1), (b"x", True), (b"x", 0), (b"x", 1024**2 + 1)]
)
def test_framed_write_rejects_before_writer_or_os_io(body, limit, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid frame reached writer or OS")

    monkeypatch.setattr(channels.os, "write", forbidden)
    with pytest.raises(ValueError, match="tds_result_protocol"):
        channels.write_worker_message(-1, body, deadline=0, max_payload=limit, writer=forbidden)


def test_framed_write_preserves_injected_writer_and_full_wire_cap():
    body = b"x" * 1024**2
    calls = []

    def writer(fd, payload, *, deadline, max_bytes):
        calls.append((fd, payload, deadline, max_bytes))

    channels.write_worker_message(91, body, deadline=17.25, max_payload=len(body), writer=writer)
    assert calls == [(91, len(body).to_bytes(4, "big") + body, 17.25, len(body) + 4)]


def test_framed_write_uses_real_borrowed_pipe_without_closing():
    with pipe() as (r, w):
        channels.write_worker_message(
            w, PAYLOAD, deadline=time.monotonic() + 1, max_payload=MAX_PAYLOAD, writer=write_worker_control
        )
        assert os.read(r, MAX_PAYLOAD) == len(PAYLOAD).to_bytes(4, "big") + PAYLOAD
        os.write(w, b"still-owned")
        assert os.read(r, 32) == b"still-owned"
