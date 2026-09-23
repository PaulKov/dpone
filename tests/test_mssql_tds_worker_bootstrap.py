"""Bootstrap ordering over real pipes, with no host limits or SQL effects."""

import os
import sys
import time
from types import SimpleNamespace

import pytest

from dpone.adapters import mssql_tds_installation, mssql_tds_worker_guard
from dpone.adapters.mssql_tds_channels import read_worker_message
from dpone.adapters.mssql_tds_process import LinuxTdsProcess
from dpone.app import mssql_tds_worker_bootstrap as bootstrap
from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_tds_frames import encode_message
from dpone.contracts.mssql_tds_result import attempt_identity_digest, decode_result_payload
from dpone.contracts.mssql_tds_worker import TdsAttemptError, TdsAttemptIdentity, TdsProcessIdentity
from dpone.contracts.strict_json import strict_json_object

H = "a" * 64
IDENTITY = TdsAttemptIdentity("target", "run", 0, 0, H, H, H, H, "db", "stage", "owned", H)
RECEIPT = TdsInputReceipt(0, 0, H)


@pytest.fixture
def harness(monkeypatch):
    events = []
    descriptors = []
    for _ in range(3):
        pair = os.pipe()
        for fd in pair:
            os.set_blocking(fd, False)
        descriptors.extend(pair)
    control_r, control_w, startup_r, startup_w, result_r, result_w = descriptors
    job = SimpleNamespace(identity=IDENTITY, policy=SimpleNamespace(max_worker_address_space_bytes=128 * 1024**2))
    monkeypatch.setattr(mssql_tds_worker_guard, "install_worker_guard", lambda **kwargs: events.append("guard"))

    def identify(pid):
        events.append("identity")
        return TdsProcessIdentity(H, "11111111-1111-1111-1111-111111111111", pid, 1)

    def decode(body):
        events.append("decode")
        assert body == b"private-job"
        return job

    def copy(job):
        events.append("copy")
        return RECEIPT

    monkeypatch.setattr(
        mssql_tds_installation, "worker_installation_digest", lambda root: events.append("installation") or H
    )
    monkeypatch.setattr(LinuxTdsProcess, "identify", identify)
    monkeypatch.setitem(sys.modules, "dpone.app.mssql_tds_worker_request", SimpleNamespace(decode_job=decode))
    monkeypatch.setattr(bootstrap, "_copy_job", copy)
    kwargs = dict(
        expected_parent_pid=os.getppid(),
        address_space=128 * 1024**2,
        startup_deadline=time.monotonic() + 1,
        operation_deadline=time.monotonic() + 2,
        control_fd=control_r,
        startup_fd=startup_w,
        result_fd=result_w,
    )
    yield SimpleNamespace(
        events=events,
        kwargs=kwargs,
        control_w=control_w,
        startup_r=startup_r,
        result_r=result_r,
        result_w=result_w,
        job=job,
    )
    for fd in descriptors:
        try:
            os.close(fd)
        except OSError:
            pass


def send_job(h):
    os.write(h.control_w, encode_message(b"private-job", max_payload=1024))
    os.close(h.control_w)


def test_guards_startup_then_private_job_and_result(harness):
    h = harness
    send_job(h)
    assert bootstrap.run_worker(**h.kwargs) == 0
    assert h.events == ["guard", "installation", "identity", "decode", "copy"]
    startup = strict_json_object(read_worker_message(h.startup_r, deadline=time.monotonic() + 1, max_payload=16384))
    assert startup["process"]["pid"] == os.getpid()
    os.close(h.result_w)
    result = decode_result_payload(
        read_worker_message(h.result_r, deadline=time.monotonic() + 1, max_payload=16384),
        expected_attempt_sha256=attempt_identity_digest(IDENTITY),
        expected_input=RECEIPT,
    )
    assert result.receipt == RECEIPT


def test_full_job_without_control_eof_never_executes(harness):
    h = harness
    os.write(h.control_w, encode_message(b"private-job", max_payload=1024))
    h.kwargs["startup_deadline"] = time.monotonic() + 0.02
    with pytest.raises(ValueError, match="tds_channel_deadline"):
        bootstrap.run_worker(**h.kwargs)
    assert h.events == ["guard", "installation", "identity"]


def test_guard_failure_cannot_announce_startup_or_read_job(harness, monkeypatch):
    h = harness

    def fail(**kwargs):
        raise RuntimeError("guard failed")

    monkeypatch.setattr(mssql_tds_worker_guard, "install_worker_guard", fail)
    with pytest.raises(RuntimeError):
        bootstrap.run_worker(**h.kwargs)
    assert not h.events
    with pytest.raises(BlockingIOError):
        os.read(h.startup_r, 32)


def test_policy_mismatch_prevents_copy(harness):
    h = harness
    send_job(h)
    h.job.policy.max_worker_address_space_bytes += 1
    with pytest.raises(ValueError, match="tds_worker_policy_mismatch"):
        bootstrap.run_worker(**h.kwargs)
    assert "copy" not in h.events


def test_sdk_error_is_bounded_failure_result(harness, monkeypatch):
    h = harness
    send_job(h)

    def fail(job):
        raise RuntimeError("sensitive SDK detail")

    monkeypatch.setattr(bootstrap, "_copy_job", fail)
    assert bootstrap.run_worker(**h.kwargs) == 1
    os.close(h.result_w)
    result = decode_result_payload(
        read_worker_message(h.result_r, deadline=time.monotonic() + 1, max_payload=16384),
        expected_attempt_sha256=attempt_identity_digest(IDENTITY),
        expected_input=RECEIPT,
    )
    assert result.receipt is None and result.error is TdsAttemptError.DRIVER


def test_changed_implementation_in_private_job_prevents_copy(harness):
    from dataclasses import replace

    h = harness
    send_job(h)
    h.job.identity = replace(IDENTITY, implementation_sha256="b" * 64)
    with pytest.raises(ValueError, match="tds_worker_implementation_mismatch"):
        bootstrap.run_worker(**h.kwargs)
    assert "copy" not in h.events
