"""Real three-pipe framing; synthetic process handles do not prove Linux reaping."""

import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters import mssql_sqlclient_departure_process as module
from dpone.adapters.mssql_sqlclient_departure_process import SqlClientDepartureProcess
from dpone.contracts.mssql_sqlclient_restricted_writer_settlement_codec import RESULT_FRAME_LIMIT
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup, encode_startup
from dpone.contracts.mssql_tds_frames import encode_message
from dpone.contracts.mssql_tds_worker import TdsProcessIdentity


def test_launch_admission_validates_only_supplied_pre_effect_fields():
    module._admit_departure_launch(implementation_sha256="b" * 64)
    module._admit_departure_launch(startup_deadline=10.25, operation_deadline=20.5)

    with pytest.raises(ValueError):
        module._admit_departure_launch(implementation_sha256="invalid")
    with pytest.raises(ValueError):
        module._admit_departure_launch(startup_deadline=20.5, operation_deadline=10.25)
    with pytest.raises(ValueError):
        module._admit_departure_launch(startup_deadline=10.25)


def test_canonical_startup_deep_validates_actual_process_identity():
    identity = TdsProcessIdentity("a" * 64, "11111111-1111-4111-8111-111111111111", 234, 1)

    startup = module._canonical_departure_startup(identity, "b" * 64, Path("/src"), b"n" * 32)

    assert startup == TdsCoordinatorStartup(identity, "b" * 64, "/src", b"n" * 32)
    assert startup.process is not identity
    with pytest.raises(ValueError, match="sqlclient_departure_process_invalid"):
        module._canonical_departure_startup(SimpleNamespace(**vars(identity)), "b" * 64, Path("/src"), b"n" * 32)


@pytest.fixture
def child(monkeypatch):
    pairs = [os.pipe() for _ in range(3)]
    identities = {fd: os.fstat(fd).st_ino for pair in pairs for fd in pair}
    for fd in identities:
        os.set_blocking(fd, False)
    identity = TdsProcessIdentity("a" * 64, "11111111-1111-4111-8111-111111111111", 234, 1)
    handle = SimpleNamespace(
        identity=identity,
        close=lambda: None,
        wait=lambda **kw: SimpleNamespace(reaped=True, exit_code=0),
        contain=lambda **kw: SimpleNamespace(reaped=True, exit_code=-9),
    )
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_departure_process.LinuxTdsProcess.identify", lambda pid: identity
    )
    expected = TdsCoordinatorStartup(identity, "b" * 64, "/src", b"n" * 32)
    deadline = time.monotonic() + 5
    worker = SqlClientDepartureProcess(
        SimpleNamespace(stdout=None, returncode=None),
        handle,
        (pairs[0][0], pairs[1][1], pairs[2][0]),
        expected=expected,
        startup_deadline=deadline,
        operation_deadline=deadline,
    )
    yield SimpleNamespace(worker=worker, pairs=pairs, expected=expected, deadline=deadline)
    for fd, inode in identities.items():
        try:
            if os.fstat(fd).st_ino == inode:
                os.close(fd)
        except OSError:
            pass


def emit(child, index, body, close=True):
    os.write(child.pairs[index][1], encode_message(body, max_payload=32768))
    if close:
        os.close(child.pairs[index][1])


def start(child):
    emit(child, 0, encode_startup(child.expected))
    assert child.worker.startup(deadline=child.deadline) == child.expected


def requesting(child):
    start(child)
    child.worker.send_request(b"private-canary", deadline=child.deadline)


def test_three_phases_and_raw_result(child):
    requesting(child)
    assert os.read(child.pairs[1][0], 100) == encode_message(b"private-canary", max_payload=1024 * 1024)
    assert os.read(child.pairs[1][0], 1) == b""
    emit(child, 2, b"result")
    assert child.worker.receive_result(deadline=child.deadline) == b"result"
    assert child.worker.received_result == b"result"
    assert b"private-canary" not in repr(vars(child.worker)).encode()
    assert child.worker.wait(deadline=child.deadline).reaped
    child.worker.close()


def test_restricted_writer_result_accepts_exact_bounded_frame_limit(child, monkeypatch):
    requesting(child)
    payload = b"r" * RESULT_FRAME_LIMIT

    def read(fd, *, deadline, max_payload, on_complete):
        assert max_payload == RESULT_FRAME_LIMIT
        on_complete(payload)
        return payload

    monkeypatch.setattr(module, "read_worker_message", read)
    assert child.worker.receive_result_bounded(deadline=child.deadline, max_payload=RESULT_FRAME_LIMIT) is payload


def test_result_limit_oversize_is_rejected_without_consuming_phase(child, monkeypatch):
    requesting(child)
    with pytest.raises(ValueError, match="result_limit"):
        child.worker.receive_result_bounded(deadline=child.deadline, max_payload=RESULT_FRAME_LIMIT + 1)

    monkeypatch.setattr(
        module,
        "read_worker_message",
        lambda fd, *, deadline, max_payload, on_complete: (on_complete(b"ok"), b"ok")[1],
    )
    assert child.worker.receive_result_bounded(deadline=child.deadline, max_payload=RESULT_FRAME_LIMIT) == b"ok"


def test_withheld_eof_has_no_retained_result(child):
    requesting(child)
    emit(child, 2, b"result", close=False)
    with pytest.raises(ValueError):
        child.worker.receive_result(deadline=time.monotonic() + 0.02)
    assert child.worker.received_result is None
    with pytest.raises(ValueError):
        child.worker.receive_result(deadline=child.deadline)


def test_complete_result_retained_on_descriptor_close_failure(child, monkeypatch):
    requesting(child)
    emit(child, 2, b"result")

    def fail(fd):
        raise OSError("close")

    monkeypatch.setattr(child.worker._resources, "close_descriptor", fail)
    with pytest.raises(OSError):
        child.worker.receive_result(deadline=child.deadline)
    assert child.worker.received_result == b"result"


def test_post_eof_deadline_retains_result_without_success(child, monkeypatch):
    requesting(child)

    def read(fd, *, deadline, max_payload, on_complete):
        on_complete(b"result")
        raise ValueError("expired")

    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_departure_process.read_worker_message", read)
    with pytest.raises(ValueError):
        child.worker.receive_result(deadline=child.deadline)
    assert child.worker.received_result == b"result"


def test_early_request_consumes_phase_and_prevents_retry(child):
    with pytest.raises(ValueError):
        child.worker.send_request(b"private-canary", deadline=child.deadline)
    # Wrong-phase call cannot silently authorize credential delivery.
    with pytest.raises(ValueError):
        child.worker.send_request(b"private-canary", deadline=child.deadline)


def test_nonrenewable_termination_deadline(child, monkeypatch):
    observed = []
    monkeypatch.setattr(child.worker._resources, "terminate", lambda *, deadline: observed.append(deadline))
    child.worker.terminate(deadline=child.deadline)
    child.worker.terminate(deadline=child.deadline + 10)
    child.worker.terminate(deadline=child.deadline - 1)
    assert observed == [child.deadline, child.deadline, child.deadline - 1]


def test_deadline_after_descriptor_close_prevents_normal_result(child, monkeypatch):
    requesting(child)
    emit(child, 2, b"result")
    original = child.worker._resources.close_descriptor

    def close(fd):
        original(fd)
        monkeypatch.setattr("dpone.adapters.mssql_sqlclient_departure_process.time.monotonic", lambda: child.deadline)

    monkeypatch.setattr(child.worker._resources, "close_descriptor", close)
    with pytest.raises(ValueError):
        child.worker.receive_result(deadline=child.deadline)
    assert child.worker.received_result == b"result"


@pytest.mark.parametrize("descriptors", [(1, 2), (1, 1, 2), (False, 1, 2), [0, 1, 2], (-1, 1, 2)])
def test_constructor_rejects_bad_descriptor_identity(child, descriptors):
    with pytest.raises(ValueError):
        SqlClientDepartureProcess(
            None,
            SimpleNamespace(identity=child.expected.process),
            descriptors,
            expected=child.expected,
            startup_deadline=child.deadline,
            operation_deadline=child.deadline,
        )


def test_wrong_startup_blocks_secret_request(child):
    from dataclasses import replace

    emit(child, 0, encode_startup(replace(child.expected, launch_nonce=b"x" * 32)))
    with pytest.raises(ValueError):
        child.worker.startup(deadline=child.deadline)
    with pytest.raises(ValueError):
        child.worker.send_request(b"private-canary", deadline=child.deadline)
    assert child.worker.startup_receipt is None


def test_invalid_result_framing_never_retained(child):
    requesting(child)
    os.write(child.pairs[2][1], b"\x00\x00\x00\x09short")
    os.close(child.pairs[2][1])
    with pytest.raises(ValueError):
        child.worker.receive_result(deadline=child.deadline)
    assert child.worker.received_result is None


def test_reentrant_failed_phase_cannot_restore_outer_success(child, monkeypatch):
    requesting(child)

    def read(fd, *, deadline, max_payload, on_complete):
        with pytest.raises(ValueError):
            child.worker.receive_result(deadline=deadline)
        on_complete(b"result")
        return b"result"

    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_departure_process.read_worker_message", read)
    with pytest.raises(ValueError):
        child.worker.receive_result(deadline=child.deadline)
    assert child.worker.received_result == b"result"
