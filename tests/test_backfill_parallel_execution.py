"""Work-conserving fixed-lane backfill scheduling contracts."""

from __future__ import annotations

import inspect
import json
import multiprocessing
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager, nullcontext, suppress
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from multiprocessing import get_context
from multiprocessing.connection import Connection, wait
from pathlib import Path
from threading import Barrier, Event, Lock, Thread
from types import SimpleNamespace

import pytest

from dpone.backfill import (
    process_lane_operation,
    process_lane_parent,
    process_lane_processes,
    process_lane_worker,
)
from dpone.backfill.parallel_execution import run_parallel_chunk_lanes, run_process_chunk_lanes
from dpone.backfill.process_lane_contracts import (
    BackfillProcessLaneBootstrap,
    ProcessLaneBinding,
    ProcessLaneDispatch,
    ProcessLaneOperationLeaseReply,
    ProcessLaneOperationLeaseRequest,
    ProcessLaneReady,
    ProcessLaneRun,
    ProcessLaneSucceeded,
)
from dpone.backfill.process_lane_group import ready_identity_is_valid
from dpone.backfill.process_lane_messages import handle_lane_message
from dpone.backfill.process_lane_operation import (
    ParentOperationLeaseCoordinator,
    adapt_operation_binding_validator,
)
from dpone.backfill.process_lane_receipt import ParentReceiptReplayCoordinator
from dpone.backfill.process_lane_worker import (
    ProcessLaneOperationLeaseError,
    _OperationLeaseController,
    _OperationLeaseFactory,
    _response_timeout,
)
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlOperationRequest,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
    operation_owner_digest,
)
from dpone.contracts.portable_relation_scope import parse_portable_relation_scope
from dpone.contracts.portable_scope_binding import (
    PORTABLE_SCOPE_BINDING_OPTION,
    PortableScopeColumnContract,
    bind_portable_scope,
)
from tests.support.process_liveness import pid_is_running as _pid_is_running


def _append_event(path: str, **event: object) -> None:
    with Path(path).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(event, sort_keys=True) + "\n")


_SELF_TERMINATE_REQUEST = b"K"


def _self_terminate_on_request(listener: socket.socket) -> None:
    connection, _address = listener.accept()
    with connection:
        if connection.recv(1) == _SELF_TERMINATE_REQUEST:
            os.kill(os.getpid(), signal.SIGKILL)


def _request_self_termination(port_path: Path, *, sentinel: object) -> None:
    port = int(port_path.read_text(encoding="utf-8"))
    with socket.create_connection(("127.0.0.1", port), timeout=5.0) as connection:
        connection.sendall(_SELF_TERMINATE_REQUEST)
        assert wait([sentinel], timeout=5.0) == [sentinel]


def _operation(*, expires_at: datetime) -> MssqlTransactionOperation:
    request = MssqlAttemptRequest(
        invocation=InvocationIdentity(
            run_id="dpone-backfill:campaign-a",
            process="orders",
            task_partition="chunk",
        ),
        target_identity=b"t" * 32,
        route_fingerprint=b"r" * 32,
        load_id="load-a",
        target_database="DWH",
        target_schema="dbo",
        target_table="orders_shadow",
        strategy="backfill",
    )
    attempt = MssqlTransactionAttempt(request=request, generation=1)
    return MssqlTransactionOperation(
        attempt=attempt,
        operation_key=b"o" * 32,
        scope_hash=b"s" * 32,
        owner_digest=operation_owner_digest("owner-1"),
        epoch=7,
        lease_expires_at_utc=expires_at,
    )


def _receipt_authority(_dispatch, load_id, _portable_binding):
    attempt = MssqlAttemptRequest(
        invocation=InvocationIdentity("dpone-backfill:campaign-a", "orders", "orders:chunk"),
        target_identity=b"t" * 32,
        route_fingerprint=b"r" * 32,
        load_id=load_id,
        target_database="DWH",
        target_schema="dbo",
        target_table="orders_shadow",
        strategy="backfill",
    )
    return attempt, MssqlOperationRequest(b"s" * 32, operation_owner_digest("owner-1"))


class _ChunkCommand(int):
    """Integer test payload carrying the parent-issued operation deadline."""

    lease_expires_at_utc: datetime

    def __new__(cls, value: int, lease_expires_at_utc: datetime):
        instance = int.__new__(cls, value)
        instance.lease_expires_at_utc = lease_expires_at_utc
        return instance

    @property
    def options(self) -> dict[str, object]:
        return {
            "__dpone_mssql_operation_scope": {
                "lease_expires_at_utc": self.lease_expires_at_utc.isoformat(),
            }
        }

    def __reduce__(self):
        return type(self), (int(self), self.lease_expires_at_utc)


def _command_expiry(command: object) -> datetime:
    return command.lease_expires_at_utc  # type: ignore[attr-defined,no-any-return]


@contextmanager
def _open_test_process_lane(worker_id, payload, operation_lease_factory):
    events_path = str(payload["events_path"])
    self_termination_listener: socket.socket | None = None
    _append_event(
        events_path,
        event="lane_opened",
        worker_id=worker_id,
        pid=os.getpid(),
        start_method=multiprocessing.get_start_method(),
        active_threads=threading.active_count(),
    )
    if str(payload.get("mode") or "") == "bootstrap_descendant_hang":
        descendant = subprocess.Popen(  # noqa: S603 - exact test interpreter, no shell.
            [
                sys.executable,
                "-c",
                "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)",
            ]
        )
        Path(str(payload["descendant_pid_path"])).write_text(str(descendant.pid), encoding="utf-8")
        time.sleep(30)
    if str(payload.get("mode") or "") == "bootstrap_descendant_exit":
        descendant = subprocess.Popen(  # noqa: S603 - exact test interpreter, no shell.
            [
                sys.executable,
                "-c",
                "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)",
            ]
        )
        Path(str(payload["descendant_pid_path"])).write_text(str(descendant.pid), encoding="utf-8")
        os._exit(91)
    if str(payload.get("mode") or "") == "idle_exit_during_preflight" and worker_id == 3:
        self_termination_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self_termination_listener.bind(("127.0.0.1", 0))
        self_termination_listener.listen(1)
        port = self_termination_listener.getsockname()[1]
        Path(str(payload["self_termination_port_path"])).write_text(str(port), encoding="utf-8")
        self_termination_thread = Thread(
            target=_self_terminate_on_request,
            args=(self_termination_listener,),
            name="dpone-test-self-termination",
            daemon=True,
        )
        self_termination_thread.start()

    def run_chunk(chunk):
        mode = str(payload.get("mode") or "normal")
        result = {"extracted_rows": 1, "loaded_rows": 1, "chunk": chunk}
        started = time.monotonic()
        _append_event(events_path, event="chunk_started", worker_id=worker_id, chunk=chunk, at=started)
        if mode == "skew" and chunk == 1:
            time.sleep(0.4)
        elif mode == "skew":
            time.sleep(0.04)
        elif mode == "native_crash_resume":
            source_path = Path(str(payload["source_path"]))
            receipt_path = Path(str(payload["receipt_dir"])) / f"chunk-{chunk}.receipt"
            if not receipt_path.exists():
                _append_event(str(source_path), chunk=chunk)
                receipt_path.write_text("committed", encoding="utf-8")
                if chunk == 1 and not Path(str(payload["crash_marker"])).exists():
                    Path(str(payload["crash_marker"])).write_text("crashed", encoding="utf-8")
                    os.kill(os.getpid(), signal.SIGSEGV)
            if chunk == 2:
                failure_observed_path = Path(str(payload["failure_observed_path"]))
                deadline = time.monotonic() + 5
                while not failure_observed_path.exists() and time.monotonic() < deadline:
                    time.sleep(0.001)
                if not failure_observed_path.exists():
                    raise RuntimeError("test.process_lane_native_failure_not_observed")
            else:
                time.sleep(0.01)
        elif mode == "native_crash_hung_peer":
            if chunk == 1:
                time.sleep(0.05)
                Path(str(payload["crash_marker_path"])).write_text(
                    str(time.monotonic()),
                    encoding="utf-8",
                )
                os.kill(os.getpid(), signal.SIGSEGV)
            if chunk == 2:
                time.sleep(10)
        elif mode == "native_crash_descendant_peer":
            if chunk == 1:
                time.sleep(0.05)
                os.kill(os.getpid(), signal.SIGSEGV)
            if chunk == 2:
                descendant = subprocess.Popen(  # noqa: S603 - exact test interpreter, no shell.
                    [
                        sys.executable,
                        "-c",
                        "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)",
                    ]
                )
                Path(str(payload["descendant_pid_path"])).write_text(
                    str(descendant.pid),
                    encoding="utf-8",
                )
                time.sleep(30)
        elif mode == "native_crash_with_descendant":
            operation = _operation(expires_at=_command_expiry(chunk))
            controller = operation_lease_factory(None, SimpleNamespace(operation=operation))
            controller.start()
            descendant = subprocess.Popen(  # noqa: S603 - exact test interpreter, no shell.
                [
                    sys.executable,
                    "-c",
                    "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)",
                ]
            )
            Path(str(payload["descendant_pid_path"])).write_text(
                str(descendant.pid),
                encoding="utf-8",
            )
            os.kill(os.getpid(), signal.SIGSEGV)
        elif mode == "native_crash_process_tree":
            ready_path = Path(str(payload["grandchild_ready_path"]))
            if chunk == 1:
                deadline = time.monotonic() + 3
                while not ready_path.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                if not ready_path.exists():
                    raise RuntimeError("grandchild readiness timeout")
                os.kill(os.getpid(), signal.SIGSEGV)
            if chunk == 2:
                process = subprocess.Popen(  # noqa: S603 - test helper executable is the current interpreter.
                    [
                        sys.executable,
                        "-c",
                        _GRANDCHILD_SCRIPT,
                        str(ready_path),
                        str(payload["grandchild_stopped_path"]),
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                Path(str(payload["grandchild_pid_path"])).write_text(str(process.pid), encoding="utf-8")
                process.wait(timeout=30)
        elif mode == "native_crash_own_grandchild_receipt":
            operation = _operation(expires_at=_command_expiry(chunk))
            controller = operation_lease_factory(None, SimpleNamespace(operation=operation))
            controller.start()
            process = subprocess.Popen(  # noqa: S603 - test helper executable is the current interpreter.
                [
                    sys.executable,
                    "-c",
                    _GRANDCHILD_SCRIPT,
                    str(payload["grandchild_ready_path"]),
                    str(payload["grandchild_stopped_path"]),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            Path(str(payload["grandchild_pid_path"])).write_text(str(process.pid), encoding="utf-8")
            ready_path = Path(str(payload["grandchild_ready_path"]))
            deadline = time.monotonic() + 3
            while not ready_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            if not ready_path.exists():
                raise RuntimeError("grandchild readiness timeout")
            Path(str(payload["receipt_path"])).write_text("committed", encoding="utf-8")
            os.kill(os.getpid(), signal.SIGSEGV)
        elif mode == "operation_lease":
            operation = _operation(expires_at=_command_expiry(chunk))
            controller = operation_lease_factory(None, SimpleNamespace(operation=operation))
            controller.start()
            controller.assert_healthy()
            time.sleep(0.55)
            controller.assert_healthy()
            controller.stop()
        elif mode == "operation_unregister_crash":
            operation = _operation(expires_at=_command_expiry(chunk))
            controller = operation_lease_factory(None, SimpleNamespace(operation=operation))
            controller.start()
            controller.assert_healthy()
            Path(str(payload["receipt_path"])).write_text("committed", encoding="utf-8")
            controller.stop()
            Path(str(payload["unregistered_path"])).write_text("true", encoding="utf-8")
            os.kill(os.getpid(), signal.SIGSEGV)
        elif mode == "exit_after_success_delivery":
            subprocess.Popen(  # noqa: S603 - exact test interpreter, no shell.
                [
                    sys.executable,
                    "-c",
                    (
                        "import os,pathlib,signal,sys,time; "
                        "marker=pathlib.Path(sys.argv[1]); done=pathlib.Path(sys.argv[2]); "
                        "deadline=time.monotonic()+5; "
                        "\nwhile not marker.exists() and time.monotonic()<deadline: time.sleep(0.001); "
                        "\nos.kill(os.getppid(), signal.SIGKILL); done.write_text('done')"
                    ),
                    str(payload["exit_marker_path"]),
                    str(payload["exit_done_path"]),
                ]
            )
        elif mode == "malformed_frame_receipt":
            operation = _operation(expires_at=_command_expiry(chunk))
            controller = operation_lease_factory(None, SimpleNamespace(operation=operation))
            controller.start()
            Path(str(payload["receipt_path"])).write_text("committed", encoding="utf-8")
            controller.stop()
            operation_lease_factory.connection.send_bytes(b"not-a-valid-pickle-frame")
            time.sleep(10)
        elif mode == "receipt_replay":
            operation_lease_factory.authorize_receipt_probe(chunk, load_id="load-a")
            result = {
                "status": "success",
                "load_id": "load-a",
                "commit_receipt_id": "receipt-a",
                "commit_outcome": "replay_suppressed",
            }
        finished = time.monotonic()
        _append_event(
            events_path,
            event="chunk_finished",
            worker_id=worker_id,
            chunk=chunk,
            at=finished,
            active_threads=threading.active_count(),
        )
        return result

    try:
        yield run_chunk
    finally:
        if self_termination_listener is not None:
            self_termination_listener.close()
        _append_event(events_path, event="lane_closed", worker_id=worker_id, pid=os.getpid())


def _events(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


_GRANDCHILD_SCRIPT = """
import os
from pathlib import Path
import signal
import sys
import time

ready = Path(sys.argv[1])
stopped = Path(sys.argv[2])

def stop(_signum, _frame):
    stopped.write_text(str(os.getpid()), encoding="utf-8")
    raise SystemExit(0)

signal.signal(signal.SIGTERM, stop)
ready.write_text(str(os.getpid()), encoding="utf-8")
time.sleep(30)
"""


def _dispatch(worker_id: int, chunk: int, capture=None) -> ProcessLaneDispatch | None:
    del worker_id
    command = _ChunkCommand(chunk, datetime.now(UTC) + timedelta(seconds=1))
    dispatch = ProcessLaneDispatch(
        command_id=f"chunk-{chunk}",
        load_config=command,
        binding=ProcessLaneBinding(
            run_key="campaign-a",
            chunk_index=chunk,
            owner=f"owner-{chunk}",
        ),
        parent_context=chunk,
    )
    if capture is None:
        return dispatch
    capture(dispatch)
    return None


def test_fast_lane_starts_its_next_chunk_without_waiting_for_slowest_lane() -> None:
    """Skew in one lane must not idle otherwise healthy source/sink sessions."""

    slow_started = Event()
    release_slow = Event()
    fast_advanced = Event()
    first_pair = Barrier(2)
    calls: list[tuple[int, int]] = []
    calls_lock = Lock()
    state_opened: list[int] = []
    state_closed: list[int] = []
    runner_opened: list[int] = []
    runner_closed: list[int] = []

    @contextmanager
    def state_store_factory(worker_id: int):
        state_opened.append(worker_id)
        try:
            yield object()
        finally:
            state_closed.append(worker_id)

    @contextmanager
    def chunk_runner_factory(worker_id: int):
        runner_opened.append(worker_id)
        try:
            yield worker_id
        finally:
            runner_closed.append(worker_id)

    def run_chunk(chunk, _store, _errors, worker_id):
        with calls_lock:
            calls.append((worker_id, chunk))
        if chunk in {1, 2}:
            first_pair.wait(timeout=2)
        if chunk == 2:
            slow_started.set()
            assert release_slow.wait(timeout=5)
        if chunk == 3:
            fast_advanced.set()
        return True

    result: list[list[str]] = []
    execution = Thread(
        target=lambda: result.append(
            run_parallel_chunk_lanes(
                [1, 2, 3, 4],
                workers=2,
                state_store_factory=state_store_factory,
                chunk_runner_factory=chunk_runner_factory,
                run_chunk=run_chunk,
            )
        )
    )
    execution.start()
    assert slow_started.wait(timeout=2)
    advanced_before_slow_lane_finished = fast_advanced.wait(timeout=1)
    release_slow.set()
    execution.join(timeout=5)

    assert not execution.is_alive()
    assert advanced_before_slow_lane_finished is True
    assert result == [[]]
    assert sorted(chunk for _worker_id, chunk in calls) == [1, 2, 3, 4]
    lane_by_chunk = {chunk: worker_id for worker_id, chunk in calls}
    assert lane_by_chunk[3] == lane_by_chunk[1]
    assert sorted(state_opened) == [0, 1]
    assert sorted(state_closed) == [0, 1]
    assert sorted(runner_opened) == [0, 1]
    assert sorted(runner_closed) == [0, 1]


def test_first_failure_stops_new_claims_with_only_current_lane_work_in_flight() -> None:
    """Fail-fast permits at most the already-running work from other lanes."""

    peer_started = Event()
    release_peer = Event()
    failed_lane_closed = Event()
    first_pair = Barrier(2)
    calls: list[int] = []
    calls_lock = Lock()
    failed_workers: set[int] = set()

    @contextmanager
    def state_store_factory(_worker_id: int):
        yield object()

    @contextmanager
    def chunk_runner_factory(worker_id: int):
        try:
            yield worker_id
        finally:
            if worker_id in failed_workers:
                failed_lane_closed.set()

    def run_chunk(chunk, _store, errors, worker_id):
        with calls_lock:
            calls.append(chunk)
        if chunk in {1, 2}:
            first_pair.wait(timeout=2)
        if chunk == 1:
            assert peer_started.wait(timeout=2)
            failed_workers.add(worker_id)
            errors.append("chunk 1 failed")
            return False
        if chunk == 2:
            peer_started.set()
            assert release_peer.wait(timeout=5)
            assert failed_lane_closed.wait(timeout=5)
        return True

    result: list[list[str]] = []
    execution = Thread(
        target=lambda: result.append(
            run_parallel_chunk_lanes(
                [1, 2, 3, 4, 5, 6],
                workers=2,
                state_store_factory=state_store_factory,
                chunk_runner_factory=chunk_runner_factory,
                run_chunk=run_chunk,
            )
        )
    )
    execution.start()
    assert peer_started.wait(timeout=2)
    release_peer.set()
    execution.join(timeout=5)

    assert not execution.is_alive()
    assert sorted(calls) == [1, 2]
    assert result == [["chunk 1 failed"]]


def test_legacy_parallel_parent_ticks_campaign_coordinator_while_lanes_run() -> None:
    ticks: list[float] = []

    @contextmanager
    def state_store_factory(_worker_id: int):
        yield object()

    def run_chunk(_chunk, _store, _errors, _runner):
        time.sleep(0.15)
        return True

    errors = run_parallel_chunk_lanes(
        [1, 2],
        workers=2,
        state_store_factory=state_store_factory,
        chunk_runner_factory=None,
        run_chunk=run_chunk,
        coordinator_tick=lambda: ticks.append(time.monotonic()),
    )

    assert errors == []
    assert len(ticks) >= 2


def test_spawned_lanes_are_work_conserving_reused_and_closed_exactly_once(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    completed: list[int] = []
    failed: list[tuple[int, str]] = []

    summary = run_process_chunk_lanes(
        [1, 2, 3, 4],
        workers=2,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={"events_path": str(events_path), "mode": "skew"},
        ),
        claim_chunk=_dispatch,
        complete_chunk=lambda dispatch, _result: completed.append(dispatch.parent_context) is None,
        fail_chunk=lambda dispatch, error: failed.append((dispatch.parent_context, error)),
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
    )

    events = _events(events_path)
    opened = [event for event in events if event["event"] == "lane_opened"]
    closed = [event for event in events if event["event"] == "lane_closed"]
    starts = {int(event["chunk"]): event for event in events if event["event"] == "chunk_started"}
    finishes = {int(event["chunk"]): event for event in events if event["event"] == "chunk_finished"}

    assert summary.errors == ()
    assert completed == [2, 3, 4, 1]
    assert failed == []
    assert len(opened) == len(closed) == 2
    assert len({event["pid"] for event in opened}) == 2
    assert {event["start_method"] for event in opened} == {"spawn"}
    assert {event["active_threads"] for event in opened} == {1}
    assert starts[3]["worker_id"] == starts[2]["worker_id"]
    assert float(finishes[3]["at"]) < float(finishes[1]["at"])
    assert summary.lanes_started == 2
    assert all(lane.faulthandler_enabled for lane in summary.lanes)
    assert all(lane.process_tree_isolated for lane in summary.lanes)
    assert all(lane.pid == lane.process_group_id == lane.session_id for lane in summary.lanes)
    assert {lane.exitcode for lane in summary.lanes} == {0}


def test_ready_peer_at_final_fence_defers_then_retries_without_campaign_abort(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    claims: list[int] = []
    completed: list[int] = []
    deferred: list[tuple[int, str]] = []
    final_proofs = 0

    def claim(worker_id: int, chunk: int, capture) -> None:
        claims.append(chunk)
        _dispatch(worker_id, chunk, capture)

    def reprove() -> None:
        nonlocal final_proofs
        final_proofs += 1
        if final_proofs != 2:
            return
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            finished = {int(event["chunk"]) for event in _events(events_path) if event.get("event") == "chunk_finished"}
            if 1 in finished:
                time.sleep(0.05)
                return
            time.sleep(0.005)
        raise AssertionError("active peer did not become ready during final fence")

    summary = run_process_chunk_lanes(
        [1, 2, 3],
        workers=2,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={"events_path": str(events_path), "mode": "skew"},
        ),
        claim_chunk=claim,
        complete_chunk=lambda dispatch, _result: completed.append(dispatch.parent_context) is None,
        fail_chunk=lambda dispatch, error: deferred.append((dispatch.parent_context, error)),
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
        coordinator_tick=lambda: None,
        coordinator_reprove=reprove,
    )

    starts = [int(event["chunk"]) for event in _events(events_path) if event.get("event") == "chunk_started"]
    assert summary.errors == ()
    assert claims == [1, 2, 3]
    assert deferred == []
    assert sorted(completed) == [1, 2, 3]
    assert sorted(starts) == [1, 2, 3]
    assert final_proofs >= 3


def test_terminal_control_loss_releases_retained_unsent_claim_once(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    claims: list[int] = []
    completed: list[int] = []
    failed: list[tuple[int, str]] = []
    final_proofs = 0
    deferred = False

    def claim(worker_id: int, chunk: int, capture) -> None:
        claims.append(chunk)
        _dispatch(worker_id, chunk, capture)

    def tick() -> None:
        if deferred:
            raise RuntimeError("campaign lease lost after deferral")

    def reprove() -> None:
        nonlocal deferred, final_proofs
        final_proofs += 1
        if final_proofs != 2:
            return
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if any(
                event.get("event") == "chunk_finished" and int(event["chunk"]) == 1 for event in _events(events_path)
            ):
                time.sleep(0.05)
                deferred = True
                return
            time.sleep(0.005)
        raise AssertionError("active peer did not become ready during final fence")

    summary = run_process_chunk_lanes(
        [1, 2, 3],
        workers=2,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={"events_path": str(events_path), "mode": "skew"},
        ),
        claim_chunk=claim,
        complete_chunk=lambda dispatch, _result: completed.append(dispatch.parent_context) is None,
        fail_chunk=lambda dispatch, error: failed.append((dispatch.parent_context, error)),
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
        coordinator_tick=tick,
        coordinator_reprove=reprove,
    )

    starts = [int(event["chunk"]) for event in _events(events_path) if event.get("event") == "chunk_started"]
    assert claims == [1, 2, 3]
    assert sorted(completed) == [1, 2]
    assert failed == [(3, "DPONE_BACKFILL_PROCESS_COORDINATOR_LOST")]
    assert sorted(starts) == [1, 2]
    assert summary.errors == ("DPONE_BACKFILL_PROCESS_COORDINATOR_LOST",)


def test_process_signal_on_retained_retry_has_single_cleanup_owner(tmp_path: Path) -> None:
    """A deferred batch transfers ownership before a retry can raise."""

    class _ProcessSignal(BaseException):
        pass

    events_path = tmp_path / "events.jsonl"
    claims: list[int] = []
    failed: list[tuple[int, str]] = []
    final_proofs = 0
    deferred = False
    chunk_three_refreshes = 0

    def claim(worker_id: int, chunk: int, capture) -> None:
        claims.append(chunk)
        _dispatch(worker_id, chunk, capture)

    def refresh(dispatch: ProcessLaneDispatch) -> ProcessLaneDispatch:
        nonlocal chunk_three_refreshes
        if dispatch.parent_context == 3:
            chunk_three_refreshes += 1
            if chunk_three_refreshes == 2:
                assert deferred is True
                raise _ProcessSignal("operator stop after deferral")
        return dispatch

    def reprove() -> None:
        nonlocal deferred, final_proofs
        final_proofs += 1
        if final_proofs != 2:
            return
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if any(
                event.get("event") == "chunk_finished" and int(event["chunk"]) == 1 for event in _events(events_path)
            ):
                time.sleep(0.05)
                deferred = True
                return
            time.sleep(0.005)
        raise AssertionError("active peer did not become ready during final fence")

    with pytest.raises(_ProcessSignal, match="operator stop after deferral"):
        run_process_chunk_lanes(
            [1, 2, 3],
            workers=2,
            runtime=BackfillProcessLaneBootstrap(
                entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
                payload={"events_path": str(events_path), "mode": "skew"},
            ),
            claim_chunk=claim,
            complete_chunk=lambda _dispatch, _result: True,
            fail_chunk=lambda dispatch, error: failed.append((dispatch.parent_context, error)),
            heartbeat_chunk=lambda _dispatch: True,
            heartbeat_interval_seconds=0.05,
            coordinator_tick=lambda: None,
            coordinator_reprove=reprove,
            refresh_dispatch=refresh,
        )

    starts = [int(event["chunk"]) for event in _events(events_path) if event.get("event") == "chunk_started"]
    assert claims == [1, 2, 3]
    assert [chunk for chunk, _error in failed] == [3]
    assert "stage=lifecycle" in failed[0][1]
    assert sorted(starts) == [1, 2]


def test_four_lane_startup_finishes_slow_peer_preflight_before_any_child_command(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    completed: list[int] = []
    claims: list[int] = []

    def slow_final_claim(worker_id: int, chunk: int, capture) -> None:
        claims.append(chunk)
        if chunk == 4:
            time.sleep(0.45)
            assert not any(event.get("event") == "chunk_started" for event in _events(events_path))
        _dispatch(worker_id, chunk, capture)

    summary = run_process_chunk_lanes(
        [1, 2, 3, 4],
        workers=4,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={"events_path": str(events_path), "mode": "receipt_replay"},
        ),
        claim_chunk=slow_final_claim,
        complete_chunk=lambda dispatch, _result: completed.append(dispatch.parent_context) is None,
        fail_chunk=lambda _dispatch, error: pytest.fail(error),
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
        issue_receipt_probe=_receipt_authority,
        validate_replay_result=lambda *_args: True,
    )

    assert summary.errors == ()
    assert claims == [1, 2, 3, 4]
    assert sorted(completed) == [1, 2, 3, 4]


def test_idle_lane_exit_during_four_lane_preflight_sends_zero_commands(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    self_termination_port_path = tmp_path / "self-termination-port"
    failed: list[int] = []

    def terminate_idle_peer_during_final_claim(worker_id: int, chunk: int, capture) -> None:
        if chunk == 4:
            _request_self_termination(
                self_termination_port_path,
                sentinel=capture.lane.process.sentinel,
            )
        _dispatch(worker_id, chunk, capture)

    summary = run_process_chunk_lanes(
        [1, 2, 3, 4],
        workers=4,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={
                "events_path": str(events_path),
                "mode": "idle_exit_during_preflight",
                "self_termination_port_path": str(self_termination_port_path),
            },
        ),
        claim_chunk=terminate_idle_peer_during_final_claim,
        complete_chunk=lambda _dispatch, _result: pytest.fail("no child command may start"),
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
    )

    assert failed == [1, 2, 3, 4]
    assert not any(event.get("event") == "chunk_started" for event in _events(events_path))
    assert any("worker_id=3" in error and "NATIVE_EXIT" in error for error in summary.errors)


@pytest.mark.parametrize("fatal", (False, True))
def test_full_frame_send_error_quiesces_lane_before_claim_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fatal: bool,
) -> None:
    class _ProcessSignal(BaseException):
        pass

    events_path = tmp_path / "events.jsonl"
    completed: list[int] = []
    failed: list[tuple[int, bool]] = []
    delivered: list[ProcessLaneRun] = []
    captured_lanes: list[process_lane_processes.ProcessLaneHandle] = []
    real_start_lanes = process_lane_parent.start_lanes
    send_failure: BaseException = (
        _ProcessSignal("operator stop after full frame") if fatal else OSError("acknowledgement side unavailable")
    )

    class _FullFrameThenError:
        def __init__(self, connection: Connection) -> None:
            self._connection = connection
            self._failed = False

        def fileno(self) -> int:
            return self._connection.fileno()

        def send(self, message: object) -> None:
            self._connection.send(message)
            if isinstance(message, ProcessLaneRun) and not self._failed:
                self._failed = True
                delivered.append(message)
                raise send_failure

        def recv(self):
            return self._connection.recv()

        def close(self) -> None:
            self._connection.close()

    def start_with_ambiguous_send(runtime, workers, *, ownership=None):
        lanes = real_start_lanes(runtime, workers, ownership=ownership)
        captured_lanes.extend(lanes)
        lanes[0].connection = _FullFrameThenError(lanes[0].connection)
        return lanes

    monkeypatch.setattr(process_lane_parent, "start_lanes", start_with_ambiguous_send)

    expectation = pytest.raises(_ProcessSignal, match="operator stop after full frame") if fatal else nullcontext()
    with expectation:
        summary = run_process_chunk_lanes(
            [1],
            workers=1,
            runtime=BackfillProcessLaneBootstrap(
                entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
                payload={"events_path": str(events_path)},
            ),
            claim_chunk=_dispatch,
            complete_chunk=lambda dispatch, _result: completed.append(dispatch.parent_context) is None,
            fail_chunk=lambda dispatch, _error: failed.append(
                (dispatch.parent_context, captured_lanes[0].process.is_alive())
            ),
            heartbeat_chunk=lambda _dispatch: True,
            heartbeat_interval_seconds=0.05,
            shutdown_timeout_seconds=0.5,
        )

    assert len(delivered) == 1
    assert completed == []
    assert failed == [(1, False)]
    if not fatal:
        assert any("stage=ipc_send" in error for error in summary.errors)


def test_fatal_partial_batch_send_stops_all_trees_before_any_claim_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ProcessSignal(BaseException):
        pass

    events_path = tmp_path / "events.jsonl"
    captured_lanes: list[process_lane_processes.ProcessLaneHandle] = []
    failed: list[tuple[int, tuple[bool, ...]]] = []
    real_start_lanes = process_lane_parent.start_lanes

    class _FullFrameThenSignal:
        def __init__(self, connection: Connection) -> None:
            self._connection = connection

        def fileno(self) -> int:
            return self._connection.fileno()

        def send(self, message: object) -> None:
            self._connection.send(message)
            if isinstance(message, ProcessLaneRun):
                raise _ProcessSignal("fatal after full frame")

        def recv(self):
            return self._connection.recv()

        def close(self) -> None:
            self._connection.close()

    def start_with_second_lane_signal(runtime, workers, *, ownership=None):
        lanes = real_start_lanes(runtime, workers, ownership=ownership)
        captured_lanes.extend(lanes)
        lanes[1].connection = _FullFrameThenSignal(lanes[1].connection)
        return lanes

    monkeypatch.setattr(process_lane_parent, "start_lanes", start_with_second_lane_signal)

    with pytest.raises(_ProcessSignal, match="fatal after full frame"):
        run_process_chunk_lanes(
            [1, 2, 3],
            workers=3,
            runtime=BackfillProcessLaneBootstrap(
                entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
                payload={"events_path": str(events_path)},
            ),
            claim_chunk=_dispatch,
            complete_chunk=lambda _dispatch, _result: pytest.fail("fatal batch cannot complete normally"),
            fail_chunk=lambda dispatch, _error: failed.append(
                (
                    dispatch.parent_context,
                    tuple(lane.process.is_alive() for lane in captured_lanes),
                )
            ),
            heartbeat_chunk=lambda _dispatch: True,
            heartbeat_interval_seconds=0.05,
            shutdown_timeout_seconds=0.5,
        )

    assert sorted(chunk for chunk, _alive in failed) == [1, 2, 3]
    assert all(not any(alive) for _chunk, alive in failed)


def test_failed_startup_claim_releases_every_prepared_owner_before_source_io(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    failed: list[int] = []

    def fail_second_claim(worker_id: int, chunk: int, capture) -> None:
        if chunk == 2:
            raise RuntimeError("claim rejected")
        _dispatch(worker_id, chunk, capture)

    summary = run_process_chunk_lanes(
        [1, 2, 3],
        workers=3,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={"events_path": str(events_path)},
        ),
        claim_chunk=fail_second_claim,
        complete_chunk=lambda _dispatch, _result: True,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.parent_context),
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
    )

    assert failed == [1]
    assert not any(event.get("event") == "chunk_started" for event in _events(events_path))
    assert len(summary.errors) == 1
    assert "stage=claim" in summary.errors[0]


def test_campaign_coordinator_failure_is_latched_during_running_lane_drain(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    ticks = 0

    def coordinator_tick() -> None:
        nonlocal ticks
        ticks += 1
        if ticks >= 2:
            raise RuntimeError("campaign lease lost")

    summary = run_process_chunk_lanes(
        [1],
        workers=1,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={"events_path": str(events_path), "mode": "skew"},
        ),
        claim_chunk=_dispatch,
        complete_chunk=lambda _dispatch, _result: True,
        fail_chunk=lambda _dispatch, _error: None,
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
        coordinator_tick=coordinator_tick,
        coordinator_reprove=coordinator_tick,
        shutdown_timeout_seconds=1.0,
    )

    assert ticks == 2
    assert summary.errors.count("DPONE_BACKFILL_PROCESS_COORDINATOR_LOST") == 1


def test_parent_rejects_unproven_process_tree_before_chunk_claim() -> None:
    context = get_context("spawn")
    parent, child = context.Pipe(duplex=True)
    process = SimpleNamespace(pid=os.getpid(), exitcode=None)
    lane = process_lane_processes.ProcessLaneHandle(worker_id=0, process=process, connection=parent)
    child.send(
        ProcessLaneReady(
            worker_id=0,
            pid=os.getpid(),
            process_group_id=os.getpid(),
            session_id=os.getpid(),
            process_tree_isolated=False,
            start_method="spawn",
            faulthandler_enabled=True,
        )
    )

    try:
        errors = process_lane_processes.await_ready([lane], timeout_seconds=0.1)
    finally:
        parent.close()
        child.close()

    assert lane.ready is None
    assert errors == [
        "DPONE_BACKFILL_PROCESS_BOOTSTRAP_FAILED: worker_id=0 error=DPONE_BACKFILL_PROCESS_TREE_ISOLATION_FAILED"
    ]


def test_native_lane_exit_stops_dispatch_and_receipt_resume_does_not_repeat_source(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    source_path = tmp_path / "source.jsonl"
    receipt_dir = tmp_path / "receipts"
    receipt_dir.mkdir()
    crash_marker = tmp_path / "crash-marker"
    failure_observed_path = tmp_path / "failure-observed"
    claims: list[int] = []
    completed: list[int] = []
    failed: list[tuple[int, str]] = []
    runtime = BackfillProcessLaneBootstrap(
        entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
        payload={
            "events_path": str(events_path),
            "mode": "native_crash_resume",
            "source_path": str(source_path),
            "receipt_dir": str(receipt_dir),
            "crash_marker": str(crash_marker),
            "failure_observed_path": str(failure_observed_path),
        },
    )

    def fail(dispatch: ProcessLaneDispatch, error: str) -> None:
        failed.append((dispatch.parent_context, error))
        if dispatch.parent_context == 1:
            failure_observed_path.write_text("observed", encoding="utf-8")

    def claim(worker_id: int, chunk: int, capture) -> None:
        # A work-conserving supervisor may dispatch until a native exit is
        # observable. Once observed, no later claim is allowed.
        assert not failure_observed_path.exists()
        claims.append(chunk)
        _dispatch(worker_id, chunk, capture)

    first = run_process_chunk_lanes(
        [1, 2, 3, 4],
        workers=2,
        runtime=runtime,
        claim_chunk=claim,
        complete_chunk=lambda dispatch, _result: completed.append(dispatch.parent_context) is None,
        fail_chunk=fail,
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
    )

    assert completed == [2]
    assert claims == [1, 2]
    assert [chunk for chunk, _error in failed] == [1]
    assert not any(event.get("chunk") in {3, 4} for event in _events(events_path))
    assert any(
        "exitcode=-11" in error and "signal=SIGSEGV" in error and "faulthandler_enabled=true" in error
        for error in first.errors
    )
    assert sum(error.startswith("DPONE_BACKFILL_PROCESS_LANE_NATIVE_EXIT") for error in first.errors) == 1
    assert sorted(lane.exitcode for lane in first.lanes if lane.exitcode is not None) == [-signal.SIGSEGV, 0]

    resumed: list[tuple[int, dict[str, object]]] = []
    second = run_process_chunk_lanes(
        [1],
        workers=1,
        runtime=runtime,
        claim_chunk=_dispatch,
        complete_chunk=lambda dispatch, result: resumed.append((dispatch.parent_context, dict(result))) is None,
        fail_chunk=fail,
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
    )

    source_events = _events(source_path)
    assert second.errors == ()
    assert resumed == [(1, {"extracted_rows": 1, "loaded_rows": 1, "chunk": 1})]
    assert [event["chunk"] for event in source_events].count(1) == 1
    assert (receipt_dir / "chunk-1.receipt").read_text(encoding="utf-8") == "committed"


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups are required")
def test_bootstrap_timeout_terminates_descendants_before_any_chunk_claim(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    descendant_pid_path = tmp_path / "descendant.pid"
    claims: list[int] = []
    descendant_pid: int | None = None

    try:
        summary = run_process_chunk_lanes(
            [1],
            workers=1,
            runtime=BackfillProcessLaneBootstrap(
                entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
                payload={
                    "events_path": str(events_path),
                    "mode": "bootstrap_descendant_hang",
                    "descendant_pid_path": str(descendant_pid_path),
                },
            ),
            claim_chunk=lambda worker, chunk, capture: claims.append(chunk) or _dispatch(worker, chunk, capture),
            complete_chunk=lambda _dispatch, _result: True,
            fail_chunk=lambda _dispatch, _error: None,
            heartbeat_chunk=lambda _dispatch: True,
            heartbeat_interval_seconds=0.05,
            startup_timeout_seconds=2.0,
            shutdown_timeout_seconds=0.2,
        )

        descendant_pid = int(descendant_pid_path.read_text(encoding="utf-8"))
        assert claims == []
        assert not _pid_is_running(descendant_pid)
        assert summary.lanes_started == 1
        assert summary.lanes[0].process_group_id == summary.lanes[0].pid
        assert any("timeout=true" in error for error in summary.errors)
    finally:
        if descendant_pid is not None:
            with suppress(ProcessLookupError):
                os.kill(descendant_pid, signal.SIGKILL)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups are required")
def test_bootstrap_exit_after_fast_opener_cleans_descendant_before_claim(tmp_path: Path) -> None:
    """The opener cannot create native work before containment is acknowledged."""

    events_path = tmp_path / "events.jsonl"
    descendant_pid_path = tmp_path / "descendant.pid"
    claims: list[int] = []
    descendant_pid: int | None = None

    try:
        summary = run_process_chunk_lanes(
            [1],
            workers=1,
            runtime=BackfillProcessLaneBootstrap(
                entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
                payload={
                    "events_path": str(events_path),
                    "mode": "bootstrap_descendant_exit",
                    "descendant_pid_path": str(descendant_pid_path),
                },
            ),
            claim_chunk=lambda worker, chunk, capture: claims.append(chunk) or _dispatch(worker, chunk, capture),
            complete_chunk=lambda _dispatch, _result: True,
            fail_chunk=lambda _dispatch, _error: None,
            heartbeat_chunk=lambda _dispatch: True,
            heartbeat_interval_seconds=0.05,
            startup_timeout_seconds=2.0,
            shutdown_timeout_seconds=0.2,
        )

        descendant_pid = int(descendant_pid_path.read_text(encoding="utf-8"))
        assert claims == []
        assert not _pid_is_running(descendant_pid)
        assert summary.lanes_started == 1
        assert summary.lanes[0].process_group_id == summary.lanes[0].pid
        assert any("exitcode=91" in error for error in summary.errors)
    finally:
        if descendant_pid is not None:
            with suppress(ProcessLookupError):
                os.kill(descendant_pid, signal.SIGKILL)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process sentinels are required")
def test_dead_idle_lane_is_rejected_before_redispatch_claim(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    marker_path = tmp_path / "exit-marker"
    done_path = tmp_path / "exit-done"
    claims: list[int] = []
    completed: list[int] = []

    def complete(dispatch: ProcessLaneDispatch, _result: object) -> bool:
        completed.append(dispatch.binding.chunk_index)
        marker_path.write_text("exit", encoding="utf-8")
        deadline = time.monotonic() + 2
        while not done_path.exists() and time.monotonic() < deadline:
            time.sleep(0.001)
        assert done_path.exists()
        time.sleep(0.05)
        return True

    summary = run_process_chunk_lanes(
        [1, 2],
        workers=1,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={
                "events_path": str(events_path),
                "mode": "exit_after_success_delivery",
                "exit_marker_path": str(marker_path),
                "exit_done_path": str(done_path),
            },
        ),
        claim_chunk=lambda worker, chunk, capture: claims.append(chunk) or _dispatch(worker, chunk, capture),
        complete_chunk=complete,
        fail_chunk=lambda _dispatch, _error: None,
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
        shutdown_timeout_seconds=0.2,
    )

    assert claims == [1]
    assert completed == [1]
    assert any("DPONE_BACKFILL_PROCESS_LANE_NATIVE_EXIT" in error for error in summary.errors)


def test_operation_lease_is_parent_renewed_without_child_threads(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    renewals: list[tuple[int, bytes, datetime]] = []

    def renew(operation: MssqlTransactionOperation, expires_at: datetime) -> bool:
        renewals.append((operation.epoch, operation.owner_digest, expires_at))
        return True

    summary = run_process_chunk_lanes(
        [1],
        workers=1,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={"events_path": str(events_path), "mode": "operation_lease"},
        ),
        claim_chunk=_dispatch,
        complete_chunk=lambda _dispatch, _result: True,
        fail_chunk=lambda _dispatch, _error: None,
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
        renew_operation_lease=renew,
        validate_operation_binding=lambda _dispatch, _operation, _binding: True,
    )

    finished = [event for event in _events(events_path) if event["event"] == "chunk_finished"]
    assert summary.errors == ()
    assert len(renewals) >= 1
    assert {(epoch, owner) for epoch, owner, _expiry in renewals} == {(7, operation_owner_digest("owner-1"))}
    assert [event["active_threads"] for event in finished] == [1]


def test_exact_receipt_replay_requires_parent_ack_and_validation_before_completion(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    completed: list[int] = []
    validated: list[tuple[str, str]] = []

    def validate(_dispatch, attempt, _operation, result) -> bool:
        validated.append((attempt.load_id, str(result["commit_receipt_id"])))
        return True

    summary = run_process_chunk_lanes(
        [1],
        workers=1,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={"events_path": str(events_path), "mode": "receipt_replay"},
        ),
        claim_chunk=_dispatch,
        complete_chunk=lambda dispatch, _result: completed.append(dispatch.binding.chunk_index) is None,
        fail_chunk=lambda _dispatch, error: pytest.fail(error),
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
        issue_receipt_probe=_receipt_authority,
        validate_replay_result=validate,
    )

    assert summary.errors == ()
    assert validated == [("load-a", "receipt-a")]
    assert completed == [1]


def test_unvalidated_receipt_replay_cannot_mark_parent_ledger_success(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    completed: list[int] = []
    failed: list[tuple[int, str]] = []

    summary = run_process_chunk_lanes(
        [1],
        workers=1,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={"events_path": str(events_path), "mode": "receipt_replay"},
        ),
        claim_chunk=_dispatch,
        complete_chunk=lambda dispatch, _result: completed.append(dispatch.binding.chunk_index) is None,
        fail_chunk=lambda dispatch, error: failed.append((dispatch.binding.chunk_index, error)),
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
        issue_receipt_probe=_receipt_authority,
        validate_replay_result=lambda *_args: False,
        shutdown_timeout_seconds=0.15,
    )

    assert completed == []
    assert failed == [(1, "DPONE_BACKFILL_PROCESS_PARENT_RECEIPT_VALIDATION_FAILED")]
    assert summary.errors == ("DPONE_BACKFILL_PROCESS_PARENT_RECEIPT_VALIDATION_FAILED",)


def test_legacy_two_argument_operation_validator_survives_spawned_claim(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    validated_chunks: list[int] = []

    def legacy_validator(dispatch: ProcessLaneDispatch, _operation: object) -> bool:
        validated_chunks.append(dispatch.binding.chunk_index)
        return True

    summary = run_process_chunk_lanes(
        [1],
        workers=1,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={"events_path": str(events_path), "mode": "operation_lease"},
        ),
        claim_chunk=_dispatch,
        complete_chunk=lambda _dispatch, _result: True,
        fail_chunk=lambda _dispatch, _error: None,
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
        renew_operation_lease=lambda _operation, _expiry: True,
        validate_operation_binding=legacy_validator,
    )

    assert summary.errors == ()
    assert validated_chunks == [1]


def test_invalid_operation_validator_is_rejected_before_chunk_claim(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    claims: list[int] = []

    with pytest.raises(ValueError, match="backfill.process_lane_operation_binding_validator_invalid"):
        run_process_chunk_lanes(
            [1],
            workers=1,
            runtime=BackfillProcessLaneBootstrap(
                entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
                payload={"events_path": str(events_path), "mode": "operation_lease"},
            ),
            claim_chunk=lambda worker, chunk, capture: claims.append(chunk) or _dispatch(worker, chunk, capture),
            complete_chunk=lambda _dispatch, _result: True,
            fail_chunk=lambda _dispatch, _error: None,
            heartbeat_chunk=lambda _dispatch: True,
            heartbeat_interval_seconds=0.05,
            renew_operation_lease=lambda _operation, _expiry: True,
            validate_operation_binding=lambda _dispatch: True,
        )

    assert claims == []
    assert not events_path.exists()


def test_legacy_operation_validator_requires_exact_parent_portable_binding() -> None:
    scope = parse_portable_relation_scope(
        {
            "column": "id",
            "kind": "equality",
            "value": {"type": "integer", "value": 1},
            "version": 1,
        }
    )
    binding = bind_portable_scope(
        scope,
        PortableScopeColumnContract("id", "integer", None, "id", "int", None),
    )
    load_config = SimpleNamespace(
        portable_scope=scope,
        options={PORTABLE_SCOPE_BINDING_OPTION: binding},
    )
    dispatch = replace(_dispatch(0, 1), load_config=load_config)
    calls: list[object] = []

    def legacy_validator(_dispatch: ProcessLaneDispatch, operation: object) -> bool:
        calls.append(operation)
        return True

    adapted = adapt_operation_binding_validator(legacy_validator)
    assert adapted is not None
    operation = object()

    assert adapted(dispatch, operation, binding) is True
    assert adapted(dispatch, operation, None) is False
    assert calls == [operation]

    def primary(_dispatch, _operation, _binding):
        return True

    assert adapt_operation_binding_validator(primary) is primary


def test_receipt_is_recovered_when_lane_crashes_after_unregister_before_success(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    receipt_path = tmp_path / "receipt"
    unregistered_path = tmp_path / "unregistered"
    recovered: list[tuple[ProcessLaneDispatch, MssqlTransactionOperation]] = []
    failed: list[tuple[int, str]] = []

    summary = run_process_chunk_lanes(
        [1, 2],
        workers=1,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={
                "events_path": str(events_path),
                "mode": "operation_unregister_crash",
                "receipt_path": str(receipt_path),
                "unregistered_path": str(unregistered_path),
            },
        ),
        claim_chunk=_dispatch,
        complete_chunk=lambda _dispatch, _result: True,
        fail_chunk=lambda dispatch, error: failed.append((dispatch.parent_context, error)),
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
        renew_operation_lease=lambda _operation, _expiry: True,
        validate_operation_binding=lambda _dispatch, _operation, _binding: True,
        recover_committed_receipt=lambda dispatch, operation: recovered.append((dispatch, operation)) is None,
    )

    assert receipt_path.read_text(encoding="utf-8") == "committed"
    assert unregistered_path.read_text(encoding="utf-8") == "true"
    assert failed == []
    assert len(recovered) == 1
    dispatch, operation = recovered[0]
    assert dispatch.binding == ProcessLaneBinding("campaign-a", 1, "owner-1")
    assert operation.owner_digest == operation_owner_digest("owner-1")
    assert not any(event.get("chunk") == 2 for event in _events(events_path))
    assert any("signal=SIGSEGV" in error for error in summary.errors)


def test_native_exit_bounds_and_terminates_a_hung_peer_lane(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    crash_marker_path = tmp_path / "native-crash.monotonic"
    failed: list[int] = []

    summary = run_process_chunk_lanes(
        [1, 2, 3],
        workers=2,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={
                "events_path": str(events_path),
                "mode": "native_crash_hung_peer",
                "crash_marker_path": str(crash_marker_path),
            },
        ),
        claim_chunk=_dispatch,
        complete_chunk=lambda _dispatch, _result: True,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.binding.chunk_index),
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
        shutdown_timeout_seconds=0.15,
    )

    # Bound the failure reaction itself. Process-spawn/import time is outside
    # the shutdown contract and varies materially under the sixteen-way CI
    # shard fan-out; the child marker is flushed immediately before the native
    # exit. Five seconds still proves prompt peer termination without making
    # the assertion flaky under saturated hosted runners.
    crash_observed_at = float(crash_marker_path.read_text(encoding="utf-8"))
    assert time.monotonic() - crash_observed_at < 5
    assert sorted(failed) == [1, 2]
    assert not any(event.get("chunk") == 3 for event in _events(events_path))
    assert any("DPONE_BACKFILL_PROCESS_PEER_DRAIN_TIMEOUT" in error for error in summary.errors)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups are required")
def test_native_exit_terminates_the_entire_peer_process_group(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    descendant_pid_path = tmp_path / "descendant.pid"
    descendant_pid: int | None = None

    try:
        summary = run_process_chunk_lanes(
            [1, 2, 3],
            workers=2,
            runtime=BackfillProcessLaneBootstrap(
                entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
                payload={
                    "events_path": str(events_path),
                    "mode": "native_crash_descendant_peer",
                    "descendant_pid_path": str(descendant_pid_path),
                },
            ),
            claim_chunk=_dispatch,
            complete_chunk=lambda _dispatch, _result: True,
            fail_chunk=lambda _dispatch, _error: None,
            heartbeat_chunk=lambda _dispatch: True,
            heartbeat_interval_seconds=0.05,
            shutdown_timeout_seconds=0.2,
        )
        assert descendant_pid_path.is_file(), summary.errors
        descendant_pid = int(descendant_pid_path.read_text(encoding="utf-8"))
        assert not _pid_is_running(descendant_pid)
        assert summary.lanes
        assert all(lane.process_group_id == lane.pid for lane in summary.lanes)
        assert all(lane.exitcode is not None for lane in summary.lanes)
    finally:
        if descendant_pid is not None:
            with suppress(ProcessLookupError):
                os.kill(descendant_pid, signal.SIGKILL)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups are required")
def test_native_exit_cleans_descendants_before_receipt_probe(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    descendant_pid_path = tmp_path / "descendant.pid"
    recovery_observations: list[bool] = []
    descendant_pid: int | None = None

    def recover(_dispatch: ProcessLaneDispatch, _operation: MssqlTransactionOperation) -> bool:
        nonlocal descendant_pid
        descendant_pid = int(descendant_pid_path.read_text(encoding="utf-8"))
        recovery_observations.append(_pid_is_running(descendant_pid))
        return False

    try:
        summary = run_process_chunk_lanes(
            [1],
            workers=1,
            runtime=BackfillProcessLaneBootstrap(
                entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
                payload={
                    "events_path": str(events_path),
                    "mode": "native_crash_with_descendant",
                    "descendant_pid_path": str(descendant_pid_path),
                },
            ),
            claim_chunk=_dispatch,
            complete_chunk=lambda _dispatch, _result: True,
            fail_chunk=lambda _dispatch, _error: None,
            heartbeat_chunk=lambda _dispatch: True,
            heartbeat_interval_seconds=0.05,
            renew_operation_lease=lambda _operation, _expiry: True,
            validate_operation_binding=lambda _dispatch, _operation: True,
            recover_committed_receipt=recover,
            shutdown_timeout_seconds=0.2,
        )

        assert recovery_observations == [False]
        assert any("signal=SIGSEGV" in error for error in summary.errors)
    finally:
        if descendant_pid is not None:
            with suppress(ProcessLookupError):
                os.kill(descendant_pid, signal.SIGKILL)


def test_corrupt_child_frame_runs_receipt_recovery_instead_of_escaping(tmp_path: Path) -> None:
    events_path = tmp_path / "events.jsonl"
    receipt_path = tmp_path / "receipt"
    recovered: list[tuple[ProcessLaneDispatch, MssqlTransactionOperation]] = []
    failed: list[int] = []

    summary = run_process_chunk_lanes(
        [1],
        workers=1,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={
                "events_path": str(events_path),
                "mode": "malformed_frame_receipt",
                "receipt_path": str(receipt_path),
            },
        ),
        claim_chunk=_dispatch,
        complete_chunk=lambda _dispatch, _result: True,
        fail_chunk=lambda dispatch, _error: failed.append(dispatch.binding.chunk_index),
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
        renew_operation_lease=lambda _operation, _expiry: True,
        validate_operation_binding=lambda _dispatch, _operation, _binding: True,
        recover_committed_receipt=lambda dispatch, operation: recovered.append((dispatch, operation)) is None,
        shutdown_timeout_seconds=0.15,
    )

    assert receipt_path.read_text(encoding="utf-8") == "committed"
    assert failed == []
    assert len(recovered) == 1
    assert recovered[0][0].binding.chunk_index == 1
    assert any(error == "DPONE_BACKFILL_PROCESS_CHANNEL_RECEIVE_FAILED" for error in summary.errors)


def test_mssql_process_path_has_no_python_thread_executor() -> None:
    modules = (
        process_lane_operation,
        process_lane_parent,
        process_lane_processes,
        process_lane_worker,
    )

    for module in modules:
        source = inspect.getsource(module)
        assert "ThreadPoolExecutor" not in source
        assert "threading.Thread" not in source
        assert "from threading import Thread" not in source


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups are required")
def test_ready_identity_never_accepts_the_parent_process_group() -> None:
    parent_group = os.getpgrp()
    lane = SimpleNamespace(process=SimpleNamespace(pid=parent_group))
    ready = SimpleNamespace(pid=parent_group, process_group_id=parent_group)

    assert not ready_identity_is_valid(lane, ready)


def test_nonserializable_process_bootstrap_fails_before_chunk_claim() -> None:
    claims: list[int] = []

    summary = run_process_chunk_lanes(
        [1],
        workers=1,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={"not_serializable": lambda: None},
        ),
        claim_chunk=lambda worker, chunk, capture: claims.append(chunk) or _dispatch(worker, chunk, capture),
        complete_chunk=lambda _dispatch, _result: True,
        fail_chunk=lambda _dispatch, _error: None,
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
    )

    assert claims == []
    assert summary.lanes_started == 0
    assert summary.errors == ("DPONE_BACKFILL_PROCESS_BOOTSTRAP_NOT_SERIALIZABLE",)


def test_unavailable_process_bootstrap_fails_before_chunk_claim() -> None:
    claims: list[int] = []

    summary = run_process_chunk_lanes(
        [1],
        workers=1,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="dpone.does_not_exist:open_lane",
            payload={},
        ),
        claim_chunk=lambda worker, chunk, capture: claims.append(chunk) or _dispatch(worker, chunk, capture),
        complete_chunk=lambda _dispatch, _result: True,
        fail_chunk=lambda _dispatch, _error: None,
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
    )

    assert claims == []
    assert summary.lanes_started == 1
    assert summary.lanes[0].process_group_id in (None, summary.lanes[0].pid)
    assert len(summary.errors) == 1
    assert summary.errors[0].startswith("DPONE_BACKFILL_PROCESS_BOOTSTRAP_FAILED")


def test_initial_claim_failure_stops_later_claims_and_reports_redacted_stage(tmp_path: Path) -> None:
    claims: list[int] = []

    def claim(worker_id: int, chunk: int, capture) -> None:
        claims.append(chunk)
        if chunk == 2:
            raise RuntimeError("claim rejected")
        _dispatch(worker_id, chunk, capture)

    summary = run_process_chunk_lanes(
        [1, 2, 3, 4],
        workers=4,
        runtime=BackfillProcessLaneBootstrap(
            entrypoint="tests.test_backfill_parallel_execution:_open_test_process_lane",
            payload={"events_path": str(tmp_path / "events.jsonl")},
        ),
        claim_chunk=claim,
        complete_chunk=lambda _dispatch, _result: True,
        fail_chunk=lambda _dispatch, _error: None,
        heartbeat_chunk=lambda _dispatch: True,
        heartbeat_interval_seconds=0.05,
    )

    assert claims == [1, 2]
    assert len(summary.errors) == 1
    assert summary.errors[0].startswith("DPONE_BACKFILL_PROCESS_CHUNK_DISPATCH_FAILED")
    assert "stage=claim" in summary.errors[0]


def test_child_operation_request_times_out_before_lease_expiry() -> None:
    context = get_context("spawn")
    parent, child = context.Pipe(duplex=True)
    expiry = datetime.now(UTC) + timedelta(seconds=0.3)
    operation = _operation(expires_at=expiry)
    factory = _OperationLeaseFactory(0, child)
    command = ProcessLaneRun("chunk-1", 1, _dispatch(0, 1).binding)
    factory.bind(command)
    controller = _OperationLeaseController(factory, operation)

    started = time.monotonic()
    with pytest.raises(ProcessLaneOperationLeaseError):
        controller.start()
    elapsed = time.monotonic() - started

    assert elapsed < 0.2
    assert datetime.now(UTC) < expiry
    factory.release(command)
    parent.close()
    child.close()


def test_child_operation_controller_start_is_idempotent_until_stop() -> None:
    """Admission and processor handoff must emit one register/unregister pair."""

    context = get_context("spawn")
    parent, child = context.Pipe(duplex=True)
    operation = _operation(expires_at=datetime.now(UTC) + timedelta(seconds=1))
    factory = _OperationLeaseFactory(0, child)
    command = ProcessLaneRun("chunk-1", 1, _dispatch(0, 1).binding)
    factory.bind(command)
    controller = _OperationLeaseController(factory, operation)
    actions: list[str] = []

    def acknowledge_lifecycle() -> None:
        for expected_action in ("register", "unregister"):
            request = parent.recv()
            assert isinstance(request, ProcessLaneOperationLeaseRequest)
            actions.append(request.action)
            assert request.action == expected_action
            parent.send(
                ProcessLaneOperationLeaseReply(
                    request_id=request.request_id,
                    worker_id=request.worker_id,
                    command_id=request.command_id,
                    active=True,
                )
            )

    parent_thread = Thread(target=acknowledge_lifecycle)
    parent_thread.start()
    controller.start()
    controller.start()
    controller.stop()

    parent_thread.join(timeout=1)
    assert not parent_thread.is_alive()
    assert actions == ["register", "unregister"]
    factory.release(command)
    parent.close()
    child.close()


def test_child_can_revalidate_a_parent_renewed_lease_after_local_expiry() -> None:
    context = get_context("spawn")
    parent, child = context.Pipe(duplex=True)
    operation = _operation(expires_at=datetime.now(UTC) + timedelta(milliseconds=50))
    factory = _OperationLeaseFactory(0, child)
    command = ProcessLaneRun("chunk-1", 1, _dispatch(0, 1).binding)
    factory.bind(command)
    controller = _OperationLeaseController(factory, operation)
    controller._started = True
    time.sleep(0.06)

    assert controller._response_timeout("status") > 0

    factory.release(command)
    parent.close()
    child.close()


def test_child_parent_response_budget_tracks_the_process_lane_renew_interval() -> None:
    expiry = datetime.now(UTC) + timedelta(seconds=90)

    timeout = _response_timeout(expiry)

    assert 29.0 <= timeout <= 30.0


def test_late_timed_out_operation_reply_cannot_poison_the_next_command() -> None:
    context = get_context("spawn")
    parent, child = context.Pipe(duplex=True)
    factory = _OperationLeaseFactory(0, child)
    first = ProcessLaneRun("chunk-1", 1, _dispatch(0, 1).binding)
    second = ProcessLaneRun("chunk-2", 2, _dispatch(0, 2).binding)
    factory.bind(first)
    controller = _OperationLeaseController(
        factory,
        _operation(expires_at=datetime.now(UTC) + timedelta(milliseconds=150)),
    )

    def delayed_parent() -> None:
        request = parent.recv()
        assert isinstance(request, ProcessLaneOperationLeaseRequest)
        time.sleep(0.12)
        parent.send(
            ProcessLaneOperationLeaseReply(
                request_id=request.request_id,
                worker_id=request.worker_id,
                command_id=request.command_id,
                active=True,
            )
        )
        parent.send(second)

    parent_thread = Thread(target=delayed_parent)
    parent_thread.start()
    with pytest.raises(ProcessLaneOperationLeaseError):
        controller.start()
    factory.release(first)

    assert factory.receive_command() == second

    parent_thread.join(timeout=1)
    assert not parent_thread.is_alive()
    parent.close()
    child.close()


def test_duplicate_operation_request_cannot_rebind_another_command() -> None:
    context = get_context("spawn")
    parent, child = context.Pipe(duplex=True)
    dispatch = _dispatch(0, 1)
    operation = _operation(expires_at=_command_expiry(dispatch.load_config))
    request = ProcessLaneOperationLeaseRequest(
        request_id="request-1",
        action="register",
        worker_id=0,
        command_id=dispatch.command_id,
        binding=dispatch.binding,
        operation_key=operation.operation_key,
        owner_digest=operation.owner_digest,
        epoch=operation.epoch,
        operation=operation,
    )
    coordinator = ParentOperationLeaseCoordinator(
        renew=lambda _operation, _expiry: True,
        validate_binding=lambda _dispatch, _operation, _binding: True,
    )

    assert coordinator.handle(parent, request, worker_id=0, dispatch=dispatch, control_error=None)
    registered_reply = child.recv()
    assert isinstance(registered_reply, ProcessLaneOperationLeaseReply)
    assert registered_reply.active is True

    duplicate = replace(request, action="status", operation=None)
    assert not coordinator.handle(parent, duplicate, worker_id=0, dispatch=dispatch, control_error=None)
    duplicate_reply = child.recv()
    assert isinstance(duplicate_reply, ProcessLaneOperationLeaseReply)
    assert duplicate_reply.active is False
    assert duplicate_reply.error == "DPONE_BACKFILL_PROCESS_OPERATION_LEASE_IDENTITY_MISMATCH"

    other_dispatch = _dispatch(0, 2)
    late = replace(
        duplicate,
        request_id="request-2",
        command_id=other_dispatch.command_id,
        binding=dispatch.binding,
    )
    assert not coordinator.handle(parent, late, worker_id=0, dispatch=other_dispatch, control_error=None)
    late_reply = child.recv()
    assert isinstance(late_reply, ProcessLaneOperationLeaseReply)
    assert late_reply.active is False
    assert coordinator.operation_for(0, dispatch) is operation
    assert coordinator.operation_for(0, other_dispatch) is None
    parent.close()
    child.close()


def test_failed_operation_renewal_is_latched_after_one_parent_attempt() -> None:
    dispatch = _dispatch(0, 1)
    operation = _operation(expires_at=_command_expiry(dispatch.load_config))
    renewals = 0

    def renew(_operation: object, _expiry: datetime) -> bool:
        nonlocal renewals
        renewals += 1
        if renewals > 1:
            raise RuntimeError("state unavailable")
        return True

    coordinator = ParentOperationLeaseCoordinator(
        renew=renew,
        validate_binding=lambda *_args: True,
    )
    parent, child = get_context("spawn").Pipe(duplex=True)
    request = ProcessLaneOperationLeaseRequest(
        request_id="register-1",
        action="register",
        worker_id=0,
        command_id=dispatch.command_id,
        binding=dispatch.binding,
        operation_key=operation.operation_key,
        owner_digest=operation.owner_digest,
        epoch=operation.epoch,
        operation=operation,
    )
    assert coordinator.handle(parent, request, worker_id=0, dispatch=dispatch, control_error=None)
    assert child.recv().active is True
    key = (0, dispatch.command_id)
    coordinator._operations[key].next_renewal = 0  # noqa: SLF001 - exact scheduler boundary contract.

    status_reply: list[ProcessLaneOperationLeaseReply] = []
    latched: list[tuple[int, str]] = []

    def service_status() -> bool:
        assert latched == [key]
        status = replace(request, request_id="status-after-renew-failure", action="status", operation=None)
        assert not coordinator.handle(parent, status, worker_id=0, dispatch=dispatch, control_error=None)
        reply = child.recv()
        assert isinstance(reply, ProcessLaneOperationLeaseReply)
        status_reply.append(reply)
        return False

    assert coordinator.tick(
        {key: dispatch},
        on_failure=latched.append,
        after_each=service_status,
    ) == {key}
    assert coordinator.tick({key: dispatch}, on_failure=pytest.fail) == set()
    assert renewals == 2
    assert coordinator._operations[key].renewing is False  # noqa: SLF001
    assert len(status_reply) == 1
    assert status_reply[0].active is False
    assert status_reply[0].error == "DPONE_BACKFILL_PROCESS_OPERATION_LEASE_LOST"
    parent.close()
    child.close()


def test_failed_renewal_latches_lane_before_co_ready_success_is_drained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatch = _dispatch(0, 1)
    operation = _operation(expires_at=_command_expiry(dispatch.load_config))
    renewals = 0

    def renew(_operation: object, _expiry: datetime) -> bool:
        nonlocal renewals
        renewals += 1
        if renewals > 1:
            raise RuntimeError("state unavailable")
        return True

    coordinator = ParentOperationLeaseCoordinator(
        renew=renew,
        validate_binding=lambda *_args: True,
    )
    parent, child = get_context("spawn").Pipe(duplex=True)
    request = ProcessLaneOperationLeaseRequest(
        request_id="register-racing-success",
        action="register",
        worker_id=0,
        command_id=dispatch.command_id,
        binding=dispatch.binding,
        operation_key=operation.operation_key,
        owner_digest=operation.owner_digest,
        epoch=operation.epoch,
        operation=operation,
    )
    assert coordinator.handle(parent, request, worker_id=0, dispatch=dispatch, control_error=None)
    assert child.recv().active is True
    key = (0, dispatch.command_id)
    coordinator._operations[key].next_renewal = 0  # noqa: SLF001 - exact scheduler race boundary.
    child.send(ProcessLaneSucceeded(0, dispatch.command_id, {"status": "success"}))

    lane = SimpleNamespace(
        worker_id=0,
        connection=parent,
        running=dispatch,
        control_error=None,
    )
    completed: list[int] = []
    recovered: list[MssqlTransactionOperation] = []
    failed: list[tuple[int, str]] = []
    monkeypatch.setattr(
        "dpone.backfill.process_lane_messages.quiesce_lane_process_tree",
        lambda _lane, *, timeout_seconds: timeout_seconds > 0,
    )

    def latch(failed_key: tuple[int, str]) -> None:
        assert failed_key == key
        lane.control_error = "DPONE_BACKFILL_PROCESS_OPERATION_LEASE_LOST"

    def drain_success() -> bool:
        assert lane.control_error == "DPONE_BACKFILL_PROCESS_OPERATION_LEASE_LOST"

        def recover(_dispatch: ProcessLaneDispatch, owned: MssqlTransactionOperation) -> bool:
            recovered.append(owned)
            return False

        return handle_lane_message(
            lane,
            complete_chunk=lambda _dispatch, _result: completed.append(1) is None,
            fail_chunk=lambda owned, error: failed.append((owned.binding.chunk_index, error)),
            operation_coordinator=coordinator,
            receipt_coordinator=ParentReceiptReplayCoordinator(issue_probe=None, validate_result=None),
            recover_committed_receipt=recover,
            errors=[],
            stop_dispatch=False,
            quiesce_timeout_seconds=0.1,
        )

    assert coordinator.tick(
        {key: dispatch},
        on_failure=latch,
        after_each=drain_success,
    ) == {key}

    assert completed == []
    assert recovered == [operation]
    assert failed == [(1, "DPONE_BACKFILL_PROCESS_OPERATION_LEASE_LOST")]
    assert lane.running is None
    parent.close()
    child.close()


def test_receipt_committed_during_renewal_keeps_terminal_status_authorized() -> None:
    dispatch = _dispatch(0, 1)
    operation = _operation(expires_at=_command_expiry(dispatch.load_config))
    renewals = 0

    def renew(_operation: object, _expiry: datetime) -> bool:
        nonlocal renewals
        renewals += 1
        return renewals == 1

    coordinator = ParentOperationLeaseCoordinator(
        renew=renew,
        validate_binding=lambda *_args: True,
    )
    parent, child = get_context("spawn").Pipe(duplex=True)
    register = ProcessLaneOperationLeaseRequest(
        request_id="register-receipt",
        action="register",
        worker_id=0,
        command_id=dispatch.command_id,
        binding=dispatch.binding,
        operation_key=operation.operation_key,
        owner_digest=operation.owner_digest,
        epoch=operation.epoch,
        operation=operation,
    )
    assert coordinator.handle(parent, register, worker_id=0, dispatch=dispatch, control_error=None)
    assert child.recv().active is True
    key = (0, dispatch.command_id)
    coordinator._operations[key].next_renewal = 0  # noqa: SLF001 - exact receipt boundary.

    assert coordinator.tick({key: dispatch}, on_failure=pytest.fail) == set()
    registered = coordinator._operations[key]  # noqa: SLF001 - exact receipt boundary.
    assert registered.receipt_committed is True
    assert registered.renewing is False

    status = replace(register, request_id="status-after-receipt", action="status", operation=None)
    assert coordinator.handle(parent, status, worker_id=0, dispatch=dispatch, control_error=None)
    assert child.recv().active is True
    assert coordinator.handle(parent, status, worker_id=0, dispatch=dispatch, control_error=None)
    assert child.recv().active is True

    unregister = replace(status, request_id="unregister-after-receipt", action="unregister")
    assert coordinator.handle(parent, unregister, worker_id=0, dispatch=dispatch, control_error=None)
    assert child.recv().active is True
    assert renewals == 2
    parent.close()
    child.close()


def test_due_operation_renewals_yield_to_peer_status_ipc_between_sql_calls() -> None:
    events: list[str] = []
    cases: list[tuple[ProcessLaneDispatch, ProcessLaneOperationLeaseRequest, Connection, Connection]] = []

    def renew(operation: MssqlTransactionOperation, _expiry: datetime) -> bool:
        events.append(f"renew-{operation.epoch}")
        return True

    coordinator = ParentOperationLeaseCoordinator(
        renew=renew,
        validate_binding=lambda *_args: True,
    )
    for worker_id in range(4):
        chunk = worker_id + 1
        expiry = datetime.now(UTC) + timedelta(minutes=2)
        dispatch = replace(_dispatch(worker_id, chunk), load_config=_ChunkCommand(chunk, expiry))
        operation = replace(
            _operation(expires_at=expiry),
            operation_key=bytes([chunk]) * 32,
            owner_digest=operation_owner_digest(f"owner-{chunk}"),
            epoch=chunk,
        )
        request = ProcessLaneOperationLeaseRequest(
            request_id=f"register-{chunk}",
            action="register",
            worker_id=worker_id,
            command_id=dispatch.command_id,
            binding=dispatch.binding,
            operation_key=operation.operation_key,
            owner_digest=operation.owner_digest,
            epoch=operation.epoch,
            operation=operation,
        )
        parent, child = get_context("spawn").Pipe(duplex=True)
        assert coordinator.handle(parent, request, worker_id=worker_id, dispatch=dispatch, control_error=None)
        assert child.recv().active is True
        coordinator._operations[(worker_id, dispatch.command_id)].next_renewal = 0  # noqa: SLF001
        cases.append((dispatch, request, parent, child))
    events.clear()
    serviced = False

    def service_peer() -> bool:
        nonlocal serviced
        if serviced:
            return False
        serviced = True
        dispatch, registration, parent, child = cases[-1]
        status = replace(
            registration,
            request_id="status-4",
            action="status",
            operation=None,
        )
        assert coordinator.handle(parent, status, worker_id=3, dispatch=dispatch, control_error=None)
        assert child.recv().active is True
        events.append("status-4")
        return False

    running = {
        (worker_id, dispatch.command_id): dispatch
        for worker_id, (dispatch, _request, _parent, _child) in enumerate(cases)
    }
    assert (
        coordinator.tick(
            running,
            on_failure=pytest.fail,
            after_each=service_peer,
        )
        == set()
    )

    assert events[:2] == ["renew-1", "status-4"]
    assert events[2:] == ["renew-2", "renew-3", "renew-4"]
    for _dispatch_case, _request, parent, child in cases:
        parent.close()
        child.close()
