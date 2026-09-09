"""Deterministic crash-fixture ordering, independent of vendor databases."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from tests.integration.postgres import postgres_mssql_backfill_lifecycle_live_support as support


class SupervisorStopped(BaseException):
    """Model parent-owned termination without sending a signal in unit tests."""


@contextmanager
def fake_production_opener(*args):
    yield lambda config: {"loaded_rows": 1}


def test_receipt_marker_does_not_make_peer_fail_before_native_signal(tmp_path, monkeypatch):
    payload = support.ReviewedNativeCrashLanePayload(
        None, str(tmp_path / "marker"), str(tmp_path / "events"), True, "child_sigsegv"
    )
    (tmp_path / "marker").write_text("receipt exists but SIGSEGV has not been requested")
    waited = []

    def wait_for_supervisor(*args):
        waited.append(True)
        raise SupervisorStopped()

    monkeypatch.setattr(support, "open_backfill_process_lane", fake_production_opener)
    monkeypatch.setattr(support, "_wait_for_native_exit_shutdown", wait_for_supervisor, raising=False)
    with support.open_reviewed_native_crash_process_lane(1, payload, None) as run:
        with pytest.raises(SupervisorStopped):
            run(SimpleNamespace(options={"backfill": {"chunk_context": {"index": 2}}}))
    assert waited == [True]


def test_native_peer_watchdog_remains_bounded(tmp_path, monkeypatch):
    observed = []
    clock = iter([0.0, 0.0, support._PARENT_KILL_DEADLINE_SECONDS])
    monkeypatch.setattr(support.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(support.time, "sleep", lambda delay: observed.append(delay))
    monkeypatch.setattr(support, "_append_native_event", lambda *args: observed.append(args[1]))
    with pytest.raises(RuntimeError, match="bounded deadline"):
        support._wait_for_native_exit_shutdown(str(tmp_path / "events"), 1, 2)
    assert observed == [0.05, "native_exit_shutdown_timeout"]


def test_core_dump_limit_preserves_hard_limit(monkeypatch):
    resource = pytest.importorskip("resource")
    limits = []
    monkeypatch.setattr(resource, "getrlimit", lambda kind: (1024, 4096))
    monkeypatch.setattr(resource, "setrlimit", lambda kind, value: limits.append((kind, value)))
    support._disable_deliberate_crash_core_dump()
    assert limits == [(resource.RLIMIT_CORE, (0, 4096))]


@pytest.mark.parametrize("mode,inject", [("child_sigsegv", True), ("parent_sigkill", True), ("child_sigsegv", False)])
def test_core_dump_change_is_scoped_to_deliberate_native_child(tmp_path, monkeypatch, mode, inject):
    events = []
    payload = support.ReviewedNativeCrashLanePayload(
        None, str(tmp_path / "marker"), str(tmp_path / "events"), inject, mode
    )
    monkeypatch.setattr(support, "open_backfill_process_lane", fake_production_opener)
    monkeypatch.setattr(support, "_disable_deliberate_crash_core_dump", lambda: events.append("core-disabled"))
    monkeypatch.setattr(support.os, "kill", lambda *args: events.append("signal"))

    def parent_killed(*args):
        raise SupervisorStopped()

    monkeypatch.setattr(support, "_wait_for_parent_sigkill", parent_killed)
    with support.open_reviewed_native_crash_process_lane(0, payload, None) as run:
        config = SimpleNamespace(options={"backfill": {"chunk_context": {"index": 1}}})
        if mode == "parent_sigkill" and inject:
            with pytest.raises(SupervisorStopped):
                run(config)
        else:
            run(config)
    assert events == (["core-disabled", "signal"] if mode == "child_sigsegv" and inject else [])


@contextmanager
def open_delayed_native_crash_fixture(worker_id, payload, operation_lease_factory):
    """Spawn entrypoint: production-shaped fixture with controlled signal delay."""
    import time
    from unittest.mock import patch

    reviewed = support.ReviewedNativeCrashLanePayload(None, payload["marker"], payload["events"], True, "child_sigsegv")
    append_event = support._append_native_event

    def delay_after_receipt(path, event, *args, **kwargs):
        append_event(path, event, *args, **kwargs)
        if event == "sigsegv_after_receipt":
            # Intentionally exceed the parent's configured peer-drain timeout.
            time.sleep(0.4)

    with (
        patch.object(support, "open_backfill_process_lane", fake_production_opener),
        patch.object(support, "_append_native_event", delay_after_receipt),
    ):
        with support.open_reviewed_native_crash_process_lane(worker_id, reviewed, operation_lease_factory) as run:
            yield run


def test_delayed_native_signal_is_observed_before_peer_abort(tmp_path):
    import json
    import os
    import signal

    from dpone.backfill.process_lane_contracts import (
        BackfillProcessLaneBootstrap,
        ProcessLaneBinding,
        ProcessLaneDispatch,
    )
    from dpone.backfill.process_lane_parent import run_process_chunk_lanes

    if os.name != "posix":
        pytest.skip("Intentional native crash fixture requires POSIX process groups")
    claims = []

    def claim(worker, chunk, capture):
        claims.append(chunk)
        capture(
            ProcessLaneDispatch(
                f"chunk-{chunk}",
                SimpleNamespace(options={"backfill": {"chunk_context": {"index": chunk}}}),
                ProcessLaneBinding("fixture-ordering", chunk, f"owner-{chunk}"),
                chunk,
            )
        )

    summary = run_process_chunk_lanes(
        [1, 2, 3],
        workers=2,
        runtime=BackfillProcessLaneBootstrap(
            "tests.test_backfill_native_crash_fixture:open_delayed_native_crash_fixture",
            {"marker": str(tmp_path / "marker"), "events": str(tmp_path / "events")},
        ),
        claim_chunk=claim,
        complete_chunk=lambda *_: True,
        fail_chunk=lambda *_: None,
        heartbeat_chunk=lambda *_: True,
        heartbeat_interval_seconds=0.05,
        shutdown_timeout_seconds=0.1,
    )
    native_errors = [error for error in summary.errors if error.startswith("DPONE_BACKFILL_PROCESS_LANE_NATIVE_EXIT")]
    assert len(native_errors) == 1 and "signal=SIGSEGV" in native_errors[0]
    assert "worker_id=0" in native_errors[0]
    assert claims == [1, 2]
    assert any(lane.exitcode == -signal.SIGSEGV for lane in summary.lanes)
    assert not any("reviewed peer stopped" in error for error in summary.errors)
    events = [json.loads(line) for line in (tmp_path / "events").read_text().splitlines()]
    assert [event["chunk_index"] for event in events if event["event"] == "production_result"] == [1]
