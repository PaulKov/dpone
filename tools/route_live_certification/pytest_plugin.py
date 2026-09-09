"""Pytest fixture for closed PostgreSQL→MSSQL route-live evidence."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from .recorder import RouteLiveObservationRecorder

_PROGRESS_PATH_ENV = "DPONE_ROUTE_LIVE_PROGRESS_JSONL"
_active_progress_path: Path | None = None


def pytest_sessionstart(session: Any) -> None:
    """Open a crash-durable diagnostic journal only when CI opts in."""

    global _active_progress_path
    _active_progress_path = None
    config = getattr(session, "config", None)
    if getattr(config, "workerinput", None) is not None:
        return
    raw_path = os.environ.get(_PROGRESS_PATH_ENV, "").strip()
    if not raw_path:
        return
    path = Path(raw_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")
    _active_progress_path = path
    _append_progress("session_started")


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    """Record the terminal pytest status without promoting diagnostics to evidence."""

    del session
    global _active_progress_path
    _append_progress("session_finished", exit_status=int(exitstatus))
    _active_progress_path = None


def pytest_runtest_logstart(nodeid: str, location: tuple[str, int | None, str]) -> None:
    """Persist the exact node boundary before setup starts."""

    del location
    _append_progress("started", nodeid=nodeid)


def pytest_runtest_logreport(report: Any) -> None:
    """Persist each completed phase so hard-kill diagnostics remain actionable."""

    _append_progress(
        "phase",
        nodeid=str(report.nodeid),
        phase=str(report.when),
        outcome=str(report.outcome),
        duration_seconds=round(float(report.duration), 6),
    )


def pytest_runtest_logfinish(nodeid: str, location: tuple[str, int | None, str]) -> None:
    """Persist the boundary reached only after teardown completes."""

    del location
    _append_progress("finished", nodeid=nodeid)


def record_route_live_subcase_started(*, suite_id: str, case_id: str, ordinal: int) -> None:
    """Persist an inner reviewed-case boundary before its first vendor operation.

    Some release suites intentionally execute thousands of reviewed cells inside
    one pytest item so they can publish one closed campaign.  A native-process
    crash cannot run pytest teardown or JUnit serialization, therefore the
    ordinary pytest node boundary is not precise enough to identify the active
    cell.  This journal entry is diagnostic-only and is fsynced by
    :func:`_append_progress`; it never contributes to certification authority.
    """

    _append_progress(
        "subcase_started",
        suite_id=suite_id,
        case_id=case_id,
        ordinal=int(ordinal),
    )


def record_route_live_subcase_finished(*, suite_id: str, case_id: str, ordinal: int) -> None:
    """Persist the boundary reached after one reviewed cell and cleanup finish."""

    _append_progress(
        "subcase_finished",
        suite_id=suite_id,
        case_id=case_id,
        ordinal=int(ordinal),
    )


def _append_progress(event: str, **details: Any) -> None:
    path = _active_progress_path
    if path is None:
        return
    payload = {
        "schema": "dpone.route_live.pytest_progress.v1",
        "diagnostic_only": True,
        "release_ready": False,
        "event": event,
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "commit_sha": os.environ.get("GITHUB_SHA"),
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID"),
        "workflow_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        **details,
    }
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(encoded + "\n")
        handle.flush()
        os.fsync(handle.fileno())


@pytest.fixture(scope="session", autouse=True)
def route_live_recorder() -> Iterator[RouteLiveObservationRecorder]:
    """Collect real-vendor observations and publish only on complete teardown."""

    recorder = RouteLiveObservationRecorder(environment=os.environ)
    yield recorder
    recorder.write_complete_authority()


__all__ = [
    "record_route_live_subcase_finished",
    "record_route_live_subcase_started",
    "route_live_recorder",
]
