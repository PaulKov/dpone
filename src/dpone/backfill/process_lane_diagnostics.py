"""Bounded and redacted diagnostics for spawned backfill process lanes."""

from __future__ import annotations

import signal
from collections.abc import Callable, Iterable
from typing import Any, Protocol, TypeVar

from dpone.backfill.process_lane_contracts import ProcessLaneDiagnostic, ProcessLaneReady
from dpone.security_redaction import redact_public_text

NATIVE_EXIT_ERROR = "DPONE_BACKFILL_PROCESS_LANE_NATIVE_EXIT"


class ProcessLaneDiagnosticSource(Protocol):
    """Structural parent-lane state required by diagnostic projections."""

    worker_id: int
    process: Any
    ready: ProcessLaneReady | None
    final_exitcode: int | None


class ProcessLaneExitSource(ProcessLaneDiagnosticSource, Protocol):
    """Mutable lane state required by receipt-aware native-exit handling."""

    running: Any | None
    stopping: bool


_LaneT = TypeVar("_LaneT", bound=ProcessLaneExitSource)


def render_dispatch_failure(stage: str, error: BaseException) -> str:
    """Return one bounded, redacted parent dispatch diagnostic."""

    detail = redact_public_text(error, fallback="backfill.process_chunk_dispatch_failed", max_length=512)
    return f"DPONE_BACKFILL_PROCESS_CHUNK_DISPATCH_FAILED stage={stage} error={detail}"


def project_lane_diagnostics(lanes: Iterable[ProcessLaneDiagnosticSource]) -> list[ProcessLaneDiagnostic]:
    """Project bounded startup diagnostics without configs, rows, or credentials."""

    return [
        ProcessLaneDiagnostic(
            worker_id=ready.worker_id,
            pid=ready.pid,
            process_group_id=ready.process_group_id,
            session_id=ready.session_id,
            process_tree_isolated=ready.process_tree_isolated,
            start_method=ready.start_method,
            faulthandler_enabled=ready.faulthandler_enabled,
            exitcode=lane.final_exitcode if lane.final_exitcode is not None else lane.process.exitcode,
        )
        for lane in lanes
        if (ready := lane.ready) is not None
    ]


def render_native_exit(lane: ProcessLaneDiagnosticSource, *, error_code: str) -> str:
    """Render signal and faulthandler state for an abnormal native exit."""

    exitcode = lane.process.exitcode
    signal_name = "none"
    if isinstance(exitcode, int) and exitcode < 0:
        try:
            signal_name = signal.Signals(-exitcode).name
        except ValueError:
            signal_name = f"SIGNAL_{-exitcode}"
    enabled = bool(lane.ready and lane.ready.faulthandler_enabled)
    return (
        f"{error_code}: worker_id={lane.worker_id} exitcode={exitcode} "
        f"signal={signal_name} faulthandler_enabled={str(enabled).lower()}"
    )


def diagnostics(lanes: list[ProcessLaneDiagnosticSource]) -> list[ProcessLaneDiagnostic]:
    """Project bounded startup diagnostics without configs, rows, or credentials."""

    return project_lane_diagnostics(lanes)


def native_exit_message(lane: ProcessLaneDiagnosticSource) -> str:
    """Render signal and faulthandler state for an abnormal native exit."""

    return render_native_exit(lane, error_code=NATIVE_EXIT_ERROR)


def handle_native_exits(
    lanes: list[_LaneT],
    ready_events: list[Any],
    *,
    resolve_failure: Callable[[_LaneT, str], None],
    errors: list[str],
) -> bool:
    """Resolve crashed lanes only through the receipt-aware parent policy."""

    found = False
    for lane in lanes:
        if lane.running is None:
            continue
        if lane.process.sentinel not in ready_events and lane.process.exitcode is None:
            continue
        lane.process.join(timeout=0)
        if lane.process.exitcode is None:
            continue
        error = native_exit_message(lane)
        errors.append(error)
        lane.stopping = True
        resolve_failure(lane, error)
        found = True
    return found


__all__ = [
    "NATIVE_EXIT_ERROR",
    "ProcessLaneDiagnosticSource",
    "ProcessLaneExitSource",
    "diagnostics",
    "handle_native_exits",
    "native_exit_message",
    "project_lane_diagnostics",
    "render_dispatch_failure",
    "render_native_exit",
]
