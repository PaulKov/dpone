"""Cleanup and liveness helpers for two-phase process-lane dispatch."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from multiprocessing.connection import wait
from typing import Protocol

from dpone.backfill.process_lane_contracts import ProcessLaneDispatch
from dpone.backfill.process_lane_processes import (
    ProcessLaneHandle,
    dispatch_failure_message,
    native_exit_message,
)

LEDGER_FAILURE_WRITE_ERROR = "DPONE_BACKFILL_PROCESS_LEDGER_FAILURE_WRITE_FAILED"


class _PreparedLaneDispatchView(Protocol):
    """Read-only dispatch claim fields required for cleanup and liveness."""

    @property
    def lane(self) -> ProcessLaneHandle: ...

    @property
    def dispatch(self) -> ProcessLaneDispatch: ...


def _prepared_lane_exit_error(prepared: Sequence[_PreparedLaneDispatchView]) -> str | None:
    """Observe every prepared sentinel once at the final pre-send boundary."""

    sentinels = [
        sentinel for item in prepared if (sentinel := getattr(item.lane.process, "sentinel", None)) is not None
    ]
    ready = set(wait(sentinels, timeout=0)) if sentinels else set()
    for item in prepared:
        process = item.lane.process
        sentinel = getattr(process, "sentinel", None)
        if process.exitcode is not None or sentinel in ready:
            return native_exit_message(item.lane)
    return None


def _abort_after_service(
    service_control: Callable[[], str | None] | None,
    prepared: Sequence[_PreparedLaneDispatchView],
    remaining: Sequence[_PreparedLaneDispatchView] | None = None,
    *,
    fail_chunk: Callable[[ProcessLaneDispatch, str], None],
    errors: list[str],
) -> bool:
    """Normalize callback failures and release every exact unsent claim.

    A service callback runs only in the parent, but it may fail after claims
    have already crossed the durable ledger boundary. Ordinary exceptions are
    reported as dispatch failures. Process-control signals are re-raised only
    after the same exact-owner cleanup has completed.
    """

    if service_control is None:
        return False
    try:
        error = service_control()
    except Exception as exc:
        error = dispatch_failure_message("service_control", exc)
        return _abort_prepared(
            error,
            prepared,
            remaining,
            fail_chunk=fail_chunk,
            errors=errors,
        )
    return _abort_prepared(
        error,
        prepared,
        remaining,
        fail_chunk=fail_chunk,
        errors=errors,
    )


def _abort_prepared(
    error: str | None,
    prepared: Sequence[_PreparedLaneDispatchView],
    remaining: Sequence[_PreparedLaneDispatchView] | None = None,
    *,
    fail_chunk: Callable[[ProcessLaneDispatch, str], None],
    errors: list[str],
) -> bool:
    if error is None:
        return False
    _append_once(errors, error)
    _abandon(prepared, fail_chunk=fail_chunk, error=error, errors=errors)
    if remaining:
        _abandon(remaining, fail_chunk=fail_chunk, error=error, errors=errors)
    return True


def _abandon(
    prepared: Sequence[_PreparedLaneDispatchView],
    *,
    fail_chunk: Callable[[ProcessLaneDispatch, str], None],
    error: str,
    errors: list[str],
) -> bool:
    """Release only claimed commands that were never sent to a child."""

    healthy = True
    for item in prepared:
        if item.lane.running is None and not _fail_claim(
            item.dispatch,
            fail_chunk=fail_chunk,
            error=error,
            errors=errors,
        ):
            healthy = False
    return healthy


def _fail_claim(
    dispatch: ProcessLaneDispatch,
    *,
    fail_chunk: Callable[[ProcessLaneDispatch, str], None],
    error: str,
    errors: list[str],
) -> bool:
    try:
        fail_chunk(dispatch, error)
    except BaseException:
        _append_once(errors, LEDGER_FAILURE_WRITE_ERROR)
        return False
    return True


def _append_once(errors: list[str], error: str) -> None:
    if error not in errors:
        errors.append(error)


def _same_claim(before: ProcessLaneDispatch, after: ProcessLaneDispatch) -> bool:
    return (
        before.command_id == after.command_id
        and before.binding == after.binding
        and before.parent_context is after.parent_context
    )


def identity_dispatch(dispatch: ProcessLaneDispatch) -> ProcessLaneDispatch:
    """Return an unchanged dispatch when no refresh port is configured."""

    return dispatch


__all__ = [
    "LEDGER_FAILURE_WRITE_ERROR",
    "identity_dispatch",
    "_abandon",
    "_abort_after_service",
    "_abort_prepared",
    "_append_once",
    "_prepared_lane_exit_error",
    "_same_claim",
]
