"""Shared-deadline abort barrier for parent-owned process lanes."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from dpone.backfill.process_lane_dispatch_support import LEDGER_FAILURE_WRITE_ERROR
from dpone.backfill.process_lane_group import PROCESS_GROUP_ERROR


def cleanup_parent_abort(
    lanes: list[Any],
    *,
    abandon_deferred: Callable[[], bool],
    resolve_quiesced: Callable[[Any, str], bool],
    terminate_before: Callable[..., Any],
    close_handles: Callable[[list[Any]], None],
    default_error: str,
    timeout_seconds: float,
    errors: list[str],
) -> str | None:
    """Quiesce every captured tree before any receipt or ledger callback."""

    ledger_healthy = True
    group_healthy = True
    captured = [(lane, lane.running, lane.control_error or default_error) for lane in lanes if lane.running is not None]
    try:
        try:
            terminate_before(
                lanes,
                deadline=time.monotonic() + timeout_seconds,
            )
        except BaseException:
            _append_once(errors, PROCESS_GROUP_ERROR)
            group_healthy = False
        if group_healthy:
            try:
                ledger_healthy = abandon_deferred()
            except BaseException:
                _append_once(errors, LEDGER_FAILURE_WRITE_ERROR)
                ledger_healthy = False
            for lane, dispatch, error in captured:
                if lane.running is not dispatch:
                    _append_once(errors, LEDGER_FAILURE_WRITE_ERROR)
                    ledger_healthy = False
                    continue
                try:
                    if not resolve_quiesced(lane, error):
                        ledger_healthy = False
                except BaseException:
                    _append_once(errors, LEDGER_FAILURE_WRITE_ERROR)
                    ledger_healthy = False
    finally:
        try:
            close_handles(lanes)
        except BaseException:
            _append_once(errors, PROCESS_GROUP_ERROR)
            group_healthy = False
    if not ledger_healthy:
        return LEDGER_FAILURE_WRITE_ERROR
    if not group_healthy:
        return PROCESS_GROUP_ERROR
    return None


def _append_once(errors: list[str], error: str) -> None:
    if error not in errors:
        errors.append(error)


__all__ = ["cleanup_parent_abort"]
