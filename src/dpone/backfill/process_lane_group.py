"""Parent-side POSIX containment for one spawned backfill lane tree."""

from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

PROCESS_GROUP_ERROR = "DPONE_BACKFILL_PROCESS_GROUP_CLEANUP_FAILED"
_IS_LINUX = sys.platform.startswith("linux")
_LINUX_PROC_ROOT = Path("/proc")
_NON_EXECUTABLE_LINUX_STATES = frozenset({"X", "Z"})


def ready_identity_is_valid(lane: Any, ready: Any) -> bool:
    """Prove that READY reports the exact spawned PID and isolated group."""

    if ready.pid <= 0 or ready.pid != lane.process.pid:
        return False
    if os.name != "posix":
        return ready.process_group_id is None
    if ready.process_group_id != ready.pid or ready.process_group_id in {1, os.getpgrp()}:
        return False
    try:
        return os.getpgid(ready.pid) == ready.process_group_id
    except ProcessLookupError:
        return False


def signal_lane(lane: Any, signal_number: int) -> None:
    """Signal a validated process group, falling back only before READY."""

    process_group_id = _validated_process_group_id(lane)
    if process_group_id is not None:
        try:
            os.killpg(process_group_id, signal_number)
        except ProcessLookupError:
            pass
        return
    if lane.process.exitcode is not None:
        return
    try:
        if signal_number == signal.SIGKILL:
            lane.process.kill()
        else:
            lane.process.terminate()
    except ProcessLookupError:
        pass


def lane_or_group_is_alive(lane: Any) -> bool:
    """Return true while the interpreter or any native descendant remains."""

    return lane.process.is_alive() or _process_group_is_alive(lane)


def join_lane_groups(lanes: list[Any], *, timeout_seconds: float) -> None:
    """Join Python children and bound observation of their process groups."""

    deadline = time.monotonic() + timeout_seconds
    for lane in lanes:
        if lane.process.exitcode is None:
            lane.process.join(timeout=max(0.0, deadline - time.monotonic()))
    while time.monotonic() < deadline and any(_process_group_is_alive(lane) for lane in lanes):
        time.sleep(0.01)


def _process_group_is_alive(lane: Any) -> bool:
    process_group_id = _validated_process_group_id(lane)
    if process_group_id is None:
        return False
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    if _IS_LINUX:
        executable_member = _linux_group_has_executable_member(process_group_id)
        return executable_member is not False
    return True


def _linux_group_has_executable_member(process_group_id: int) -> bool | None:
    """Classify a Linux group, preserving ambiguity as fail-closed evidence."""

    group_member_seen = False
    try:
        for process_dir in _LINUX_PROC_ROOT.iterdir():
            if not process_dir.name.isascii() or not process_dir.name.isdecimal():
                continue
            try:
                state, member_group_id = _read_linux_process_stat(process_dir)
            except FileNotFoundError:
                continue
            except (OSError, UnicodeError, ValueError):
                return None
            if member_group_id != process_group_id:
                continue
            group_member_seen = True
            if state not in _NON_EXECUTABLE_LINUX_STATES:
                return True
    except OSError:
        return None
    if group_member_seen:
        return False
    return None


def _read_linux_process_stat(process_dir: Path) -> tuple[str, int]:
    expected_pid = int(process_dir.name)
    stat = (process_dir / "stat").read_text(encoding="utf-8")
    command_start = stat.find("(")
    command_end = stat.rfind(")")
    if command_start <= 0 or command_end <= command_start or stat[:command_start].strip() != str(expected_pid):
        raise ValueError("malformed Linux process identity")
    fields = stat[command_end + 1 :].split()
    if len(fields) < 3 or len(fields[0]) != 1:
        raise ValueError("malformed Linux process state")
    process_group_id = int(fields[2])
    if process_group_id <= 0:
        raise ValueError("invalid Linux process-group identity")
    return fields[0], process_group_id


def _validated_process_group_id(lane: Any) -> int | None:
    ready = lane.ready
    process_pid = lane.process.pid
    if (
        os.name != "posix"
        or ready is None
        or process_pid is None
        or ready.pid != process_pid
        or ready.process_group_id != ready.pid
        or ready.process_group_id in {1, os.getpgrp()}
    ):
        return None
    return ready.process_group_id


__all__ = [
    "PROCESS_GROUP_ERROR",
    "join_lane_groups",
    "lane_or_group_is_alive",
    "ready_identity_is_valid",
    "signal_lane",
]
