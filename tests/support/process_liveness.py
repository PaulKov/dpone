"""Conservative kernel-state oracle for tests waiting for process cleanup.

This observes whether a PID may still execute; it does not establish ownership
or guarantee a future state. Ambiguous reads and identity changes remain live.
Linux zombies/dead tasks still answer signal zero, so two matching kernel
identity observations are required before treating those states as stopped.
Other platforms retain the signal-zero existence check.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _kernel_identity(pid: int) -> tuple[bytes, int, bytes] | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_bytes()
        identity, separator, tail = raw.rpartition(b") ")
        prefix = f"{pid} (".encode("ascii")
        fields = tail.split()
        if not separator or not identity.startswith(prefix) or len(fields) < 20:
            return None
        if len(fields[0]) != 1 or not fields[19].isdigit():
            return None
        for value in fields[1:19]:
            if not value.removeprefix(b"-").isdigit():
                return None
        return identity, int(fields[19]), fields[0]
    except (OSError, ValueError):
        return None


def pid_is_running(pid: int) -> bool:
    """Return False only for an absent PID or verified Linux Z/X observations."""
    if type(pid) is not int or pid <= 0:
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except (OSError, OverflowError):
        return True
    if sys.platform != "linux":
        return True
    first = _kernel_identity(pid)
    if first is None:
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except (OSError, OverflowError):
        return True
    second = _kernel_identity(pid)
    if second is None or first[:2] != second[:2]:
        return True
    return first[2] not in {b"Z", b"X"} or second[2] not in {b"Z", b"X"}
